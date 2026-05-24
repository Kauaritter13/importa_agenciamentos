"""
Importa N imoveis DIRETO da API do Vista para o Postgres.

Bypassa o S3 (que nao tem Caracteristicas/InfraEstrutura/proprietarios completos).
Pega TUDO: campos diretos (imoveis+carac+infra) + subgrupos (Foto, FotoEmpreendimento,
Video, Anexo, Autorizacao, PontoInteresse, Corretor, Agencia, prontuarios, proprietarios).

Uso:
  python inserir_lote_vista.py --quantidade 100
  python inserir_lote_vista.py --quantidade 100 --workers 4
  python inserir_lote_vista.py --quantidade 100 --apenas-ativos
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any, Dict, List, Optional, Tuple

try:
    from dotenv import load_dotenv
    load_dotenv(os.path.join(os.path.dirname(os.path.abspath(__file__)), ".env"))
except ImportError:
    pass

import psycopg
import requests

import exporta_vista_para_s3 as exp
import inserir_no_postgres as ipg


logger = logging.getLogger("inserir_lote_vista")


_STATUS_SEM_DETALHES = {"suspenso", "vendido", "alugado", "inativo",
                          "vendido terceiros", "vendido urban", "locado"}


def _vista_aceita_detalhes(vc: exp.VistaClient, codigo: str) -> bool:
    """Faz 1 ping rapido em /detalhes (com showInternal=1) pra ver se Vista responde
    dict ou [] (bloqueado). Custa 1 call extra mas economiza 8 calls falhas.
    """
    try:
        r = vc._get_raw("/imoveis/detalhes", {
            "imovel": codigo,
            "showInternal": "1",
            "showSuspended": "1",
            "pesquisa": json.dumps({"fields": ["Codigo"]}, ensure_ascii=False),
        })
        if r.status_code != 200:
            return False
        j = r.json()
        return isinstance(j, dict)
    except Exception:
        return False


def buscar_imovel_completo(vc: exp.VistaClient, codigo: str,
                            carac_fields: List[str], infra_fields: List[str],
                            direct: List[str], subgrupos: List[Dict[str, List[str]]]) -> Dict[str, Any]:
    """Busca 1 imovel completo do Vista: direct fields + subgrupos.

    OTIMIZACAO: detecta antes se Vista aceita /detalhes pra este imovel.
    Se nao aceita (Suspenso/Vendido), pula as 8 calls de subgrupos que falhariam.
    """
    # 1) Direct via /listar (suporta Suspenso/Vendido tambem)
    det = vc.listar_imovel_unico(codigo, direct) or {}
    if not det:
        status, det = vc.detalhes(codigo, ["Codigo"] + direct)
        if status != 200:
            return {}

    # Marca schema p/ _achatar_vista usar
    det["__schema_carac__"] = carac_fields
    det["__schema_infra__"] = infra_fields

    # 2) Heuristica: se Status indica Suspenso/Vendido, nao tentar subgrupos
    status_imovel = str(det.get("Status") or "").strip().lower()
    pular_subgrupos = any(s in status_imovel for s in _STATUS_SEM_DETALHES)
    if not pular_subgrupos and not _vista_aceita_detalhes(vc, codigo):
        pular_subgrupos = True

    if not pular_subgrupos:
        # Subgrupos: 1 call cada (mais robusto que pedir tudo junto)
        for sub in subgrupos:
            grupo = list(sub.keys())[0]
            status2, det2 = vc.detalhes(codigo, ["Codigo", sub])
            if status2 == 200 and isinstance(det2, dict) and det2.get(grupo):
                det[grupo] = det2[grupo]

    return det


import threading

# Cada thread tem sua propria conexao Postgres (psycopg nao e thread-safe).
# Registramos as conexoes pra fechar todas no fim do lote (evita idle vazando).
_TLS = threading.local()
_ALL_CONNS: list[psycopg.Connection] = []
_ALL_CONNS_LOCK = threading.Lock()


def _get_conn(db_url: str) -> psycopg.Connection:
    c = getattr(_TLS, "conn", None)
    if c is None or c.closed:
        c = psycopg.connect(db_url, connect_timeout=20)
        c.autocommit = False
        _TLS.conn = c
        with _ALL_CONNS_LOCK:
            _ALL_CONNS.append(c)
    return c


def _close_all_thread_conns() -> int:
    """Fecha todas as conexoes abertas pelas threads. Chamar no fim do lote."""
    fechadas = 0
    with _ALL_CONNS_LOCK:
        for c in _ALL_CONNS:
            try:
                if not c.closed:
                    c.close()
                    fechadas += 1
            except Exception:
                pass
        _ALL_CONNS.clear()
    return fechadas


def fetch_e_inserir(vc: exp.VistaClient, codigo: str, db_url: str, agent_user_id: str,
                     carac_fields: List[str], infra_fields: List[str],
                     direct: List[str], subgrupos: List[Dict[str, List[str]]],
                     sem_documents: bool = False) -> Dict[str, int]:
    """Worker thread: faz fetch do Vista + insert no Postgres. 1 conexao por thread."""
    stats = {"custom_fields": 0, "fotos": 0, "videos": 0, "docs": 0,
             "historico": 0, "owners": 0, "empreendimento": 0}
    # 1) Fetch do Vista (sem connection holding)
    det = buscar_imovel_completo(vc, codigo, carac_fields, infra_fields, direct, subgrupos)
    if not det:
        return {"erro_essencial": 1, **stats}

    # 2) Insert no Postgres com conexao da thread
    conn = _get_conn(db_url)
    try:
        with conn.cursor() as cur:
            # Resolve user_id do CORRETOR REAL do imovel (em vez de vista-import)
            corretor_id = ipg.resolver_corretor_do_imovel(cur, det, agent_user_id)
            row = ipg.montar_property_row(det, cur, corretor_id)
            if not row:
                conn.rollback()
                return {"erro_essencial": 1, **stats}
            prop_id = ipg.upsert_property(cur, row)

            # Empreendimento (com cache global do ipg)
            emp_nome = ipg._to_str(det.get("Empreendimento"))
            if emp_nome:
                dev_id = ipg.get_or_create_empreendimento(cur, emp_nome, corretor_id)
                if dev_id:
                    cur.execute(
                        "UPDATE properties SET development_id = %s WHERE id = %s",
                        (dev_id, prop_id),
                    )
                    stats["empreendimento"] = 1

            stats["fotos"]         += ipg.inserir_fotos(cur, prop_id, det, corretor_id)
            # property_videos e property_documents foram removidas no novo schema.
            # Videos vao via campo URL 'link_do_video'. Anexos vao via campo ATTACHMENT 'outros'.
            stats["historico"]     += ipg.inserir_historico(cur, prop_id, det, corretor_id)
            stats["owners"]        += ipg.inserir_proprietarios(cur, prop_id, det, corretor_id)
            stats["custom_fields"] += ipg.inserir_custom_fields(cur, prop_id, det)
            # Pos-processo: migra URLs Vista -> nosso S3 nos custom_fields ATTACHMENT
            stats.setdefault("anexos_migrados", 0)
            stats["anexos_migrados"] += ipg.migrar_attachments_pra_s3(cur, prop_id)

            # Reset property_agents: re-imports devem refletir o corretor ATUAL
            # (sem acumular placeholders antigos como "Corretor Vista <cod>").
            cur.execute("DELETE FROM property_agents WHERE property_id = %s", (prop_id,))
            cur.execute("""
                INSERT INTO property_agents (property_id, user_id, created_at)
                VALUES (%s, %s, NOW()) ON CONFLICT DO NOTHING
            """, (prop_id, corretor_id))

        conn.commit()
        stats["sucesso"] = 1
        return stats
    except Exception as exc:
        conn.rollback()
        logger.warning("falha imovel %s: %s", codigo, exc)
        return {"erro": 1, **stats}


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--quantidade", type=int, default=100)
    ap.add_argument("--todos", action="store_true",
                    help="ignora --quantidade e pega TODOS os imoveis do Vista")
    ap.add_argument("--workers", type=int, default=8, help="paralelismo total (fetch + insert)")
    ap.add_argument("--apenas-ativos", action="store_true",
                    help="filtra so imoveis com Status != Suspenso/Inativo")
    ap.add_argument("--status", default=None,
                    help='filtra por Status especifico (ex: Venda, "Pre-Venda", Locacao). Sobrescreve --apenas-ativos.')
    ap.add_argument("--codigos", nargs="*", help="codigos especificos (sobrescreve --quantidade)")
    ap.add_argument("--sem-documents", action="store_true",
                    help="nao popular tabela property_documents; anexos vao SOMENTE para custom_field 'outros'")
    ap.add_argument("--checkpoint", default="_lote_checkpoint.json",
                    help="arquivo de checkpoint para retomada")
    ap.add_argument("--retomar", action="store_true",
                    help="retoma a partir do checkpoint (pula codigos ja processados)")
    args = ap.parse_args()
    if args.todos:
        args.quantidade = 10**9

    logging.basicConfig(level=logging.WARNING,
                        format="%(asctime)s - %(levelname)s - %(message)s")

    DB = os.getenv("DATABASE_PUBLIC_URL") or os.getenv("DATABASE_URL")
    if not DB:
        print("ERRO: DATABASE_URL nao configurada")
        return 1

    print("=" * 70)
    print("Importacao em LOTE direto do Vista")
    print(f"  Quantidade  : {args.quantidade}")
    print(f"  Workers     : {args.workers}")
    print(f"  So ativos   : {args.apenas_ativos}")
    print("=" * 70)

    # 1) Schema (1x). Session com pool grande pra suportar 16+ workers
    session = requests.Session()
    pool_size = max(args.workers * 2, 32)
    adapter = requests.adapters.HTTPAdapter(
        pool_connections=pool_size,
        pool_maxsize=pool_size,
        max_retries=2,
    )
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    vc = exp.VistaClient(os.getenv("VISTA_API_HOST"), os.getenv("VISTA_API_KEY"), session)
    print("\nDescobrindo schema do Vista...")
    schema = vc.listarcampos()
    carac_fields = list(schema.get("carac", []))
    infra_fields = list(schema.get("infra", []))
    direct = (list(schema.get("imoveis", []))
              + list(schema.get("codigo", []))
              + carac_fields + infra_fields)
    direct = list(dict.fromkeys(direct))

    subgrupos = []
    for grp in ("Foto", "FotoEmpreendimento", "Video", "Anexo",
                "Autorizacao", "PontoInteresse", "prontuarios", "proprietarios"):
        c = schema.get(grp)
        if isinstance(c, list) and c:
            subgrupos.append({grp: list(c)})
    print(f"  direct fields: {len(direct)}")
    print(f"  subgrupos: {[list(s.keys())[0] for s in subgrupos]}")

    # Carrega usuarios do Vista (cache global) para resolver corretores reais
    print("\nCarregando usuarios do Vista (/usuarios/listar)...")
    n_corr = ipg.carregar_corretores_vista(vc)
    print(f"  {n_corr} corretores Vista carregados em cache")

    # 2) Lista codigos
    checkpoint_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), args.checkpoint)
    ja_processados: set[str] = set()
    if args.retomar and os.path.exists(checkpoint_path):
        try:
            with open(checkpoint_path, "r", encoding="utf-8") as f:
                ja_processados = set(json.load(f).get("processados", []))
            print(f"  checkpoint: {len(ja_processados)} ja processados")
        except Exception as exc:
            print(f"  checkpoint corrupto: {exc} (recomecando)")
            ja_processados = set()

    if args.codigos:
        codigos = list(args.codigos)
    else:
        alvo = args.quantidade if not args.todos else 10**9
        print(f"\nListando codigos de imoveis (alvo={alvo if alvo < 10**6 else 'TODOS'})...")
        codigos = []
        page = 1
        per = 50  # limite hard do Vista
        if args.status:
            extra = {"Status": args.status}
        elif args.apenas_ativos:
            extra = {"Status": ["Venda", "Pré-Venda", "Locação"]}
        else:
            extra = None
        print(f"  filtro Status: {extra}")
        while len(codigos) < alvo:
            try:
                data = vc.listar_codigos(page, per, extra)
            except Exception as exc:
                print(f"  erro listando page {page}: {exc} (parando)")
                break
            novos = 0
            for k, v in data.items():
                if k in ("total", "paginas", "pagina", "quantidade"):
                    continue
                if isinstance(v, dict) and v.get("Codigo"):
                    codigos.append(str(v["Codigo"]))
                    novos += 1
            total = data.get("total")
            paginas = data.get("paginas")
            if total and page == 1:
                print(f"  Vista reportou {total} imoveis em {paginas} paginas")
            if novos == 0:
                break
            if paginas and page >= int(paginas):
                break
            page += 1
        if not args.todos:
            codigos = codigos[: args.quantidade]

    # Filtra ja processados (checkpoint)
    if ja_processados:
        antes = len(codigos)
        codigos = [c for c in codigos if c not in ja_processados]
        print(f"  pulando {antes - len(codigos)} ja processados, faltam {len(codigos)}")

    print(f"  {len(codigos)} codigos a processar")

    # 3) Bootstrap: pega agent_user_id (1 vez, conexao temporaria)
    print("\nConectando no Postgres (bootstrap)...")
    boot = psycopg.connect(DB, connect_timeout=20)
    boot.autocommit = False
    with boot.cursor() as cur:
        agent_user_id = ipg.get_or_create_import_user(cur)
    boot.commit()
    boot.close()
    print(f"  user import: {agent_user_id}")

    try:
        # 4) Pool paralelo: cada worker faz fetch + insert com sua propria conexao
        totals = {"sucesso": 0, "erro": 0, "erro_essencial": 0,
                  "custom_fields": 0, "fotos": 0, "videos": 0, "docs": 0,
                  "historico": 0, "owners": 0, "empreendimento": 0,
                  "anexos_migrados": 0}
        inicio = time.time()

        print(f"\nBuscando + inserindo {len(codigos)} imoveis em paralelo "
              f"(workers={args.workers}, cada um com sua conexao PG)...\n")

        processados_set = set(ja_processados)
        def _save_checkpoint():
            try:
                with open(checkpoint_path, "w", encoding="utf-8") as f:
                    json.dump({"processados": sorted(processados_set)}, f)
            except Exception as exc:
                logger.warning("falha salvando checkpoint: %s", exc)

        with ThreadPoolExecutor(max_workers=args.workers, thread_name_prefix="vista") as ex:
            futures = {
                ex.submit(fetch_e_inserir, vc, cod, DB, agent_user_id,
                          carac_fields, infra_fields, direct, subgrupos,
                          args.sem_documents): cod
                for cod in codigos
            }
            for i, fut in enumerate(as_completed(futures), 1):
                cod = futures[fut]
                try:
                    stats = fut.result()
                except Exception as exc:
                    logger.warning("worker exception %s: %s", cod, exc)
                    totals["erro"] += 1
                    continue
                if stats.get("sucesso"):
                    totals["sucesso"] += 1
                    processados_set.add(cod)
                elif stats.get("erro_essencial"):
                    totals["erro_essencial"] += 1
                    processados_set.add(cod)  # nao tentar de novo (Vista nao tem)
                elif stats.get("erro"):
                    totals["erro"] += 1
                for k in ("custom_fields","fotos","videos","docs","historico","owners","empreendimento","anexos_migrados"):
                    totals[k] += stats.get(k, 0)

                if i % 25 == 0 or i == len(codigos):
                    _save_checkpoint()

                if i % 10 == 0 or i == len(codigos):
                    elapsed = time.time() - inicio
                    rate = i / max(elapsed, 0.1)
                    eta = (len(codigos) - i) / max(rate, 0.01)
                    print(f"  {i:>5}/{len(codigos)} | ok={totals['sucesso']} "
                          f"err={totals['erro']+totals['erro_essencial']} | "
                          f"custom={totals['custom_fields']} fotos={totals['fotos']} "
                          f"propr={totals['owners']} hist={totals['historico']} "
                          f"emp={totals['empreendimento']} | "
                          f"{elapsed:.0f}s ({rate:.1f} im/s, ETA {eta/60:.1f}min)", flush=True)
        _save_checkpoint()

        # IMPORTANTE: fecha TODAS as conexoes que as threads abriram
        n_fechadas = _close_all_thread_conns()
        if n_fechadas:
            print(f"\n  fechadas {n_fechadas} conexoes de worker threads (evita idle no PG)")

        # 5) Resumo final + contagem das tabelas
        elapsed = time.time() - inicio
        print()
        print("=" * 70)
        print(f"Concluido em {elapsed:.1f}s ({elapsed/60:.1f}min)")
        print("=" * 70)
        for k, v in totals.items():
            print(f"  {k:25s}: {v:,}")

        # Conexao final pra resumo
        with psycopg.connect(DB, connect_timeout=20) as conn:
            with conn.cursor() as cur:
                print("\nContagem final das tabelas:")
                for tab in ("properties", "property_field_values", "property_photos",
                            "property_history",
                            "property_owner_properties", "property_owners", "property_agents"):
                    cur.execute(f"SELECT COUNT(*) FROM {tab}")
                    print(f"  {tab:35s}: {cur.fetchone()[0]:>8,}")

                # Media de custom_fields por imovel
                cur.execute("""
                    SELECT AVG(c) FROM (
                        SELECT COUNT(*) AS c FROM property_field_values
                        GROUP BY property_id
                    ) x
                """)
                avg = cur.fetchone()[0]
                if avg:
                    print(f"\n  Media custom_fields/imovel: {avg:.1f}/299")
    finally:
        pass

    return 0


if __name__ == "__main__":
    sys.exit(main())
