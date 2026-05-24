"""
Insere os imoveis MAIS COMPLETOS do S3 no Postgres preenchendo TUDO:
  properties + property_photos + property_videos + property_documents
  + property_activity_logs (historico) + property_owners + property_owner_properties
  + property_categories + property_statuses

Uso:
  python inserir_no_postgres.py                           # 100 mais completos
  python inserir_no_postgres.py --quantidade 500
  python inserir_no_postgres.py --quantidade 100 --limpar # limpa tudo antes
  python inserir_no_postgres.py --apenas-com-fotos
  python inserir_no_postgres.py --dry-run
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import re
import sys
import time
import uuid
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

# Carrega .env
try:
    from dotenv import load_dotenv
    load_dotenv(os.path.join(os.path.dirname(os.path.abspath(__file__)), ".env"))
except ImportError:
    pass

import psycopg
from psycopg.rows import dict_row
import threading

import exporta_vista_para_s3 as exp
import mapeamento_total_vista_rafa as mtv  # mapeamento total 299/299 Vista -> Rafa


logger = logging.getLogger("inserir_no_postgres")

# Lock global para serializar operacoes que fazem SELECT-then-INSERT em recursos
# compartilhados entre threads (empreendimentos, owners, categorias, statuses).
_GLOBAL_LOCK = threading.RLock()

DATABASE_URL = os.getenv("DATABASE_PUBLIC_URL") or os.getenv("DATABASE_URL")


# ============================================================
# Helpers de conversao
# ============================================================

def _to_float(v: Any) -> Optional[float]:
    if v is None or v == "": return None
    if isinstance(v, (int, float)): return float(v)
    s = str(v).strip().replace(",", ".")
    if not s: return None
    try: return float(s)
    except ValueError: return None


def _to_int(v: Any) -> Optional[int]:
    f = _to_float(v)
    return int(f) if f is not None else None


def _to_str(v: Any, max_len: Optional[int] = None) -> Optional[str]:
    if v is None: return None
    s = str(v).strip()
    if not s: return None
    if max_len and len(s) > max_len: s = s[:max_len]
    return s


def _to_bool(v: Any) -> bool:
    if isinstance(v, bool): return v
    if isinstance(v, str): return v.strip().lower() in ("sim","yes","true","1","s")
    return bool(v)


def _to_date(v: Any) -> Optional[datetime]:
    if not v: return None
    if isinstance(v, datetime): return v
    s = str(v).strip()
    if not s or s == "0000-00-00": return None
    for fmt in ("%Y-%m-%d %H:%M:%S","%Y-%m-%d","%d/%m/%Y"):
        try: return datetime.strptime(s, fmt)
        except ValueError: continue
    return None


def _strip_html(s: Optional[str]) -> Optional[str]:
    if not s: return s
    return re.sub(r"<[^>]+>", " ", s).replace("&nbsp;", " ").strip()


def _safe_filename_from_url(url: str, fallback: str) -> str:
    from urllib.parse import urlparse
    try:
        path = urlparse(url).path
        name = os.path.basename(path) or fallback
    except Exception:
        name = fallback
    return name or fallback


# ============================================================
# Bootstrap: pega/cria user, categorias, statuses
# ============================================================

def _hash_simples_senha(senha: str) -> str:
    """Hash bcrypt-like compativel com sistemas Node.js. Usa hashlib se nao houver bcrypt."""
    try:
        import bcrypt
        return bcrypt.hashpw(senha.encode(), bcrypt.gensalt()).decode()
    except ImportError:
        # Fallback: hash sha256 (sistema vai recusar login mas user existe)
        import hashlib
        return "$2b$10$" + hashlib.sha256(senha.encode()).hexdigest()[:54]


def get_or_create_import_user(cur) -> str:
    """Retorna o id do user 'vista-import' (cria se nao existir)."""
    cur.execute("SELECT id FROM users WHERE email = %s LIMIT 1", ("vista-import@sistema.local",))
    row = cur.fetchone()
    if row:
        return str(row[0])

    # Pega 1 user existente como fallback (pra usar role/departamento valido)
    cur.execute("""
        SELECT id, role_id, department_id FROM users
        WHERE is_active = true ORDER BY created_at LIMIT 1
    """)
    base = cur.fetchone()
    role_id = base[1] if base else None
    dept_id = base[2] if base else None

    new_id = str(uuid.uuid4())
    cur.execute("""
        INSERT INTO users (id, email, hashed_password, name, is_active, role_id, department_id, created_at, updated_at)
        VALUES (%s, %s, %s, %s, true, %s, %s, NOW(), NOW())
    """, (new_id, "vista-import@sistema.local", _hash_simples_senha("ImportSystem!2026"),
          "Vista Import (Sistema)", role_id, dept_id))
    return new_id


_corretor_cache: Dict[str, str] = {}  # cache: vista_codigo -> crm_user_id
_CORRETORES_VISTA: Dict[str, Dict[str, str]] = {}  # cache: vista_codigo -> {nome, email, celular}


def carregar_corretores_vista(vc) -> int:
    """Carrega TODOS os usuarios do Vista (/usuarios/listar) e cacheia em
    _CORRETORES_VISTA. Chamar 1x no inicio do pipeline.

    Usa showInactive=1, showSuspended=1, showInternal=1 para puxar TAMBEM
    usuarios excluidos/suspensos (ex: cod 243 = "Cristiane Freyer (Excluido)").
    Sem esses params, o Vista esconde inativos e o codigo vira placeholder
    "Corretor Vista <cod>" sem email/nome reais.
    """
    global _CORRETORES_VISTA
    page = 1
    while True:
        try:
            r = vc._get_raw("/usuarios/listar", {
                "showtotal": "1",
                "showInactive": "1",
                "showSuspended": "1",
                "showInternal": "1",
                "pesquisa": json.dumps({
                    "fields": ["Codigo", "Nome", "Email", "Celular", "Fone", "Inativo"],
                    "paginacao": {"pagina": page, "quantidade": 50},
                }, ensure_ascii=False),
            })
            if r.status_code != 200:
                break
            j = r.json()
        except Exception:
            break
        novos = 0
        for k, v in j.items():
            if k in ("total", "paginas", "pagina", "quantidade"): continue
            if isinstance(v, dict):
                cod = str(v.get("Codigo") or k)
                _CORRETORES_VISTA[cod] = {
                    "nome":  (v.get("Nome") or "").strip(),
                    "email": (v.get("Email") or "").strip().lower(),
                    "celular": (v.get("Celular") or "").strip(),
                    "fone": (v.get("Fone") or "").strip(),
                    "inativo": (v.get("Inativo") or "").strip().lower() == "sim",
                }
                novos += 1
        paginas = j.get("paginas")
        if paginas and page >= int(paginas):
            break
        if novos == 0:
            break
        page += 1
    return len(_CORRETORES_VISTA)


def get_or_create_corretor_user(cur, codigo_corretor: Optional[str],
                                  corretor_nome: Optional[str],
                                  fallback_user_id: str) -> str:
    """Resolve user_id no CRM representando um corretor do Vista.

    Ordem de busca:
      1. Cache em memoria (vista_codigo -> crm_user_id)
      2. Match por EMAIL exato no CRM (usando dados do _CORRETORES_VISTA)
      3. Match por NOME exato (case-insensitive)
      4. Match por NOME parcial (LIKE %name%) - se UNICO match
      5. Cria novo user com dados reais do Vista
    """
    cod = (codigo_corretor or "").strip()
    nome_raw = (corretor_nome or "").strip()
    # CorretorNome vem como "278:Loja Nilo" - separa
    if ":" in nome_raw:
        partes = nome_raw.split(":", 1)
        if len(partes) == 2:
            if not cod: cod = partes[0].strip()
            nome_raw = partes[1].strip()

    if not cod and not nome_raw:
        return fallback_user_id

    cache_key = cod or nome_raw.lower()
    if cache_key in _corretor_cache:
        return _corretor_cache[cache_key]

    # Busca dados reais no cache de usuarios Vista (nome, email)
    vista_user = _CORRETORES_VISTA.get(cod) or {}
    nome_real = vista_user.get("nome") or nome_raw
    email_real = vista_user.get("email") or ""

    with _GLOBAL_LOCK:
        if cache_key in _corretor_cache:
            return _corretor_cache[cache_key]

        # 1) Match por EMAIL exato no CRM
        if email_real:
            cur.execute(
                "SELECT id FROM users WHERE LOWER(email) = LOWER(%s) AND is_active = true LIMIT 1",
                (email_real,)
            )
            r = cur.fetchone()
            if r:
                uid = str(r[0])
                _corretor_cache[cache_key] = uid
                return uid

        # 2) Match por NOME exato no CRM
        if nome_real:
            cur.execute(
                "SELECT id FROM users WHERE LOWER(name) = LOWER(%s) AND is_active = true LIMIT 1",
                (nome_real,)
            )
            r = cur.fetchone()
            if r:
                uid = str(r[0])
                _corretor_cache[cache_key] = uid
                return uid

        # 3) Match por NOME parcial (se UNICO match)
        if nome_real and len(nome_real) >= 4:
            cur.execute("""
                SELECT id FROM users
                WHERE LOWER(name) LIKE LOWER(%s) AND is_active = true
                LIMIT 2
            """, (f"%{nome_real}%",))
            rows = cur.fetchall()
            if len(rows) == 1:
                uid = str(rows[0][0])
                _corretor_cache[cache_key] = uid
                return uid

        # 4) Cria novo user com dados reais do Vista
        email_para_user = email_real or f"corretor-vista-{cod}@import.local"
        # Pega role/dept de um user existente
        cur.execute("""
            SELECT role_id, department_id FROM users
            WHERE is_active = true AND role_id IS NOT NULL
            ORDER BY created_at LIMIT 1
        """)
        base = cur.fetchone()
        role_id = base[0] if base else None
        dept_id = base[1] if base else None

        new_id = str(uuid.uuid4())
        cur.execute("""
            INSERT INTO users (id, email, hashed_password, name, is_active, role_id, department_id, created_at, updated_at)
            VALUES (%s, %s, %s, %s, true, %s, %s, NOW(), NOW())
            ON CONFLICT (email) DO UPDATE SET updated_at = NOW()
            RETURNING id
        """, (new_id, email_para_user, _hash_simples_senha("ImportSystem!2026"),
              nome_real or f"Corretor Vista {cod}", role_id, dept_id))
        rr = cur.fetchone()
        new_id = str(rr[0]) if rr else new_id
        cur.connection.commit()
        _corretor_cache[cache_key] = new_id
        return new_id


def resolver_corretor_do_imovel(cur, det: Dict[str, Any], fallback_user_id: str) -> str:
    """Resolve o user_id do corretor responsavel pelo imovel.

    Ordem de prioridade:
      1. CorretorPrimeiroAge / CaptadorAccountId (corretor PESSOA que captou)
      2. Corretor do PRIMEIRO prontuario cronologico ("Inclusao de cadastro",
         "Cadastro do imovel", etc) - quem efetivamente cadastrou
      3. Agenciador / CodigoCorretor (geralmente loja/empresa)
      4. fallback
    """
    # 1) Tenta campos do captador real
    cod = _to_str(det.get("CorretorPrimeiroAge") or det.get("CaptadorAccountId"))
    if cod:
        v = _CORRETORES_VISTA.get(cod) or {}
        nome = v.get("nome") or ""
        if nome:
            return get_or_create_corretor_user(cur, cod, nome, fallback_user_id)

    # 2) Primeiro prontuario cronologico (quem cadastrou)
    pront = det.get("prontuarios")
    if isinstance(pront, dict) and pront:
        # Ordena cronologicamente
        sorted_keys = sorted(
            (k for k, v in pront.items() if isinstance(v, dict)),
            key=lambda kk: (pront[kk].get("Data") or "", pront[kk].get("Hora") or "")
        )
        for k in sorted_keys:
            p = pront[k]
            cod_pront = _to_str(p.get("CodigoCorretor"))
            nome_pront = _to_str(p.get("Corretor"))
            if cod_pront or nome_pront:
                return get_or_create_corretor_user(cur, cod_pront, nome_pront, fallback_user_id)

    # 3) Top-level (CorretorNome geralmente eh a loja/empresa)
    cod_top = _to_str(det.get("CodigoCorretor") or det.get("Agenciador"))
    nome_top = _to_str(det.get("CorretorNome"))
    return get_or_create_corretor_user(cur, cod_top, nome_top, fallback_user_id)


def resolver_corretor_do_prontuario(cur, prontuario: Dict[str, Any], fallback_user_id: str) -> str:
    codigo = _to_str(prontuario.get("CodigoCorretor"))
    nome = _to_str(prontuario.get("Corretor"))
    if not codigo and not nome:
        return fallback_user_id
    return get_or_create_corretor_user(cur, codigo, nome, fallback_user_id)


# Mapeamento Vista -> categoria/status existentes no banco
CATEGORIA_VISTA_BANCO = {
    "Apartamento": "Apartamento",
    "Apartamento Duplex": "Apartamento Duplex",
    "Apartamento Garden": "Apartamento Garden",
    "Cobertura": "Cobertura",
    "Casa": "Casa",
    "Casa em Condomínio": "Casa em Condomínio",
    "Casa em Condominio": "Casa em Condomínio",
    "Loft": "Loft",
    "Terreno": "Terreno",
    "Empreendimento": "Empreendimento",
    "Sala": "Apartamento",
    "Sala Comercial": "Apartamento",
    "Salas e Conjunto": "Apartamento",
    "Casa Comercial": "Casa",
    "Casa de Alvenaria": "Casa",
    "Sobrado": "Casa",
}

STATUS_VISTA_BANCO = {
    "Venda": "Venda",
    "Pré-Venda": "Pré-Venda",
    "Pre-Venda": "Pré-Venda",
    "Pendente": "Pendente",
    "Suspenso": "Suspenso",
    "Vendido Terceiros": "Vendido Terceiros",
    "Vendido": "Vendido Urban Select",
    "Vendido Urban Lançamentos": "Vendido Urban Lançamentos",
    "Vendido Urban Lancamentos": "Vendido Urban Lançamentos",
    "Vendido Urban Select": "Vendido Urban Select",
    "Vendido Urban Canoas": "Vendido Urban Canoas",
    "Vendido Urban Itapema": "Vendido Urban Itapema",
    "Locação": "Locação",
    "Locacao": "Locação",
    "Alugado": "Locação",
}

_cat_cache: Dict[str, Optional[str]] = {}
_status_cache: Dict[str, Optional[str]] = {}


def get_existing_category(cur, vista_name: str) -> Optional[str]:
    """Retorna id da categoria existente (mapeada). Thread-safe via cache + lock."""
    if not vista_name: vista_name = "Apartamento"
    target = CATEGORIA_VISTA_BANCO.get(vista_name.strip(), vista_name.strip())
    if target in _cat_cache:
        return _cat_cache[target]
    with _GLOBAL_LOCK:
        if target in _cat_cache:
            return _cat_cache[target]
        cur.execute("SELECT id FROM property_categories WHERE LOWER(name) = LOWER(%s) LIMIT 1",
                    (target,))
        r = cur.fetchone()
        cid = str(r[0]) if r else None
        if not cid:
            cur.execute("SELECT id FROM property_categories WHERE LOWER(name) = 'apartamento' LIMIT 1")
            r = cur.fetchone()
            cid = str(r[0]) if r else None
        _cat_cache[target] = cid
        return cid


def get_existing_status(cur, vista_name: str) -> Optional[str]:
    """Retorna id do status existente (mapeado). Thread-safe via cache + lock."""
    if not vista_name: vista_name = "Venda"
    target = STATUS_VISTA_BANCO.get(vista_name.strip(), "Venda")
    if target in _status_cache:
        return _status_cache[target]
    with _GLOBAL_LOCK:
        if target in _status_cache:
            return _status_cache[target]
        cur.execute("SELECT id FROM property_statuses WHERE LOWER(name) = LOWER(%s) LIMIT 1",
                    (target,))
        r = cur.fetchone()
        sid = str(r[0]) if r else None
        if not sid:
            cur.execute("SELECT id FROM property_statuses WHERE LOWER(name) = 'venda' LIMIT 1")
            r = cur.fetchone()
            sid = str(r[0]) if r else None
        _status_cache[target] = sid
        return sid


# Aliases pra compatibilidade
get_or_create_category = get_existing_category
get_or_create_status = get_existing_status


_emp_cache: Dict[str, Optional[str]] = {}


# Lixo conhecido em campo Empreendimento do Vista
EMPREENDIMENTO_LIXO = {
    "casa", "casa de rua", "de rua", "rua", "apartamento", "apto",
    "sobrado", "terreno", "loft", "cobertura", "sala", "loja",
    "predio", "edificio", "condominio", "padrão", "padrao",
    "n/a", "na", "-", "--", "sem", "nao", "não", "sem nome",
    "x", "xx", "xxx", ".", "0",
}


def _empreendimento_valido(nome: str) -> bool:
    """Filtra lixo: nomes muito curtos, generic terms, etc."""
    if not nome: return False
    s = nome.strip().lower()
    if len(s) < 4: return False
    if s in EMPREENDIMENTO_LIXO: return False
    # Só letras/numeros/poucas chars: lixo
    import re
    if re.fullmatch(r"[\W_]+", s): return False
    return True


def get_or_create_empreendimento(cur, nome_empreendimento: str, agent_user_id: str) -> Optional[str]:
    """Match com empreendimento existente OU cria como property tipo Empreendimento.

    THREAD-SAFE: usa lock global para evitar race condition (varios workers
    tentando criar o mesmo empreendimento simultaneamente). O empreendimento
    e committado IMEDIATAMENTE para ficar visivel para outros workers.
    """
    if not nome_empreendimento: return None
    key = str(nome_empreendimento).strip()
    if not _empreendimento_valido(key): return None

    cache_key = key.lower()
    # Fast path: cache hit sem lock
    if cache_key in _emp_cache:
        return _emp_cache[cache_key]

    with _GLOBAL_LOCK:
        # Double-check: outro thread pode ter criado enquanto esperavamos o lock
        if cache_key in _emp_cache:
            return _emp_cache[cache_key]

        cur.execute("SELECT id FROM property_categories WHERE LOWER(name) = 'empreendimento' LIMIT 1")
        cat = cur.fetchone()
        if not cat:
            _emp_cache[cache_key] = None
            return None
        cat_id = cat[0]

        # Match por custom_field nome_empreendimento (exato + LIKE parcial)
        cur.execute("""
            SELECT p.id, pfv.value
            FROM properties p
            JOIN property_field_values pfv ON pfv.property_id = p.id
            JOIN property_fields pf ON pf.id = pfv.field_id
            WHERE p.category_id = %s AND pf.name = 'nome_empreendimento'
        """, (cat_id,))
        for rid, rval in cur.fetchall():
            rt = (rval or "").strip().lower()
            if rt and (rt == cache_key or rt in cache_key or cache_key in rt):
                _emp_cache[cache_key] = str(rid)
                return str(rid)

        # Cria novo empreendimento com ON CONFLICT por seguranca
        status_id = get_existing_status(cur, "Venda")
        new_id = str(uuid.uuid4())
        code = f"EMP-{abs(hash(cache_key)) % 1_000_000:06d}"
        cur.execute("""
            INSERT INTO properties (
                id, code, category_id, status_id, development_id, is_active,
                agents, owners, created_by_id, updated_by_id, created_at, updated_at
            ) VALUES (
                %s, %s, %s, %s, NULL, true,
                %s::jsonb, %s::jsonb, %s, %s, NOW(), NOW()
            )
            ON CONFLICT (code) DO UPDATE SET updated_at = NOW()
            RETURNING id
        """, (new_id, code, cat_id, status_id,
              json.dumps([agent_user_id]), json.dumps([]),
              agent_user_id, agent_user_id))
        r = cur.fetchone()
        emp_id = str(r[0]) if r else new_id

        # Grava o nome em nome_empreendimento + empreendimento
        for fname in ("nome_empreendimento", "empreendimento", "nome_condominio"):
            fid = get_field_id(cur, fname)
            if fid:
                upsert_custom_value(cur, fid, emp_id, key)

        # COMMIT IMEDIATO: garante visibilidade para outros workers
        cur.connection.commit()

        _emp_cache[cache_key] = emp_id
        return emp_id


# ============================================================
# Mapping: data.json -> linha de properties + relacionados
# ============================================================

def _map_status_simple(s: Optional[str]) -> str:
    """status varchar default 'active'."""
    if not s: return "inactive"
    sl = str(s).strip().lower()
    if "venda" in sl and "vendido" not in sl: return "active"
    if "locacao" in sl or "aluguel" in sl: return "active"
    if "vendido" in sl: return "sold"
    if "alugado" in sl: return "rented"
    if "suspenso" in sl: return "suspended"
    if "pendente" in sl: return "pending"
    return "inactive"


def montar_property_row(det: Dict[str, Any], cur, agent_user_id: str) -> Optional[Dict[str, Any]]:
    """Monta a linha de properties (schema enxuto: tudo o resto vai pra property_field_values).

    Colunas reais da tabela: id, code, category_id, status_id, development_id,
    is_active, agents (jsonb), owners (jsonb), created_by_id, updated_by_id,
    created_at, updated_at, deleted_at.
    """
    codigo = _to_str(det.get("Codigo"))
    if not codigo:
        return None

    cat_name = _to_str(det.get("Categoria")) or "Sem Categoria"
    status_name = _to_str(det.get("Status")) or "Indefinido"
    category_id = get_or_create_category(cur, cat_name)
    status_id = get_or_create_status(cur, status_name)

    return {
        "id": str(uuid.uuid4()),
        "code": codigo,
        "category_id": category_id,
        "status_id": status_id,
        "development_id": None,
        "is_active": _to_bool(det.get("ExibirNoSite")),
        "agents": json.dumps([agent_user_id]),
        "owners": json.dumps([]),
        "created_by_id": agent_user_id,
        "updated_by_id": agent_user_id,
        "created_at": _to_date(det.get("DataCadastro")) or datetime.now(),
        "updated_at": _to_date(det.get("DataAtualizacao") or det.get("DataHoraAtualizacao")) or datetime.now(),
    }


# ============================================================
# Subordinated tables: photos, videos, documents, history, owners
# ============================================================

def upsert_property(cur, row: Dict[str, Any]) -> str:
    """UPSERT atomico via ON CONFLICT - thread-safe.

    Retorna o id da row final (existente ou nova).
    """
    cur.execute("""
        INSERT INTO properties (
            id, code, category_id, status_id, development_id, is_active,
            agents, owners, created_by_id, updated_by_id, created_at, updated_at
        ) VALUES (
            %(id)s, %(code)s, %(category_id)s, %(status_id)s, %(development_id)s, %(is_active)s,
            %(agents)s::jsonb, %(owners)s::jsonb, %(created_by_id)s, %(updated_by_id)s,
            %(created_at)s, %(updated_at)s
        )
        ON CONFLICT (code) DO UPDATE SET
            category_id = EXCLUDED.category_id,
            status_id = EXCLUDED.status_id,
            is_active = EXCLUDED.is_active,
            agents = EXCLUDED.agents,
            owners = EXCLUDED.owners,
            updated_by_id = EXCLUDED.updated_by_id,
            -- Preserva datas REAIS do Vista (DataCadastro / DataAtualizacao).
            -- Sem isso, re-imports zeram created_at e o frontend mostra "0 dias".
            created_at = COALESCE(EXCLUDED.created_at, properties.created_at),
            updated_at = COALESCE(EXCLUDED.updated_at, properties.updated_at)
        RETURNING id
    """, row)
    return str(cur.fetchone()[0])


_S3_FOTOS_INDEX: Optional[Dict[str, int]] = None  # filename -> size


def _carregar_index_fotos_s3() -> Dict[str, int]:
    """Lista TODAS as fotos no bucket S3 1x e mapeia filename -> size.
    Como as fotos ja foram baixadas, podemos usar size do bucket em vez de HEAD remoto."""
    global _S3_FOTOS_INDEX
    if _S3_FOTOS_INDEX is not None:
        return _S3_FOTOS_INDEX
    print("  [s3] indexando fotos no bucket pra obter file_size...")
    s3 = exp.build_s3_client()
    idx: Dict[str, int] = {}
    paginator = s3.get_paginator("list_objects_v2")
    prefix = exp.s3_key("imoveis") + "/"
    for page in paginator.paginate(Bucket=exp.S3_BUCKET, Prefix=prefix):
        for obj in page.get("Contents", []) or []:
            key = obj.get("Key", "")
            if "/fotos/" not in key and "/foto_empreendimento/" not in key:
                continue
            sz = obj.get("Size", 0)
            if sz <= 0: continue
            # Extrai nome do arquivo final
            fname = key.rsplit("/", 1)[-1]
            # Os arquivos no bucket tem prefixo "0001-{nome}", remove o prefixo
            if "-" in fname[:5]:
                core = fname.split("-", 1)[1]
                idx[core] = sz
            idx[fname] = sz
    _S3_FOTOS_INDEX = idx
    print(f"  [s3] indexadas {len(idx):,} fotos")
    return idx


def _size_e_mime(url: str, fotos_idx: Dict[str, int]) -> Tuple[int, str]:
    """Pega size do index do S3 (instantaneo). Mime pela extensao."""
    import mimetypes
    fname = url.rsplit("/", 1)[-1]
    # Remove query string se houver
    fname = fname.split("?")[0]
    size = fotos_idx.get(fname, 1)
    mime, _ = mimetypes.guess_type(fname)
    return size, mime or "image/jpeg"


# ============================================================
# CDN MIGRATION: baixa midia do Vista e re-upload pro nosso S3
# Objetivo: ZERO dependencia do Vista apos importacao concluida.
# ============================================================

import uuid as _uuid_mod

_S3_CLIENT_CACHE = None
_S3_BUCKET_CACHE: Optional[str] = None
_S3_PREFIX_CACHE: Optional[str] = None


def _get_s3():
    """Cliente S3 cached (thread-safe ja que boto3 client suporta)."""
    global _S3_CLIENT_CACHE, _S3_BUCKET_CACHE, _S3_PREFIX_CACHE
    if _S3_CLIENT_CACHE is None:
        _S3_CLIENT_CACHE = exp.build_s3_client()
        _S3_BUCKET_CACHE = os.getenv("S3_BUCKET")
        _S3_PREFIX_CACHE = (os.getenv("S3_PREFIX") or "vista-export").rstrip("/")
    return _S3_CLIENT_CACHE, _S3_BUCKET_CACHE, _S3_PREFIX_CACHE


def baixar_e_subir_pro_s3(vista_url: str, prop_code: str, kind: str,
                            session: Optional[Any] = None) -> Optional[Dict[str, Any]]:
    """Baixa um arquivo da URL do Vista e sobe pro nosso S3.

    Retorna {'s3_key': str, 'size': int, 'mime': str, 'ext': str} ou None se falhou.

    kind: 'fotos' | 'anexos' | 'fotoemp' | 'videos' (subpasta no S3)
    """
    if not vista_url or not isinstance(vista_url, str) or not vista_url.startswith("http"):
        return None
    import mimetypes
    import requests as _rq

    s3, bucket, prefix = _get_s3()
    if not bucket:
        logger.warning("S3_BUCKET nao configurado - pulando upload")
        return None

    try:
        sess = session or _rq
        r = sess.get(vista_url, timeout=60)
        if r.status_code != 200:
            logger.warning("download falhou %s: status=%s", vista_url[:80], r.status_code)
            return None
        raw = r.content
        if not raw:
            return None
    except Exception as exc:
        logger.warning("download erro %s: %s", vista_url[:80], exc)
        return None

    # Determina extensao e mime
    url_limpa = vista_url.split("?")[0]
    ext = url_limpa.rsplit(".", 1)[-1].lower() if "." in url_limpa.rsplit("/", 1)[-1] else "bin"
    if len(ext) > 6: ext = "bin"  # extensoes esquisitas
    mime, _ = mimetypes.guess_type(url_limpa)
    if not mime:
        mime = "application/octet-stream"

    # S3 key: {prefix}/{kind}-crm/{prop_code}/{uuid}.{ext}
    s3_key = f"{prefix}/{kind}-crm/{prop_code}/{_uuid_mod.uuid4().hex}.{ext}"

    try:
        s3.put_object(Bucket=bucket, Key=s3_key, Body=raw, ContentType=mime)
    except Exception as exc:
        logger.warning("upload S3 erro %s: %s", s3_key, exc)
        return None

    return {"s3_key": s3_key, "size": len(raw), "mime": mime, "ext": ext}


def inserir_fotos(cur, prop_id: str, det: Dict[str, Any], uploaded_by: str) -> int:
    """Insere fotos do imovel BAIXANDO do Vista e SUBINDO pro nosso S3.

    Salva apenas a S3-key em file_url (sem URL do Vista). Garante que o CRM
    nao depende mais do Vista para servir fotos.

    Pega:
      - Foto: dict ou list com fotos do imovel
      - FotoEmpreendimento: dict ou list com fotos do empreendimento
      - FotoDestaque / FotoDestaquePequena: top-level (single)
    """
    cur.execute("DELETE FROM property_photos WHERE property_id = %s", (prop_id,))

    # Pega code do imovel pra usar no path do S3
    cur.execute("SELECT code FROM properties WHERE id = %s", (prop_id,))
    r = cur.fetchone()
    prop_code = str(r[0]) if r else prop_id

    inseridas = 0
    order_idx = 0

    def _inserir_uma(url: str, descricao: Optional[str], tipo: Optional[str],
                     is_destaque: bool, fonte: str) -> None:
        nonlocal inseridas, order_idx
        if not (isinstance(url, str) and url.startswith("http")):
            return
        # BAIXA do Vista e SOBE pro nosso S3
        meta = baixar_e_subir_pro_s3(url, prop_code, kind="fotos")
        if not meta:
            return  # nao consegue salvar sem S3
        order_idx += 1
        nome = _safe_filename_from_url(url, f"foto_{order_idx}.{meta['ext']}")
        cur.execute("""
            INSERT INTO property_photos (
                id, property_id, filename, file_url, file_size, mime_type,
                title, description, order_index, is_primary, uploaded_by_id, created_at,
                show_on_site, custom_flags
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW(), true, '{}'::jsonb)
        """, (
            str(uuid.uuid4()), prop_id, nome, meta["s3_key"], meta["size"], meta["mime"],
            _to_str(tipo, 255),
            _to_str(descricao),
            order_idx,
            is_destaque,
            uploaded_by,
        ))
        inseridas += 1

    # 1) Fotos do imovel
    fotos = exp._normalizar_lista(det.get("Foto"))
    for i, foto in enumerate(fotos, start=1):
        url = foto.get("FotoOriginal") or foto.get("Foto") or foto.get("FotoPequena")
        _inserir_uma(
            url, foto.get("Descricao"), foto.get("Tipo"),
            is_destaque=(_to_bool(foto.get("Destaque")) or i == 1),
            fonte="Foto",
        )

    # 2) Fotos de empreendimento (lista)
    fotos_emp = exp._normalizar_lista(det.get("FotoEmpreendimento"))
    for foto in fotos_emp:
        url = foto.get("Foto") or foto.get("FotoOriginal") or foto.get("FotoPequena")
        _inserir_uma(
            url, foto.get("Descricao") or "Foto do empreendimento",
            foto.get("Tipo") or "empreendimento",
            is_destaque=False, fonte="FotoEmpreendimento",
        )

    # 3) FotoDestaque / FotoDestaqueEmpreendimento (top-level URLs)
    for key, label in (("FotoDestaque", "Foto destaque"),
                        ("FotoDestaqueEmpreendimento", "Foto destaque empreendimento")):
        url = _to_str(det.get(key))
        if url and url.startswith("http"):
            _inserir_uma(url, label, key, is_destaque=False, fonte=key)

    return inseridas


def inserir_videos(cur, prop_id: str, det: Dict[str, Any], uploaded_by: str) -> int:
    """Insere videos."""
    cur.execute("DELETE FROM property_videos WHERE property_id = %s", (prop_id,))

    videos = exp._normalizar_lista(det.get("Video"))
    inseridos = 0
    for idx, v in enumerate(videos, start=1):
        url = _to_str(v.get("Video"))
        if not url: continue
        cur.execute("""
            INSERT INTO property_videos (
                id, property_id, external_url, provider, title, description,
                order_index, is_primary, uploaded_by_id, created_at
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, NOW())
        """, (
            str(uuid.uuid4()), prop_id, url,
            _to_str(v.get("Tipo"), 50),
            _to_str(v.get("Descricao"), 255),
            _to_str(v.get("DescricaoWeb")),
            idx,
            _to_bool(v.get("Destaque")) or idx == 1,
            uploaded_by,
        ))
        inseridos += 1
    return inseridos


def inserir_documentos(cur, prop_id: str, det: Dict[str, Any], uploaded_by: str) -> int:
    """Insere documentos/anexos."""
    cur.execute("DELETE FROM property_documents WHERE property_id = %s", (prop_id,))

    anexos = exp._normalizar_lista(det.get("Anexo"))
    inseridos = 0
    for idx, a in enumerate(anexos, start=1):
        url = _to_str(a.get("Anexo")) or _to_str(a.get("Arquivo"))
        if not url: continue
        nome = _safe_filename_from_url(url, f"anexo_{idx}")[:255]
        cur.execute("""
            INSERT INTO property_documents (
                id, property_id, name, description, filename, file_url,
                file_size, mime_type, uploaded_by_id, created_at, updated_at
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, NOW(), NOW())
        """, (
            str(uuid.uuid4()), prop_id, nome[:255],
            _to_str(a.get("Descricao")),
            nome, url, 0, "application/octet-stream",
            uploaded_by,
        ))
        inseridos += 1
    return inseridos


def inserir_historico(cur, prop_id: str, det: Dict[str, Any], user_id: str) -> int:
    """Insere prontuarios do Vista em property_history (preservando o que foi feito).

    Para cada prontuario:
      - action: o Assunto real do prontuario (ex: "Inclusao de anexos",
        "Atualizacao de cadastro", "Proposta de venda", "CONTINUA A VENDA")
      - payload: dict com TODOS os campos do prontuario do Vista, sem perder
        nada (texto/detalhes, corretor, datas, valores, status, etc).
      - created_at: data/hora REAL do prontuario no Vista.

    Schema: id, property_id, action, payload jsonb, created_by, created_at.
    """
    # Limpa TODO historico anterior do imovel - re-imports tem que refletir o
    # estado atual do Vista. Filtrar por created_by deixa orfaos de placeholders
    # antigos (ex: "Corretor Vista 243" virou "Cristiane Freyer").
    cur.execute("DELETE FROM property_history WHERE property_id = %s", (prop_id,))

    pront = det.get("prontuarios")
    if not isinstance(pront, dict):
        return 0

    # Ordena por data + hora para inserir cronologicamente
    items = []
    for k, p in pront.items():
        if isinstance(p, dict):
            data_str = _to_str(p.get("Data")) or ""
            hora_str = _to_str(p.get("Hora")) or ""
            ts = None
            try:
                if data_str and hora_str:
                    ts = datetime.strptime(f"{data_str} {hora_str}", "%Y-%m-%d %H:%M:%S")
                elif data_str:
                    ts = datetime.strptime(data_str, "%Y-%m-%d")
            except ValueError: pass
            items.append((ts or datetime.now(), k, p))
    items.sort(key=lambda x: x[0])

    inseridos = 0
    for ts, k, p in items:
        # Resolve corretor REAL deste prontuario (cada prontuario pode ser de
        # corretor diferente). Fallback: o user_id passado (do imovel).
        prontuario_user_id = resolver_corretor_do_prontuario(cur, p, user_id)

        # action = assunto real do prontuario (o que foi feito)
        action = _to_str(p.get("Assunto"), 100) or "Prontuario"

        # payload preserva TUDO que o Vista respondeu
        payload = {
            "assunto":          _to_str(p.get("Assunto")),
            "texto":            _to_str(p.get("Texto")) or "",
            "data":             _to_str(p.get("Data")),
            "hora":             _to_str(p.get("Hora")),
            "corretor":         _to_str(p.get("Corretor")),
            "codigo_corretor":  _to_str(p.get("CodigoCorretor")),
            "anunciado":        _to_str(p.get("Anunciado")),
            "retranca":         _to_str(p.get("Retranca")),
            "proposta":         _to_str(p.get("PROPOSTA")),
            "valor_proposta":   _to_float(p.get("ValorProposta")),
            "veiculo_publicado": _to_str(p.get("VeiculoPublicado")),
            "data_anuncio":     _to_str(p.get("DataAnuncio")),
            "data_inicio":      _to_str(p.get("Datainicio")),
            "status":           _to_str(p.get("Status")),
            "status_imovel":    _to_str(p.get("Statusdoimóvel")),
            "pendente":         _to_str(p.get("Pendente")),
            "privado":          _to_str(p.get("Privado")),
            "cliente":          _to_str(p.get("Cliente")),
            "solicitante_chave": _to_str(p.get("SolicitanteChave")),
            "bairro":           _to_str(p.get("Bairro")),
            "codigo_prontuario": _to_str(p.get("Codigo")),
            "fonte": "vista_import",
        }
        # remove chaves None para nao poluir o jsonb
        payload = {k2: v for k2, v in payload.items() if v not in (None, "")}

        cur.execute("""
            INSERT INTO property_history (
                id, property_id, action, payload, created_by, created_at
            ) VALUES (%s, %s, %s, %s::jsonb, %s, %s)
        """, (str(uuid.uuid4()), prop_id, action, json.dumps(payload, ensure_ascii=False),
              prontuario_user_id, ts))
        inseridos += 1
    return inseridos


# ============================================================
# CUSTOM FIELDS - mapping Vista -> field_name do banco
# IMPORTANTE: NAO cria campos novos. So popula os que ja existem.
# ============================================================

import unicodedata

def _slug(s: Any) -> str:
    """Normaliza pra comparacao: remove acentos, lowercase, soh letras/digitos."""
    if not s: return ""
    s = unicodedata.normalize("NFKD", str(s)).encode("ascii", "ignore").decode()
    return "".join(c for c in s.lower() if c.isalnum())


# Caracteristicas (sub-keys do dict do Vista) -> field_name (BOOLEAN)
# Mapping flexivel: matching feito por slug (sem acentos, sem espacos)
CARAC_MAP = {
    "Adega": "caract_adega",
    "Agua Quente": "caract_agua_quente", "AguaQuente": "caract_agua_quente",
    "Alarme": "caract_alarme",
    "Ar Central": "caract_ar_central", "ArCentral": "caract_ar_central",
    "Ar Condicionado": "caract_ar_condicionado", "ArCondicionado": "caract_ar_condicionado",
    "Armarios Embutidos": "caract_armarios_embutidos", "ArmarioEmbutido": "caract_armarios_embutidos",
    "Area Servico": "caract_area_servico", "AreaServico": "caract_area_servico",
    "Banheiro Social": "caract_banheiro_social", "BanheiroSocial": "caract_banheiro_social",
    "Banheiro Empregada": "caract_banheiro_empregada", "BanheiroEmpregada": "caract_banheiro_empregada",
    "BanheiroAuxiliar": "caract_banheiro_empregada", "WCEmpregada": "caract_banheiro_empregada",
    "Bar": "caract_bar",
    "Churrasqueira": "caract_churrasqueira",
    "Copa": "caract_copa",
    "Copa Cozinha": "caract_copa_cozinha", "CopaCozinha": "caract_copa_cozinha",
    "Cozinha": "caract_cozinha",
    "Cozinha Americana": "caract_cozinha_americana", "CozinhaAmericana": "caract_cozinha_americana",
    "Cozinha Planejada": "caract_cozinha_planejada", "CozinhaPlanejada": "caract_cozinha_planejada",
    "CozinhaMontada": "caract_cozinha_planejada", "CozinhaComTanque": "caract_cozinha",
    "Deck": "caract_deck",
    "Dependencia de Empregada": "caract_dependencia_empregada",
    "DependenciaDeEmpregada": "caract_dependencia_empregada",
    "DependenciadeEmpregada": "caract_dependencia_empregada",
    "Despensa": "caract_despensa",
    "Dormitorio com Armarios": "caract_dormitorio_armarios",
    "DormitorioComArmario": "caract_dormitorio_armarios",
    "Edicula": "caract_edicula",
    "Escritorio": "caract_escritorio",
    "Espera Split": "caract_espera_split",
    "Estar Intimo": "caract_estar_intimo", "EstarIntimo": "caract_estar_intimo",
    "Reformado": "caract_reformado",
    "Gerador Energia": "caract_gerador_energia", "GeradorEnergia": "caract_gerador_energia",
    "Hall": "caract_hall",
    "Home Theater": "caract_home_theater", "HomeTheater": "caract_home_theater",
    "Jardim Inverno": "caract_jardim_inverno", "JardimInverno": "caract_jardim_inverno",
    "Hidromassagem": "caract_hidromassagem",
    "Lareira": "caract_lareira",
    "Lavabo": "caract_lavabo",
    "Mobiliado": "caract_mobiliado",
    "Piscina": "caract_piscina",
    "Piso Elevado": "caract_piso_elevado", "PisoElevado": "caract_piso_elevado",
    "Quintal": "caract_quintal",
    "Sacada": "caract_sacada",
    "Sacada com Churrasqueira": "caract_sacada_churrasqueira",
    "SacadaComChurrasqueira": "caract_sacada_churrasqueira",
    "Sala com Armarios": "caract_sala_armarios", "SalaComArmarios": "caract_sala_armarios",
    "Sala Jantar": "caract_sala_jantar", "SalaJantar": "caract_sala_jantar",
    "Sala TV": "caract_sala_tv", "SalaTV": "caract_sala_tv",
    "Sauna": "caract_sauna",
    "Semi Mobiliado": "caract_semi_mobiliado", "SemiMobiliado": "caract_semi_mobiliado",
    "Split": "caract_split",
    "Suite Master": "caract_suite_master", "SuiteMaster": "caract_suite_master",
    "Terraco": "caract_terraco",
    "Vista Panoramica": "caract_vista_panoramica", "VistaPanoramica": "caract_vista_panoramica",
    "Vista Mar": "caract_vista_mar", "VistaMar": "caract_vista_mar",
    "Gradeado": "caract_gradeado",
    "Placa Solar": "caract_placa_solar", "PlacaSolar": "caract_placa_solar",
    "Airbnb": "caract_airbnb",
    "PCD": "caract_pcd",
}

# Top-level Vista -> field_name + tipo
# Tipos: 'bool' | 'text' | 'number' | 'currency' | 'select' | 'chips' | 'textarea' | 'date' | 'url'
TOPLEVEL_MAP = [
    # ===== Identificacao (BOOLEAN) =====
    ("AltoPadrao",                  "alto_padrao",        "bool"),
    ("Lancamento",                  "lancamento",         "bool"),
    ("Exclusivo",                   "exclusivo",          "bool"),
    ("TemPlaca",                    "tem_placa",          "bool"),
    ("Terreo",                      "terreo",             "bool"),
    ("AceitaDacao",                 "aceita_dacao",       "bool"),
    ("ExibirNoSite",                "exibir_no_site",     "bool"),
    ("DestaqueWeb",                 "destaque_web",       "bool"),
    ("SuperDestaqueWeb",            "super_destaque",     "bool"),
    # ===== Identificacao (TEXT/NUMBER/DATE/SELECT) =====
    ("Empreendimento",              "nome_condominio",    "text"),
    ("AndarDoApto",                 "andar",              "number"),
    ("AnoConstrucao",               "ano_construcao",     "number"),
    ("DataLiberacao",               "data_liberacao",     "date"),
    ("DataEntrega",                 "data_entrega",       "date"),
    ("Ocupacao",                    "ocupacao",           "select"),
    ("Situacao",                    "situacao",           "select"),
    ("EstadoConservacaoImovel",     "conservacao",        "select"),
    # ===== Endereco =====
    ("Bloco",                       "bloco",              "text"),
    ("TipoEndereco",                "tipo_logradouro",    "select"),
    ("Imediacoes",                  "imediacoes",         "textarea"),
    # ===== Quantidades (NUMBER) =====
    ("QtdVarandas",                 "varandas",           "number"),
    ("Salas",                       "salas",              "number"),
    # ===== Atributos (NUMBER/TEXT/CHIPS) =====
    ("Closet",                      "closet",             "number"),
    ("LivingAmbientes",             "living",             "number"),
    ("HidroSuite",                  "hidromassagem",      "number"),
    ("PisoSala",                    "piso_area_social",   "text"),
    ("PisoDormitorio",              "piso_dormitorios",   "text"),
    ("FacePredio",                  "face_predio",        "chips"),
    ("Face",                        "face_unidade",       "chips"),
    # ===== Valores e Areas (CURRENCY) =====
    ("ValorIptu",                   "iptu",               "currency"),
    ("ValorCondominio",             "condominio",         "currency"),
    # ===== Dados do Proprietario =====
    ("Proprietario",                "nome_do_proprietario", "text"),
    # ===== Dados do Edificio =====
    ("AdministradoraCondominio",    "administradora",     "text"),
    ("Construtora",                 "construtora",        "select"),
    ("PadraoConstrucao",            "perfil_construcao",  "select"),
    ("EstadoConservacaoEdificio",   "estado_edificio",    "text"),
    ("Fachada",                     "tipo_fachada",       "text"),
    ("Andares",                     "numero_de_andares",  "number"),
    ("AptosAndar",                  "imoveis_por_andar",  "number"),
    ("AptosEdificio",               "total_de_imoveis",   "number"),
    ("Elevadores",                  "qtd_elevadores",     "number"),
    ("OrientacaoSolar",             "orientacao_solar",   "select"),
    ("DescricaoEmpreendimento",     "descricao_do_empreendimento", "textarea"),
    # ===== Dados Comerciais =====
    ("Matricula",                   "matricula",          "text"),
    ("Zona",                        "zona",               "text"),
    ("AceitaFinanciamento",         "possibilidade_financiamento", "bool"),
    ("InformacaoVenda",             "condicoes_negociacao", "textarea"),
    ("ValorLivreProprietario",      "proprietario_livre", "currency"),
    ("PercentualComissao",          "percentual_proprietario", "number"),
    ("SaldoDivida",                 "saldo_devedor",      "currency"),
    ("ValorComissao",               "comissao",           "currency"),
    # ===== Controle de Chaves =====
    ("ResponsavelReserva",          "responsavel_reserva", "text"),
    ("ZeladorNome",                 "zelador_visitas",    "text"),
    ("ZeladorTelefone",             "telefone_zelador",   "text"),
    ("NumeroChave",                 "numero_chave",       "text"),
    ("StatusChave",                 "status_chave",       "text"),
    ("CorretorChave",               "corretor_chave",     "text"),
    ("DataChave",                   "data_chave",         "date"),
    ("RecepcionistaChave",          "recepcionista",      "text"),
    ("Visita",                      "observacoes_visitas", "textarea"),
    # ===== Permuta =====
    ("ValorPermutaImovel",          "valor_permuta",      "currency"),
    ("AceitaPermutaCarro",          "permuta_veiculo",    "bool"),
    ("AceitaPermuta",               "permuta_imovel",     "bool"),
    # ===== Vídeo =====
    ("URLVideo",                    "link_do_video",      "url"),
    ("VideoDestaque",               "link_do_video",      "url"),
    # ===== Tipo de Vaga (numero do box) =====
    ("GaragemNumeroBox",            "numero_box",         "text"),
    # ===== Fallbacks: Vista pode usar diferentes nomes =====
    ("Observacoes",                 "observacoes_internas", "textarea"),
    ("ObsLocacao",                  "observacoes_internas", "textarea"),
    ("ObsVenda",                    "observacoes_internas", "textarea"),
    ("Fachada",                     "tipo_fachada",       "text"),
    # Fotografo (MULTI_SELECT - passa como JSON array)
    ("Fotografo",                   "fotografo",          "multiselect"),
    # Pavimentos como fallback de Andares
    ("Pavimentos",                  "numero_de_andares",  "number"),
]


# ===== Itens de Infraestrutura (30) - vem do dict InfraEstrutura =====
INFRA_MAP = {
    "AquecimentoCentral":           "infra_aquecimento_central",
    "Aquecimento Central":          "infra_aquecimento_central",
    "Brinquedoteca":                "infra_brinquedoteca",
    "ChurrasqueiraCondominio":      "infra_churrasqueira_coletiva",
    "Churrasqueira Condominio":     "infra_churrasqueira_coletiva",
    "CircuitoFechadoTV":            "infra_circuito_tv",
    "Circuito Fechado TV":          "infra_circuito_tv",
    "CondominioFechado":            "infra_condominio_fechado",
    "Condominio Fechado":           "infra_condominio_fechado",
    "Elevador":                     "infra_elevador",
    "ElevadorServico":              "infra_elevador_servico",
    "Elevador Servico":             "infra_elevador_servico",
    "EmpresaDeMonitoramento":       "infra_empresa_monitoramento",
    "Empresa De Monitoramento":     "infra_empresa_monitoramento",
    "EmpresaMonitoramento":         "infra_empresa_monitoramento",
    "EntradaServicoIndependente":   "infra_entrada_servico",
    "Entrada Servico Independente": "infra_entrada_servico",
    "EspacoGourmet":                "infra_espaco_gourmet",
    "Espaco Gourmet":               "infra_espaco_gourmet",
    "Estacionamento":               "infra_estacionamento",
    "EstacionamentoVisitantes":     "infra_estacionamento_visitantes",
    "Estacionamento Visitantes":    "infra_estacionamento_visitantes",
    "Guarita":                      "infra_guarita",
    "GasCentral":                   "infra_gas_central",
    "Gas Central":                  "infra_gas_central",
    "Interfone":                    "infra_interfone",
    "PiscinaAquecida":              "infra_piscina_aquecida",
    "Piscina Aquecida":             "infra_piscina_aquecida",
    "PiscinaColetiva":              "infra_piscina_coletiva",
    "Piscina Coletiva":             "infra_piscina_coletiva",
    "PiscinaInfantil":              "infra_piscina_infantil",
    "Piscina Infantil":             "infra_piscina_infantil",
    "Playground":                   "infra_playground",
    "Portaria":                     "infra_portaria",
    "Portaria24Hrs":                "infra_portaria_24h",
    "Portaria 24h":                 "infra_portaria_24h",
    "Portaria24h":                  "infra_portaria_24h",
    "PorteiroEletronico":           "infra_porteiro_eletronico",
    "Porteiro Eletronico":          "infra_porteiro_eletronico",
    "SalaJogos":                    "infra_sala_jogos",
    "Sala Jogos":                   "infra_sala_jogos",
    "SalaFitness":                  "infra_sala_fitness",
    "Sala Fitness":                 "infra_sala_fitness",
    "SalaoFestas":                  "infra_salao_festas",
    "Salao Festas":                 "infra_salao_festas",
    "SegurancaPatrimonial":         "infra_seguranca",
    "Seguranca Patrimonial":        "infra_seguranca",
    "Spa":                          "infra_spa",
    "TerracoColetivo":              "infra_terraco_coletivo",
    "Terraco Coletivo":             "infra_terraco_coletivo",
    "Vigilancia24Horas":            "infra_vigilancia_24h",
    "Vigilancia 24 Horas":          "infra_vigilancia_24h",
    "Zelador":                      "infra_zelador",
}


# ===== Portais (Vista -> banco) =====
# Vista key -> banco field_name
PORTAIS_MAP = [
    ("ZapTipoOferta",               "portal_zap"),
    ("VivaRealPublicationType",     "portal_viva_real"),
    ("OLXFinalidadesPublicadas",    "portal_olx"),
    ("ImovelwebModelo",             "portal_imovelweb"),
    ("ChavesNaMaoDestaque",         "portal_chaves_na_mao"),
    ("123iPublicationType",         "portal_123i"),
    ("MercadoLivreTipoML",          "portal_mercado_livre"),
]


def _normalizar_portal(raw: Any) -> Optional[str]:
    """Normaliza valor do Vista pra opcoes do portal: Nao publicar / Simples / Destaque / Super Destaque."""
    if raw in (None, "", "Nao", False, "0", 0):
        return "Não publicar"
    s = str(raw).strip().lower()
    if s in ("padrao", "padrão", "simples", "normal", "padrao 1", "oferta normal"):
        return "Simples"
    if "super" in s and "destaque" in s:
        return "Super Destaque"
    if "destaque" in s or s == "sim":
        return "Destaque"
    if s in ("nao", "não", "0", "nao publicar", "não publicar"):
        return "Não publicar"
    # default conservador
    return "Simples"


# Construtoras conhecidas (options do banco)
CONSTRUTORAS_VALIDAS = [
    "Cyrela", "Cyrela Goldsztein", "Gafisa", "MRV",
    "Melnick", "Rossi", "Goldsztein", "Tenda",
]

def _match_construtora(raw: Optional[str]) -> Optional[str]:
    """Fuzzy match contra CONSTRUTORAS_VALIDAS. Retorna nome valido."""
    if not raw: return None
    s = str(raw).strip()
    if not s or s.lower() in ("0","nao","não","-","--","sem","nao informado"): return None
    sl = s.lower()
    # Match exato
    for opt in CONSTRUTORAS_VALIDAS:
        if opt.lower() == sl:
            return opt
    # Match palavra-chave (cyrela/melnick/etc dentro do nome)
    for opt in CONSTRUTORAS_VALIDAS:
        opt_l = opt.lower()
        if opt_l in sl or sl in opt_l:
            return opt
    # Sem match - retorna o nome ORIGINAL pra adicionar como nova option no banco
    return s


def _adicionar_option_a_field(cur, field_name: str, novo_nome: str) -> None:
    """Adiciona uma nova option a um campo SELECT.

    THREAD-SAFE: protegido por _GLOBAL_LOCK. Commit imediato pra outros workers verem.
    """
    if not novo_nome or not field_name: return
    with _GLOBAL_LOCK:
        cur.execute("""
            SELECT id, options FROM property_fields
            WHERE name = %s AND is_active = true LIMIT 1
        """, (field_name,))
        r = cur.fetchone()
        if not r: return
        fid, opts = r

        options_list: List[str] = []
        if isinstance(opts, list):
            for o in opts:
                v = o if isinstance(o, str) else (o.get("label") if isinstance(o, dict) else None)
                if v: options_list.append(str(v))
        elif isinstance(opts, dict) and "options" in opts:
            for o in opts.get("options") or []:
                v = o.get("label") if isinstance(o, dict) else o
                if v: options_list.append(str(v))

        # Ja existe? (case-insensitive)
        if any(o.lower() == novo_nome.lower() for o in options_list):
            return

        options_list.append(novo_nome)
        cur.execute("""
            UPDATE property_fields SET options = %s::jsonb, updated_at = NOW()
            WHERE id = %s
        """, (json.dumps(options_list, ensure_ascii=False), fid))
        # Commit imediato para outros workers verem o option novo
        cur.connection.commit()


def _adicionar_option_construtora(cur, novo_nome: str) -> None:
    """Wrapper legacy."""
    _adicionar_option_a_field(cur, "construtora", novo_nome)
    if novo_nome and novo_nome not in CONSTRUTORAS_VALIDAS:
        CONSTRUTORAS_VALIDAS.append(novo_nome)


# Perfil construcao mapping
PERFIL_CONSTRUCAO_MAP = {
    "alto padrao": "Alto Padrão", "alto padrão": "Alto Padrão",
    "intermediario": "Intermediário", "intermediário": "Intermediário",
    "comum": "Comum",
    "popular": "Popular",
    "luxo": "Luxo",
}


# CodigoAgencia (Vista) -> nome (column properties.agency)
AGENCIA_VISTA_BANCO = {
    "4": "URBAN SELECT",
    "5": "URBAN CANOAS",
    "6": "URBAN COMPANY",
}


def _resolve_agencia(codigo_agencia: Any) -> Optional[str]:
    """Mapeia CodigoAgencia -> nome valido (URBAN SELECT/CANOAS/COMPANY)."""
    if codigo_agencia in (None, "", 0, "0"): return None
    return AGENCIA_VISTA_BANCO.get(str(codigo_agencia).strip())

# Normalizacao de valores SELECT (Vista -> banco)
SELECT_VALUE_MAP = {
    "ocupacao": {
        "Proprietário": "Proprietário", "Proprietario": "Proprietário",
        "Inquilino": "Inquilino",
        "Desocupado": "Desocupado", "Vazio": "Desocupado",
        "Ocupado por Terceiros": "Ocupado por Terceiros",
        "Terceiros": "Ocupado por Terceiros",
    },
    "situacao": {
        "Novo": "Novo", "Usado": "Usado",
        "Na Planta": "Na Planta", "NaPlanta": "Na Planta",
        "Em Construção": "Em Construção", "Em Construcao": "Em Construção",
        "EmConstrucao": "Em Construção",
        "Construção": "Em Construção", "Construcao": "Em Construção",
        "Lançamento": "Lançamento", "Lancamento": "Lançamento",
    },
    "conservacao": {
        "Otimo": "Ótimo", "Ótimo": "Ótimo", "OTIMO": "Ótimo",
        "MUITO BOM": "Ótimo", "Muito Bom": "Ótimo",
        "Bom": "Bom", "BOM": "Bom",
        "Regular": "Regular", "REGULAR": "Regular",
        "Ruim": "Necessita Reparos", "Necessita Reparos": "Necessita Reparos",
        "Em Reforma": "Em Reforma", "EmReforma": "Em Reforma",
    },
    "tipo_logradouro": {
        "Rua": "Rua", "RUA": "Rua",
        "Avenida": "Avenida", "AV": "Avenida", "Av": "Avenida",
        "Travessa": "Travessa",
        "Alameda": "Alameda",
        "Praça": "Praça", "Praca": "Praça",
        "Rodovia": "Rodovia",
        "Estrada": "Estrada",
        "Largo": "Largo",
    },
}

# Direcoes validas pra CHIPS (face_predio, face_unidade, orientacao_solar)
DIRECOES_VALIDAS = {"Norte","Sul","Leste","Oeste","Nordeste","Noroeste","Sudeste","Sudoeste"}

# Normaliza string de direcao
def _normalizar_direcao(s: Optional[str]) -> Optional[str]:
    if not s: return None
    s = str(s).strip()
    # Substitui acentos / caps
    norm = s.replace("à","a").replace("á","a").replace("é","e").replace("í","i") \
            .replace("ó","o").replace("ú","u").lower()
    mapa = {"norte":"Norte","sul":"Sul","leste":"Leste","oeste":"Oeste",
            "nordeste":"Nordeste","noroeste":"Noroeste",
            "sudeste":"Sudeste","sudoeste":"Sudoeste",
            "n":"Norte","s":"Sul","l":"Leste","o":"Oeste","e":"Leste","w":"Oeste"}
    return mapa.get(norm)


# Cache de field ids (1 query por field_name)
_field_id_cache: Dict[str, Optional[str]] = {}


def get_field_id(cur, field_name: str) -> Optional[str]:
    """Pega o id do property_field. NAO cria. Thread-safe via cache + lock."""
    if field_name in _field_id_cache:
        return _field_id_cache[field_name]
    with _GLOBAL_LOCK:
        if field_name in _field_id_cache:
            return _field_id_cache[field_name]
        cur.execute("SELECT id FROM property_fields WHERE name = %s AND is_active = true LIMIT 1",
                    (field_name,))
        r = cur.fetchone()
        fid = str(r[0]) if r else None
        _field_id_cache[field_name] = fid
        return fid


def upsert_custom_value(cur, field_id: str, prop_id: str, value: str, index: int = 0) -> None:
    """UPSERT em property_field_values. PK composta: (property_id, field_id, index)."""
    cur.execute("""
        INSERT INTO property_field_values
            (property_id, field_id, value, index, created_at, updated_at)
        VALUES (%s, %s, %s, %s, NOW(), NOW())
        ON CONFLICT (property_id, field_id, index) DO UPDATE
            SET value = EXCLUDED.value, updated_at = NOW()
    """, (prop_id, field_id, value, index))


def _set_field(cur, prop_id: str, field_name: str, value: str) -> bool:
    cf_id = get_field_id(cur, field_name)
    if not cf_id:
        return False
    upsert_custom_value(cur, cf_id, prop_id, value)
    return True


# ============================================================
# Pipeline TOTAL (cobre 299/299 campos do Rafa via mapeamento_total_vista_rafa)
# ============================================================


def _achatar_vista(det: Dict[str, Any]) -> Dict[str, Any]:
    """Achata estrutura aninhada do Vista para o formato que `mtv.resolve` espera.

    Caracteristicas e InfraEstrutura no Vista NAO sao subgrupos - vem como campos
    DIRETOS no top-level. Esta funcao detecta isso de duas formas:

      1. Dict aninhado: det['Caracteristicas'] = {'Adega': 'Sim'} -> 'carac.Adega'
      2. Campos top-level com nomes conhecidos (CARAC_TOPLEVEL / INFRA_TOPLEVEL):
         det['Adega'] = 'Sim' -> 'carac.Adega'
      3. Lista vinda do schema (__schema_carac__/__schema_infra__): se o caller
         passar a lista de nomes, propaga direto.
    """
    plano: Dict[str, Any] = dict(det)

    # === Caso 1: Caracteristicas / InfraEstrutura como dict aninhado ===
    carac = det.get("Caracteristicas")
    if isinstance(carac, dict):
        for k, v in carac.items():
            plano[f"carac.{k}"] = v
    infra = det.get("InfraEstrutura")
    if isinstance(infra, dict):
        for k, v in infra.items():
            plano[f"infra.{k}"] = v

    # === Caso 2: nomes conhecidos no top-level (export Vista achatado) ===
    # Lista de nomes que sao caracteristicas no schema do Vista
    for nome in CARAC_TOPLEVEL:
        if nome in det and f"carac.{nome}" not in plano:
            plano[f"carac.{nome}"] = det[nome]
    for nome in INFRA_TOPLEVEL:
        if nome in det and f"infra.{nome}" not in plano:
            plano[f"infra.{nome}"] = det[nome]

    # === Caso 3: schema injetado dinamicamente pelo caller ===
    schema_carac = det.get("__schema_carac__") or []
    schema_infra = det.get("__schema_infra__") or []
    for nome in schema_carac:
        if nome in det and f"carac.{nome}" not in plano:
            plano[f"carac.{nome}"] = det[nome]
    for nome in schema_infra:
        if nome in det and f"infra.{nome}" not in plano:
            plano[f"infra.{nome}"] = det[nome]

    # proprietarios: pega primeiro item (chave mais baixa) como representante
    propr = det.get("proprietarios")
    primeiro_propr: Optional[Dict[str, Any]] = None
    if isinstance(propr, dict) and propr:
        try:
            primeiro_propr = propr[sorted(propr.keys())[0]] if propr else None
        except Exception:
            primeiro_propr = next((v for v in propr.values() if isinstance(v, dict)), None)
    elif isinstance(propr, list) and propr:
        primeiro_propr = propr[0] if isinstance(propr[0], dict) else None
    if isinstance(primeiro_propr, dict):
        for k, v in primeiro_propr.items():
            plano[f"proprietarios.{k}"] = v

    # Mesma coisa para Agencia/Corretor se vierem como dict
    for grupo in ("Agencia", "Corretor"):
        sub = det.get(grupo)
        if isinstance(sub, dict):
            for k, v in sub.items():
                plano[f"{grupo}.{k}"] = v

    return plano


# Nomes do grupo `carac` do Vista (vem como top-level no JSON, mas mapeamento
# usa prefixo carac.X)
CARAC_TOPLEVEL = frozenset([
    "Adega", "AguaQuente", "Airbnb", "Alarme", "AntenaParabolica", "AquecimentoEletrico",
    "ArCentral", "ArCondicionado", "AreaServico", "ArmarioEmbutido",
    "BanheiroAuxiliar", "BanheiroSocial", "Bar", "Calefacao", "CanaletasNoRodape",
    "CercaEletrica", "Churrasqueira", "ConstrucaoAlvenaria",
    "Copa", "CopaCozinha", "Cozinha", "CozinhaAmericana", "CozinhaComTanque",
    "CozinhaMontada", "CozinhaPlanejada", "Deck",
    "DependenciaDeEmpregada", "DependenciadeEmpregada", "Despensa",
    "DormitorioComArmario", "Edicula", "Escritorio", "EsperaSplit", "EstarIntimo",
    "Forro", "Gabinete", "Gradeado", "Hidromassagem", "HomeTheater",
    "JardimInverno", "Lareira", "Lavabo", "Leste", "Living", "LivingHall",
    "Mezanino", "Mobiliado", "Monitoramento", "Norte", "Oeste", "Patio",
    "PCD", "Piscina", "PisoElevado", "PlacaSolar", "Porao", "Quintal",
    "Reformado", "Sacada", "SacadaComChurrasqueira", "Sala", "SalaArmarios",
    "SalaEstar", "SalaJantar", "SalaTV", "Sauna", "SemiMobiliado", "Sotao",
    "Split", "SuiteMaster", "Sul", "Terraco", "TVCabo", "VigiaExterno",
    "VigiaInterno", "VistaMar", "VistaPanoramica", "Vitrine", "WCEmpregada",
])

# Nomes do grupo `infra` do Vista
INFRA_TOPLEVEL = frozenset([
    "Agua", "AquecimentoCentral", "Bicicletario", "Brinquedoteca", "CabineDeForca",
    "Canil", "CapacidadePiso", "ChurrasqueiraCondominio", "CircuitoFechadoTV",
    "CondominioFechado", "ConstrucaoMista", "Deposito", "EdificioResidencial",
    "Elevador", "ElevadorServico", "EmpresaDeMonitoramento", "EnergiaEletrica",
    "EnergiaTrifasica", "EntradaServicoIndependente", "EspacoGourmet",
    "Estacionamento", "EstacionamentoVisitantes", "Garagem", "GaragemCoberta",
    "GasCentral", "GeradorEnergia", "Gradil", "Guarita", "Heliponto", "Interfone",
    "Jardim", "Junker", "Lavanderia", "Marquise", "NomeEmpresaMonitoramento",
    "OnibusProximo", "Pavimentacao", "Pilotis", "PiscinaAquecida", "PiscinaColetiva",
    "PiscinaInfantil", "PistaCaminhada", "Playground", "PocoArtesiano", "Portaria",
    "Portaria24Hrs", "PortariaBlindada", "PorteiroEletronico", "PortoesComEclusa",
    "PossuiViabilidade", "QuadraEsportes", "QuadraPoliEsportiva", "QuadraTenis",
    "Quiosque", "RedeEsgoto", "SalaDeRecepcao", "SalaFitness", "SalaoFestas",
    "SalaoJogos", "SaunaCondominio", "SegurancaPatrimonial", "Shaft", "Spa",
    "TerracoColetivo", "Tubulacao", "Vigilancia24Horas", "Zelador",
])


def _valor_para_storage(field_name: str, tipo: str, raw: Any) -> Optional[str]:
    """Converte o valor cru do Vista para a string que vai em
    property_custom_field_values.value, respeitando o tipo do campo.

    Retorna None se nao deve ser inserido (valor vazio/invalido).
    """
    if raw is None or raw == "":
        return None

    if tipo == "BOOLEAN":
        # Grava SEMPRE quando o Vista respondeu valor explicito (Sim/Nao/true/false)
        # para preencher os 137 campos boolean mesmo quando false
        if isinstance(raw, bool):
            return "true" if raw else "false"
        s = str(raw).strip().lower()
        if s in ("sim", "yes", "true", "1", "s", "y", "x"):
            return "true"
        if s in ("nao", "não", "no", "false", "0", "n"):
            return "false"
        return None

    if tipo in ("NUMBER", "CURRENCY"):
        f = _to_float(raw)
        if f is None or f == 0:
            return None
        return str(f)

    if tipo == "DATE":
        d = _to_date(raw)
        return d.strftime("%Y-%m-%d") if d else None

    if tipo == "URL":
        s = str(raw).strip()
        return s if s.startswith("http") else None

    if tipo == "CHIPS":
        # face_*, orientacao_solar: array com 1 direcao normalizada
        if field_name in ("face_predio", "face_unidade", "orientacao_solar"):
            dir_val = _normalizar_direcao(raw)
            return json.dumps([dir_val], ensure_ascii=False) if dir_val else None
        # dormitorios CHIPS: pega como int em array
        if isinstance(raw, list):
            arr = [str(x).strip() for x in raw if x not in (None, "")]
        else:
            arr = [str(raw).strip()] if str(raw).strip() else []
        return json.dumps(arr, ensure_ascii=False) if arr else None

    if tipo == "MULTI_SELECT":
        if isinstance(raw, list):
            arr = [str(x).strip() for x in raw if x not in (None, "")]
        else:
            s = str(raw).strip()
            arr = [s] if s and s not in ("0", "Nao", "Não", "N/A", "-", "--", "Pendente") else []
        return json.dumps(arr, ensure_ascii=False) if arr else None

    if tipo == "SELECT":
        return _resolver_select(field_name, raw)

    if tipo == "MAP":
        if isinstance(raw, dict):
            lat, lng = raw.get("lat"), raw.get("lng")
            if lat is None and lng is None:
                return None
            return json.dumps({"lat": lat, "lng": lng}, ensure_ascii=False)
        return None

    if tipo == "PHOTO":
        s = str(raw).strip()
        return s if s.startswith("http") else None

    if tipo == "ATTACHMENT":
        # Aceita: URL simples (http://...), path (/...), ou JSON estruturado
        # ([{url,name,...}] ou {url,name,...}) usado pelo campo 'outros'.
        if isinstance(raw, (list, dict)):
            return json.dumps(raw, ensure_ascii=False) if raw else None
        s = str(raw).strip()
        if not s:
            return None
        if s.startswith("http") or s.startswith("/") or s.startswith("[") or s.startswith("{"):
            return s
        return None

    if tipo == "SYSTEM":
        if isinstance(raw, (dict, list)):
            return json.dumps(raw, ensure_ascii=False)
        s = str(raw).strip()
        return s or None

    if tipo in ("TEXT", "TEXTAREA"):
        v = str(raw).strip()
        if not v or v in ("0", "0000-00-00", "00/00/0000", "Nao", "Não", "N/A", "-", "--"):
            return None
        return v

    return None


def _resolver_select(field_name: str, raw: Any) -> Optional[str]:
    """Normaliza valor de SELECT para uma das opcoes validas do banco.

    Reaproveita CONSTRUTORAS_VALIDAS, PERFIL_CONSTRUCAO_MAP, SELECT_VALUE_MAP
    e _normalizar_portal definidos mais acima.
    """
    if raw is None or raw == "":
        return None

    if field_name == "construtora":
        return _match_construtora(raw)
    if field_name == "perfil_construcao":
        return PERFIL_CONSTRUCAO_MAP.get(str(raw).strip().lower())
    if field_name == "orientacao_solar":
        return _normalizar_direcao(raw)
    if field_name.startswith("portal_"):
        return _normalizar_portal(raw)

    options_map = SELECT_VALUE_MAP.get(field_name)
    if options_map is not None:
        return options_map.get(str(raw).strip())

    # Sem mapping especifico: devolve o valor como string limpa
    s = str(raw).strip()
    return s or None


def migrar_attachments_pra_s3(cur, prop_id: str) -> int:
    """Apos inserir_custom_fields(): varre TODOS os ATTACHMENT/PHOTO/URL preenchidos
    que ainda tem URL do Vista e faz download+upload+atualiza com S3-key nosso.

    Garante ZERO dependencia do Vista apos importacao.
    """
    cur.execute("SELECT code FROM properties WHERE id = %s", (prop_id,))
    r = cur.fetchone()
    prop_code = str(r[0]) if r else prop_id

    cur.execute("""
        SELECT pfv.field_id, pf.name, pf.type, pfv.value
        FROM property_field_values pfv
        JOIN property_fields pf ON pf.id = pfv.field_id
        WHERE pfv.property_id = %s AND pf.type IN ('ATTACHMENT', 'PHOTO', 'URL')
    """, (prop_id,))
    rows = cur.fetchall()

    def _eh_vista(u: str) -> bool:
        return isinstance(u, str) and u.startswith("http") and (
            "vistahost.com.br" in u or "static.vista" in u or "s3.sa-east-1" in u
        )

    migrados = 0
    for fid, fname, ftype, value in rows:
        if not value:
            continue
        kind = "fotos" if ftype == "PHOTO" else ("videos" if ftype == "URL" else "anexos")
        try:
            data = json.loads(value) if value.startswith("{") else None
        except Exception:
            data = None

        # Caso 1: string URL pura
        if data is None and _eh_vista(value):
            meta = baixar_e_subir_pro_s3(value, prop_code, kind=kind)
            if meta:
                cur.execute(
                    "UPDATE property_field_values SET value=%s, updated_at=NOW() WHERE property_id=%s AND field_id=%s",
                    (meta["s3_key"], prop_id, fid),
                )
                migrados += 1
            continue

        # Caso 2: dict {url, name, ...} com URL Vista
        if isinstance(data, dict):
            url = data.get("url", "")
            if _eh_vista(url):
                meta = baixar_e_subir_pro_s3(url, prop_code, kind=kind)
                if meta:
                    data["url"] = meta["s3_key"]
                    data["size"] = meta["size"]
                    data["mimeType"] = meta["mime"]
                    # Acrescenta extensao ao name se nao tiver
                    nome = data.get("name", "")
                    if nome and not nome.lower().endswith("." + meta["ext"].lower()):
                        nome_com_ext = f"{nome}.{meta['ext']}"
                        data["name"] = nome_com_ext
                        data["filename"] = nome_com_ext
                    cur.execute(
                        "UPDATE property_field_values SET value=%s, updated_at=NOW() WHERE property_id=%s AND field_id=%s",
                        (json.dumps(data, ensure_ascii=False), prop_id, fid),
                    )
                    migrados += 1
    return migrados


def inserir_custom_fields(cur, prop_id: str, det: Dict[str, Any]) -> int:
    """Popula TODOS os 299 custom_fields do Rafa usando o mapeamento total.

    Para cada campo do Rafa em mapeamento_total_vista_rafa.MAPEAMENTO:
      1. resolve() devolve o valor cru a partir do Vista (direto, fallback,
         derivado, anexo ou default)
      2. _valor_para_storage() converte para a string que vai em
         property_custom_field_values.value, aplicando coercao por tipo
      3. _set_field() faz upsert no banco (skip se o field_name nao existir)

    Returns: total de campos efetivamente inseridos/atualizados.
    """
    if not isinstance(det, dict):
        return 0

    det_plano = _achatar_vista(det)
    inseridos = 0

    for field_name, spec in mtv.MAPEAMENTO.items():
        try:
            raw = mtv.resolve(field_name, det_plano)
        except Exception as exc:
            logger.warning("resolve(%s) falhou: %s", field_name, exc)
            continue

        value = _valor_para_storage(field_name, spec.tipo, raw)
        if value is None:
            continue

        # Se SELECT construtora trouxe nome fora da lista, adiciona como nova option
        if field_name == "construtora" and value not in CONSTRUTORAS_VALIDAS:
            try:
                _adicionar_option_construtora(cur, value)
            except Exception as exc:
                logger.warning("nao consegui adicionar construtora '%s': %s", value, exc)

        try:
            if _set_field(cur, prop_id, field_name, value):
                inseridos += 1
        except Exception as exc:
            logger.warning("upsert custom_field %s falhou: %s", field_name, exc)

    return inseridos


def _inserir_custom_fields_legacy(cur, prop_id: str, det: Dict[str, Any]) -> int:
    """Versao legacy (pre-mapeamento-total). Mantida para referencia/rollback.

    Mapeia top-level + Caracteristicas + InfraEstrutura + Portais
    + derivados (ultimo_andar, garagem) baseado no data.json do Vista.
    """
    inseridos = 0

    # 1) Top-level fields (com tratamento por tipo)
    for vista_key, field_name, ftype in TOPLEVEL_MAP:
        v = det.get(vista_key)
        if v is None or v == "":
            continue

        if ftype == "bool":
            # Pula se Nao/0 - so seta quando true
            if not _to_bool(v):
                continue
            value = "true"
        elif ftype in ("number", "currency"):
            f = _to_float(v)
            if f is None or f == 0:
                continue
            value = str(f)
        elif ftype == "select":
            # Trata casos especificos com listas de options conhecidas
            if field_name == "construtora":
                value = _match_construtora(v)
                # Se valor nao bate com options existentes, adiciona como nova option
                if value and value not in CONSTRUTORAS_VALIDAS:
                    _adicionar_option_construtora(cur, value)
            elif field_name == "perfil_construcao":
                norm = str(v).strip().lower()
                value = PERFIL_CONSTRUCAO_MAP.get(norm)
            elif field_name == "orientacao_solar":
                value = _normalizar_direcao(v)
            else:
                # Usa SELECT_VALUE_MAP padrao
                options_map = SELECT_VALUE_MAP.get(field_name, {})
                value = options_map.get(str(v).strip())
            if not value:
                continue
        elif ftype == "chips":
            dir_val = _normalizar_direcao(v)
            if not dir_val: continue
            value = json.dumps([dir_val], ensure_ascii=False)
        elif ftype == "date":
            d = _to_date(v)
            if not d: continue
            value = d.strftime("%Y-%m-%d")
        elif ftype == "url":
            sv = str(v).strip()
            if not sv or not sv.startswith("http"): continue
            value = sv
        elif ftype == "multiselect":
            # MULTI_SELECT: aceita JSON array. Vista geralmente vem como string unica.
            sv = str(v).strip()
            if not sv or sv in ("0","Nao","Não","N/A","-","--","Pendente"): continue
            value = json.dumps([sv], ensure_ascii=False)
        elif ftype in ("text", "textarea"):
            value = str(v).strip()
            if not value: continue
            # Pula valores zero/falsy: "0", "0000-00-00", "Nao", "-"
            if value in ("0", "0000-00-00", "00/00/0000", "Nao", "Não", "N/A", "-", "--"):
                continue
        else:
            continue

        if _set_field(cur, prop_id, field_name, value):
            inseridos += 1

    # 2) Caracteristicas (sub-objeto Vista) - matching por slug
    carac = det.get("Caracteristicas")
    if isinstance(carac, dict):
        # Pre-computa slug -> field_name de CARAC_MAP
        carac_slug = {_slug(k): v for k, v in CARAC_MAP.items()}
        for k, v in carac.items():
            if not _to_bool(v): continue
            field_name = CARAC_MAP.get(k) or carac_slug.get(_slug(k))
            if not field_name: continue
            if _set_field(cur, prop_id, field_name, "true"):
                inseridos += 1

    # 3) InfraEstrutura (sub-objeto Vista) - 30 fields infra_*, matching por slug
    infra = det.get("InfraEstrutura")
    if isinstance(infra, dict):
        infra_slug = {_slug(k): v for k, v in INFRA_MAP.items()}
        for k, v in infra.items():
            if not _to_bool(v): continue
            field_name = INFRA_MAP.get(k) or infra_slug.get(_slug(k))
            if not field_name: continue
            if _set_field(cur, prop_id, field_name, "true"):
                inseridos += 1

    # 4) Portais (9 SELECT)
    for vista_key, banco_key in PORTAIS_MAP:
        if vista_key not in det:
            continue
        portal_value = _normalizar_portal(det.get(vista_key))
        if portal_value and _set_field(cur, prop_id, banco_key, portal_value):
            inseridos += 1

    # 5) Ultimo andar (derivado: AndarDoApto >= Andares total)
    andar = _to_int(det.get("AndarDoApto"))
    pavimentos = _to_int(det.get("Andares")) or _to_int(det.get("AptosEdificio"))
    if andar and pavimentos and pavimentos > 0 and andar >= pavimentos:
        if _set_field(cur, prop_id, "ultimo_andar", "true"):
            inseridos += 1

    # 6) Tipo de Vaga / Garagem (logica corrigida)
    # Soma todas as fontes de vaga
    vagas = _to_int(det.get("Vagas")) or 0
    vagas_cob = _to_int(det.get("VagasCobertas")) or 0
    vagas_desc = _to_int(det.get("VagasDescobertas")) or 0
    estac_vagas = _to_int(det.get("EstacionamentoVagas")) or 0
    garagem_tipo_str = _to_str(det.get("GaragemTipo"))
    total_vagas = vagas + vagas_cob + vagas_desc + estac_vagas
    tem_vaga = total_vagas > 0 or bool(garagem_tipo_str)

    if not tem_vaga:
        if _set_field(cur, prop_id, "garagem_sem_vaga", "true"):
            inseridos += 1
    else:
        # tipo_garagem (Coberta/Descoberta) baseado em VagasCobertas vs VagasDescobertas
        tipo_garagem_val = None
        if vagas_cob > 0 and vagas_desc == 0:
            tipo_garagem_val = "Coberta"
        elif vagas_desc > 0 and vagas_cob == 0:
            tipo_garagem_val = "Descoberta"
        elif garagem_tipo_str:
            gt = garagem_tipo_str.strip().lower()
            if "cober" in gt and "desc" not in gt:
                tipo_garagem_val = "Coberta"
            elif "desc" in gt:
                tipo_garagem_val = "Descoberta"
        if tipo_garagem_val:
            if _set_field(cur, prop_id, "tipo_garagem", tipo_garagem_val):
                inseridos += 1

    return inseridos


def inserir_proprietarios(cur, prop_id: str, det: Dict[str, Any], created_by: str) -> int:
    """Insere proprietarios (cria entidades + linka via property_owner_properties +
    sincroniza properties.owners jsonb)."""
    # Limpa links anteriores
    cur.execute("DELETE FROM property_owner_properties WHERE property_id = %s", (prop_id,))

    propr = det.get("proprietarios")
    if not isinstance(propr, dict): propr = {} if propr is None else propr
    if isinstance(propr, list):
        items = [(i, p) for i, p in enumerate(propr) if isinstance(p, dict)]
    elif isinstance(propr, dict):
        items = [(k, v) for k, v in propr.items() if isinstance(v, dict)]
    else:
        items = []

    inseridos = 0
    owner_ids_criados: List[str] = []
    for k, p in items:
        nome = _to_str(p.get("Nome"), 255)
        if not nome: continue
        cpf = _to_str(p.get("CPFCNPJ"), 50)
        email = _to_str(p.get("EmailResidencial") or p.get("EmailComercial"), 255)
        phone = _to_str(p.get("Celular") or p.get("FonePrincipal") or p.get("FoneResidencial"), 50)

        # Verifica/cria owner com lock global (race em cpf_cnpj entre workers)
        owner_id = None
        with _GLOBAL_LOCK:
            if cpf:
                cur.execute("SELECT id FROM property_owners WHERE cpf_cnpj = %s LIMIT 1", (cpf,))
                r = cur.fetchone()
                if r: owner_id = str(r[0])
            if not owner_id:
                # match por nome+email como fallback (sem CPF)
                if email and nome:
                    cur.execute("""
                        SELECT id FROM property_owners
                        WHERE LOWER(name) = LOWER(%s) AND LOWER(email) = LOWER(%s)
                        LIMIT 1
                    """, (nome, email))
                    r = cur.fetchone()
                    if r: owner_id = str(r[0])
            if not owner_id:
                owner_id = str(uuid.uuid4())
                cur.execute("""
                    INSERT INTO property_owners (id, name, cpf_cnpj, email, phone, is_active, created_by_id, created_at, updated_at)
                    VALUES (%s, %s, %s, %s, %s, true, %s, NOW(), NOW())
                """, (owner_id, nome, cpf, email, phone, created_by))
                # commit imediato para owner ficar visivel pra outros workers
                cur.connection.commit()

        cur.execute("""
            INSERT INTO property_owner_properties (property_owner_id, property_id, created_at)
            VALUES (%s, %s, NOW())
            ON CONFLICT DO NOTHING
        """, (owner_id, prop_id))
        owner_ids_criados.append(owner_id)
        inseridos += 1

    # Sincroniza properties.owners jsonb com objetos COMPLETOS (frontend espera assim)
    if owner_ids_criados:
        cur.execute("""
            SELECT id, name, email, phone, cpf_cnpj
            FROM property_owners
            WHERE id = ANY(%s::uuid[])
        """, (owner_ids_criados,))
        objs = [
            {"id": str(r[0]), "name": r[1], "email": r[2], "phone": r[3], "cpf_cnpj": r[4]}
            for r in cur.fetchall()
        ]
    else:
        objs = []
    cur.execute(
        "UPDATE properties SET owners = %s::jsonb, updated_at = NOW() WHERE id = %s",
        (json.dumps(objs, ensure_ascii=False), prop_id),
    )
    return inseridos


# ============================================================
# Pipeline principal
# ============================================================

CAMPOS_OBRIGATORIOS = ["Codigo", "Categoria", "Bairro", "Cidade"]
CAMPOS_DESEJAVEIS = ["Endereco","Numero","UF","CEP","Latitude","Longitude",
                      "AreaPrivativa","AreaTotal","Dormitorios","Vagas",
                      "ValorVenda","DescricaoWeb"]


def _completude(det: Dict[str, Any]) -> Tuple[bool, int]:
    for c in CAMPOS_OBRIGATORIOS:
        if not det.get(c): return False, 0
    score = sum(1 for c in CAMPOS_DESEJAVEIS if det.get(c) not in (None, "", 0))
    return True, score


def listar_candidatos(s3, bucket: str) -> List[Tuple[int, str]]:
    cands = []
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix=exp.s3_key("imoveis") + "/"):
        for obj in page.get("Contents", []) or []:
            k = obj.get("Key", "")
            if not k.endswith("/data.json"): continue
            sz = obj.get("Size", 0)
            parts = k.split("/")
            if len(parts) >= 4:
                cands.append((sz, parts[-2]))
    cands.sort(reverse=True)
    return cands


def fotos_no_s3(s3, bucket: str, codigo: str) -> int:
    prefix = exp.s3_key("imoveis", codigo, "fotos") + "/"
    resp = s3.list_objects_v2(Bucket=bucket, Prefix=prefix)
    return sum(1 for o in resp.get("Contents", []) or [] if o.get("Size", 0) > 0)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--quantidade", type=int, default=100)
    parser.add_argument("--apenas-com-fotos", action="store_true")
    parser.add_argument("--min-score", type=int, default=6)
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--limpar", action="store_true",
                        help="DELETE de fotos/videos/docs/historico/owner-links das properties existentes antes")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s - %(levelname)s - %(message)s")

    if not DATABASE_URL:
        print("ERRO: DATABASE_URL nao configurada no .env")
        return 1

    print("=" * 70)
    print("Insercao COMPLETA de imoveis no Postgres")
    print(f"  Quantidade alvo : {args.quantidade}")
    print(f"  Min score       : {args.min_score} de {len(CAMPOS_DESEJAVEIS)}")
    print(f"  So com fotos    : {args.apenas_com_fotos}")
    print(f"  Dry run         : {args.dry_run}")
    print("=" * 70)

    s3 = exp.build_s3_client()

    print("\nListando candidatos no S3 (ordenado por completude)...")
    t0 = time.time()
    candidatos = listar_candidatos(s3, exp.S3_BUCKET)
    print(f"  {len(candidatos):,} imoveis (em {time.time()-t0:.1f}s)")
    if not candidatos:
        return 0

    # Conecta no Postgres e prepara bootstrap
    print("\nConectando no Postgres...")
    conn = psycopg.connect(DATABASE_URL, connect_timeout=15)
    conn.autocommit = False
    try:
        with conn.cursor() as cur:
            agent_user_id = get_or_create_import_user(cur)
            print(f"  user 'vista-import': {agent_user_id}")
        conn.commit()

        if args.limpar:
            with conn.cursor() as cur:
                print("\nLimpando dados antigos das tabelas property_*...")
                for t in ("property_photos","property_videos","property_documents",
                          "property_activity_logs","property_owner_properties"):
                    cur.execute(f"DELETE FROM {t}")
                    print(f"  {t}: limpo")
            conn.commit()

        # Loop nos candidatos
        stats = {"inseridos": 0, "rejeitados_essenciais": 0, "rejeitados_score": 0,
                 "rejeitados_sem_foto": 0, "erros": 0,
                 "fotos": 0, "videos": 0, "docs": 0, "historico": 0, "owners": 0,
                 "custom_fields": 0}
        inicio = time.time()

        print(f"\nProcessando candidatos ate {args.quantidade} validos...\n")
        for size, codigo in candidatos:
            if stats["inseridos"] >= args.quantidade:
                break

            try:
                obj = s3.get_object(Bucket=exp.S3_BUCKET,
                                    Key=exp.s3_key("imoveis", codigo, "data.json"))
                det = json.loads(obj["Body"].read())
            except Exception:
                stats["erros"] += 1
                continue

            ok, score = _completude(det)
            if not ok:
                stats["rejeitados_essenciais"] += 1
                continue
            if score < args.min_score:
                stats["rejeitados_score"] += 1
                continue

            if args.apenas_com_fotos:
                if fotos_no_s3(s3, exp.S3_BUCKET, codigo) == 0:
                    stats["rejeitados_sem_foto"] += 1
                    continue

            if args.dry_run:
                stats["inseridos"] += 1
                if stats["inseridos"] % 20 == 0:
                    print(f"  ... {stats['inseridos']}/{args.quantidade} validados (dry-run)")
                continue

            # Faz tudo numa transacao por imovel
            try:
                with conn.cursor() as cur:
                    row = montar_property_row(det, cur, agent_user_id)
                    if not row:
                        stats["rejeitados_essenciais"] += 1
                        continue

                    prop_id = upsert_property(cur, row)

                    # Vincula empreendimento (cria property tipo Empreendimento se nao existir)
                    emp_nome = _to_str(det.get("Empreendimento"))
                    if emp_nome:
                        dev_id = get_or_create_empreendimento(cur, emp_nome, agent_user_id)
                        if dev_id:
                            cur.execute(
                                "UPDATE properties SET development_id = %s WHERE id = %s",
                                (dev_id, prop_id),
                            )

                    stats["fotos"]         += inserir_fotos(cur, prop_id, det, agent_user_id)
                    stats["videos"]        += inserir_videos(cur, prop_id, det, agent_user_id)
                    stats["docs"]          += inserir_documentos(cur, prop_id, det, agent_user_id)
                    stats["historico"]     += inserir_historico(cur, prop_id, det, agent_user_id)
                    stats["owners"]        += inserir_proprietarios(cur, prop_id, det, agent_user_id)
                    stats["custom_fields"] += inserir_custom_fields(cur, prop_id, det)

                    # Vincula tambem na tabela N:N property_agents (agenciador)
                    cur.execute("""
                        INSERT INTO property_agents (property_id, user_id, created_at)
                        VALUES (%s, %s, NOW())
                        ON CONFLICT DO NOTHING
                    """, (prop_id, agent_user_id))

                conn.commit()
                stats["inseridos"] += 1
                if stats["inseridos"] % 10 == 0:
                    elapsed = time.time() - inicio
                    print(f"  ... {stats['inseridos']}/{args.quantidade} | "
                          f"fotos={stats['fotos']} videos={stats['videos']} "
                          f"docs={stats['docs']} historico={stats['historico']} owners={stats['owners']} "
                          f"custom={stats['custom_fields']} "
                          f"({elapsed:.0f}s)")
            except Exception as e:
                conn.rollback()
                stats["erros"] += 1
                logger.warning("Falha no imovel %s: %s", codigo, e)

        elapsed = time.time() - inicio
        print(f"\n{'='*70}")
        print(f"Concluido em {elapsed:.1f}s")
        for k, v in stats.items():
            print(f"  {k:30s}: {v:,}")

        # Confere totais
        with conn.cursor() as cur:
            for tab in ("properties","property_photos","property_videos","property_documents",
                        "property_history","property_owner_properties","property_agents",
                        "property_field_values"):
                cur.execute(f"SELECT COUNT(*) FROM {tab}")
                print(f"  TOTAL {tab:35s}: {cur.fetchone()[0]:,}")
        print("=" * 70)

    finally:
        conn.close()

    return 0


if __name__ == "__main__":
    sys.exit(main())
