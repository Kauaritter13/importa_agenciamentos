"""
Exporta TODOS os imoveis do Vista CRM para o S3 (Tigris compativel).

Estrategia:
  1. Descobre o schema completo via /imoveis/listarcampos (uma vez no startup)
  2. Valida cada subgrupo (Foto, Video, prontuarios, etc) testando contra um imovel
     real - os campos rejeitados sao removidos automaticamente
  3. Para cada imovel, divide a query em multiplas calls GET (URL tem limite ~8KB):
       - chunks de ~80 campos diretos
       - 1 call por subgrupo grande (prontuarios, proprietarios)
       - 1 call agregando subgrupos pequenos (Foto, Video, Anexo, etc)
     e merge no fim.
  4. Baixa todas as fotos da galeria + anexos + foto destaque para o S3.
  5. Salva data.json (imovel completo) + prontuarios.json (historico) + binarios.

Layout no bucket:
  {S3_PREFIX}/imoveis/{codigo}/data.json           <- imovel + Foto + Video + Anexo + Autorizacao + PontoInteresse
  {S3_PREFIX}/imoveis/{codigo}/prontuarios.json    <- historico
  {S3_PREFIX}/imoveis/{codigo}/proprietarios.json  <- proprietarios
  {S3_PREFIX}/imoveis/{codigo}/fotos/{ordem}-{nome}.jpg
  {S3_PREFIX}/imoveis/{codigo}/anexos/{nome}
  {S3_PREFIX}/_schema/vista_schema.json
  {S3_PREFIX}/_schema/vista_validated.json
  {S3_PREFIX}/_manifest/imoveis-*.json
"""

from __future__ import annotations

import argparse
import os
import re
import sys
import json
import time
import logging
import mimetypes
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import urlparse

# Carrega .env automaticamente
try:
    from dotenv import load_dotenv
    load_dotenv(os.path.join(os.path.dirname(os.path.abspath(__file__)), ".env"))
except ImportError:
    pass

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

import boto3
from botocore.config import Config as BotoConfig
from botocore.exceptions import ClientError


# ============================================================
# CONFIGURACAO GLOBAL
# ============================================================

# --- Vista CRM ---
VISTA_API_HOST: str = os.getenv("VISTA_API_HOST", "urbanpoa20097-rest.vistahost.com.br")
VISTA_API_KEY: str = os.getenv("VISTA_API_KEY", "0000ac34876dec13413a9271aa6ca141")

# --- S3 (Tigris) ---
AWS_ACCESS_KEY_ID: str = os.getenv(
    "AWS_ACCESS_KEY_ID",
    "tid_uFGyrkpzFxabPDcxIhipThnKiMhYp_cuzDHTTFZNCHmrj_oIry",
)
AWS_SECRET_ACCESS_KEY: str = os.getenv(
    "AWS_SECRET_ACCESS_KEY",
    "tsec_AgQ-Ya35bh5tHAWUcO5UB7lNuP4+QQsGky8_+CFH-GzzUwA9whTV5lCJWeYnCVTt+tLPzT",
)
AWS_REGION: str = os.getenv("AWS_REGION", "auto")
S3_BUCKET: str = os.getenv("S3_BUCKET", "bucket-or-development-mzssco")
S3_PREFIX: str = os.getenv("S3_PREFIX", "vista-export")
S3_ENDPOINT_URL: str = os.getenv("S3_ENDPOINT_URL", "https://t3.storageapi.dev")

# --- Performance ---
MAX_WORKERS: int = int(os.getenv("MAX_WORKERS", "12"))           # imoveis paralelos
PAGE_SIZE: int = int(os.getenv("PAGE_SIZE", "50"))
PHOTO_WORKERS: int = int(os.getenv("PHOTO_WORKERS", "8"))        # fotos paralelas por imovel
SUBGROUP_WORKERS: int = int(os.getenv("SUBGROUP_WORKERS", "5"))  # subgrupos paralelos por imovel (Foto, Video, prontuarios, etc)
REQUEST_TIMEOUT: int = int(os.getenv("REQUEST_TIMEOUT", "45"))
# Numero maximo de campos diretos por call GET (limite de URL ~ 8KB).
FIELDS_CHUNK_SIZE: int = int(os.getenv("FIELDS_CHUNK_SIZE", "80"))

# --- Backup local ---
LOCAL_BACKUP_DIR: str = os.getenv("LOCAL_BACKUP_DIR", r"C:\Users\kauar\iCloudDrive\CRM Vaultech\vista")

# --- Comportamento ---
SKIP_EXISTING: bool = os.getenv("SKIP_EXISTING", "true").lower() == "true"
# Tamanho minimo (bytes) de data.json no S3 pra considerar um imovel como
# "ja processado". Placeholders sem dados ficam com ~50 bytes - setando 500
# fazemos reprocessar so os que falharam antes.
MIN_DATA_SIZE: int = int(os.getenv("MIN_DATA_SIZE", "500"))
FORCE_PHOTOS: bool = os.getenv("FORCE_PHOTOS", "false").lower() == "true"
VISTA_FILTER_JSON: str = os.getenv("VISTA_FILTER_JSON", "")

# Subgrupos que serao puxados (cada um vira 1 call GET extra). Pode desligar pra ir mais rapido.
INCLUDE_FOTO: bool = os.getenv("INCLUDE_FOTO", "true").lower() == "true"
INCLUDE_FOTO_EMPREENDIMENTO: bool = os.getenv("INCLUDE_FOTO_EMPREENDIMENTO", "true").lower() == "true"
INCLUDE_VIDEO: bool = os.getenv("INCLUDE_VIDEO", "true").lower() == "true"
INCLUDE_ANEXO: bool = os.getenv("INCLUDE_ANEXO", "true").lower() == "true"
INCLUDE_AUTORIZACAO: bool = os.getenv("INCLUDE_AUTORIZACAO", "true").lower() == "true"
INCLUDE_PONTO_INTERESSE: bool = os.getenv("INCLUDE_PONTO_INTERESSE", "true").lower() == "true"
INCLUDE_PRONTUARIOS: bool = os.getenv("INCLUDE_PRONTUARIOS", "true").lower() == "true"
INCLUDE_PROPRIETARIOS: bool = os.getenv("INCLUDE_PROPRIETARIOS", "true").lower() == "true"
DOWNLOAD_FOTOS: bool = os.getenv("DOWNLOAD_FOTOS", "true").lower() == "true"
DOWNLOAD_ANEXOS: bool = os.getenv("DOWNLOAD_ANEXOS", "true").lower() == "true"

LOG_LEVEL: str = os.getenv("LOG_LEVEL", "INFO")


# ============================================================
# Logging
# ============================================================

_WORKER_TAG = os.getenv("WORKER_ID", "")
_log_suffix = f"_w{_WORKER_TAG}" if _WORKER_TAG else ""
logging.basicConfig(
    level=getattr(logging, LOG_LEVEL.upper(), logging.INFO),
    format="%(asctime)s - %(levelname)s - [%(threadName)s] - %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(
            f"vista_to_s3_{datetime.now():%Y%m%d_%H%M%S}{_log_suffix}.log",
            encoding="utf-8",
        ),
    ],
)
logger = logging.getLogger("vista2s3")


# ============================================================
# Validacao
# ============================================================

def _validate_config() -> None:
    missing: List[str] = []
    if not VISTA_API_HOST: missing.append("VISTA_API_HOST")
    if not VISTA_API_KEY: missing.append("VISTA_API_KEY")
    if not S3_BUCKET: missing.append("S3_BUCKET")
    if bool(AWS_ACCESS_KEY_ID) ^ bool(AWS_SECRET_ACCESS_KEY):
        missing.append("AWS_ACCESS_KEY_ID/AWS_SECRET_ACCESS_KEY (defina ambos ou nenhum)")
    if missing:
        raise SystemExit(f"Configuracao invalida. Faltando: {', '.join(missing)}")


# ============================================================
# HTTP session
# ============================================================

def build_http_session() -> requests.Session:
    sess = requests.Session()
    # Retry agressivo pra status transitorios (500/502/503/504) e rate limit (429)
    # Aplicado a tudo - GET de /listar e /detalhes
    retry = Retry(
        total=5, connect=5, read=5, backoff_factor=0.5,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=frozenset(["GET"]),
        raise_on_status=False,
    )
    # Pool generoso para evitar warnings "Connection pool is full"
    pool = max(64, MAX_WORKERS * SUBGROUP_WORKERS + PHOTO_WORKERS * 4)
    adapter = HTTPAdapter(
        max_retries=retry,
        pool_connections=pool,
        pool_maxsize=pool,
        pool_block=False,
    )
    sess.mount("http://", adapter)
    sess.mount("https://", adapter)
    sess.headers.update({"Accept": "application/json"})
    return sess


# ============================================================
# Vista client
# ============================================================

INVALID_GROUP_RE = re.compile(r"Origem\s+(\w+)\s+e\s+campo\s+([\wÀ-ſ]+)", re.IGNORECASE)
INVALID_FIELD_RE = re.compile(r"campo\s+([\wÀ-ſ]+)\s+n[aã]o\s+est[aá]\s+dispon[ií]vel", re.IGNORECASE)


class VistaClient:
    def __init__(self, host: str, key: str, session: requests.Session):
        self.base = f"https://{host.rstrip('/')}"
        self.key = key
        self.s = session

    def _get_raw(self, path: str, params: Dict[str, Any]) -> requests.Response:
        params = {"key": self.key, **params}
        return self.s.get(f"{self.base}{path}", params=params,
                          timeout=REQUEST_TIMEOUT, headers={"Accept": "application/json"})

    def _get_json(self, path: str, params: Dict[str, Any]) -> Dict[str, Any]:
        r = self._get_raw(path, params)
        r.raise_for_status()
        try: return r.json()
        except ValueError: return json.loads(r.text)

    def listarcampos(self) -> Dict[str, Any]:
        """Retorna o schema completo com TODOS os campos disponiveis."""
        return self._get_json("/imoveis/listarcampos", {})

    def listar_codigos(self, page: int, per_page: int, extra_filter: Optional[Dict[str, Any]]) -> Dict[str, Any]:
        pesquisa: Dict[str, Any] = {
            "fields": ["Codigo"],
            "order": {"Codigo": "asc"},
            "paginacao": {"pagina": page, "quantidade": per_page},
        }
        if extra_filter:
            pesquisa["filter"] = extra_filter
        return self._get_json("/imoveis/listar", {
            "showtotal": "1", "showSuspended": "1", "showInternal": "1",
            "pesquisa": json.dumps(pesquisa, ensure_ascii=False),
        })

    def listar_imovel_unico(self, codigo: str, fields: List[str]) -> Dict[str, Any]:
        """Pega 1 imovel via /imoveis/listar (filtrado por Codigo).
        Funciona ate para imoveis Suspenso/Vendido onde /detalhes retorna vazio.
        Retry ativo ate 3x com backoff em caso de 500/timeout.
        Retorna dict do imovel ou {} se Vista nao tem o imovel mesmo.
        """
        for tentativa in range(3):
            try:
                r = self._get_raw("/imoveis/listar", {
                    "showtotal": "1", "showSuspended": "1", "showInternal": "1",
                    "pesquisa": json.dumps({
                        "fields": fields,
                        "filter": {"Codigo": codigo},
                        "paginacao": {"pagina": 1, "quantidade": 1},
                    }, ensure_ascii=False),
                })
            except requests.exceptions.RequestException as e:
                if tentativa < 2:
                    time.sleep(0.5 * (tentativa + 1))
                    continue
                logger.warning("listar_imovel_unico %s: %s", codigo, type(e).__name__)
                return {}
            if r.status_code in (500, 502, 503, 504):
                if tentativa < 2:
                    time.sleep(0.5 * (tentativa + 1))
                    continue
                logger.warning("listar_imovel_unico %s: HTTP %d apos retries", codigo, r.status_code)
                return {}
            if r.status_code != 200:
                return {}
            try: payload = r.json()
            except ValueError: return {}
            for k, v in payload.items():
                if k in ("total","paginas","pagina","quantidade"): continue
                if isinstance(v, dict): return v
            return {}
        return {}

    def detalhes(self, codigo: str, fields: List[Any]) -> Tuple[int, Dict[str, Any]]:
        """Faz 1 call /detalhes com a lista de fields informada.
        Retorna (status, json). Auto-remove campos rejeitados ate ter sucesso ou esgotar.
        Erros 500/RetryError sao retornados silenciosamente como (500, {}) para
        permitir que o pipeline siga com os demais subgrupos.

        showInternal=1 e CRITICO: sem isso, Vista retorna [] para imoveis com
        ExibirNoSite=Nao, mesmo com Status=Venda. Com showInternal=1, traz tudo
        (anexos, fotos, prontuarios, proprietarios etc).
        """
        attempts = list(fields)
        for _ in range(8):
            try:
                r = self._get_raw("/imoveis/detalhes", {
                    "imovel": codigo,
                    "showInternal": "1",
                    "showSuspended": "1",
                    "pesquisa": json.dumps({"fields": attempts}, ensure_ascii=False),
                })
            except requests.exceptions.RequestException as e:
                logger.warning("detalhes(%s) request falhou: %s", codigo, type(e).__name__)
                return 599, {}
            if r.status_code == 200:
                try: return 200, r.json()
                except ValueError: return 200, json.loads(r.text)
            if r.status_code in (500, 502, 503, 504):
                logger.warning("detalhes(%s) HTTP %d - skip subgrupo", codigo, r.status_code)
                return r.status_code, {}
            if r.status_code == 403:
                # Sem permissao no subgrupo - skip silencioso
                return 403, {}
            if r.status_code == 404:
                # Imovel nao encontrado
                return 404, {}
            if r.status_code == 400:
                try: msgs = r.json().get("message", "")
                except Exception: msgs = r.text
                if not isinstance(msgs, list): msgs = [msgs]

                # Remove campos rejeitados
                bads_direct: List[str] = []
                bads_in_group: Dict[str, List[str]] = {}
                for m in msgs:
                    s = str(m)
                    mat_g = INVALID_GROUP_RE.search(s)
                    if mat_g:
                        origem, campo = mat_g.group(1), mat_g.group(2)
                        bads_in_group.setdefault(origem, []).append(campo)
                        continue
                    mat_f = INVALID_FIELD_RE.search(s)
                    if mat_f:
                        bads_direct.append(mat_f.group(1))

                changed = False
                new_attempts: List[Any] = []
                for entry in attempts:
                    if isinstance(entry, str):
                        if entry in bads_direct:
                            changed = True; continue
                        new_attempts.append(entry)
                    elif isinstance(entry, dict):
                        for grupo, campos in entry.items():
                            bads = set(bads_in_group.get(grupo, []))
                            kept = [c for c in campos if c not in bads]
                            if kept:
                                new_attempts.append({grupo: kept})
                            if len(kept) != len(campos):
                                changed = True
                attempts = new_attempts
                if not changed:
                    logger.error("Detalhes %s: 400 sem campo identificavel: %s", codigo, msgs[:2])
                    return r.status_code, {}
                continue
            r.raise_for_status()
        return 400, {}


# ============================================================
# Schema discovery
# ============================================================

# Subgrupos conhecidos (ordem de preferencia para incluir)
KNOWN_SUBGROUPS = (
    "Foto", "FotoEmpreendimento", "Video", "Anexo",
    "Autorizacao", "PontoInteresse",
    "Corretor", "Agencia",
    # estes dois sao pesados - rodam em call separada
    "prontuarios", "proprietarios",
)


def discover_schema(vc: VistaClient) -> Dict[str, Any]:
    """Descobre o schema do Vista e valida cada grupo contra um imovel real."""
    logger.info("Descobrindo schema via /imoveis/listarcampos...")
    schema = vc.listarcampos()
    if not isinstance(schema, dict):
        raise RuntimeError("listarcampos retornou payload inesperado")

    # Pega 1 codigo de imovel para validar contra
    sample = vc.listar_codigos(1, 1, None)
    sample_cod = next(
        (v["Codigo"] for k, v in sample.items()
         if isinstance(v, dict) and v.get("Codigo")),
        None,
    )
    if not sample_cod:
        raise RuntimeError("Nao consegui obter codigo de imovel para validar schema")

    logger.info("Validando campos contra imovel exemplo %s...", sample_cod)

    # Valida campos diretos (chunked porque sao muitos)
    direct = list(schema.get("imoveis", [])) + list(schema.get("codigo", []))
    direct = list(dict.fromkeys(direct))  # de-dup mantendo ordem
    valid_direct: List[str] = []
    for i in range(0, len(direct), FIELDS_CHUNK_SIZE):
        chunk = direct[i:i + FIELDS_CHUNK_SIZE]
        status, j = vc.detalhes(sample_cod, chunk)
        if status == 200:
            # Considera validos os que deram sucesso (apos remocao automatica)
            for c in chunk:
                # se esta na resposta OU se passou validacao (vc.detalhes ja removeu invalidos)
                valid_direct.append(c)
    # Filtra repetidos (vc.detalhes ja removeu invalidos via 400 loop)
    # Re-valida tudo numa unica passada para garantir
    valid_direct_final: List[str] = []
    for c in valid_direct:
        if c not in valid_direct_final:
            valid_direct_final.append(c)

    # Valida subgrupos
    valid_subs: Dict[str, List[str]] = {}
    for grupo in KNOWN_SUBGROUPS:
        campos = schema.get(grupo)
        if not isinstance(campos, list) or not campos:
            continue
        status, j = vc.detalhes(sample_cod, ["Codigo", {grupo: list(campos)}])
        if status == 200:
            # vc.detalhes ja remove campos invalidos. Mas precisamos saber quais sobraram.
            # Re-roda com os mesmos campos mas dessa vez salva o que passou - mais facil:
            # pegamos do .detalhes - se Vista aceitou, todos passaram. Se removeu, tentamos achar.
            # Estrategia: ja validamos via _get_raw/loop. Os campos sobreviventes estao no proximo
            # call. Vamos fazer um teste em isolacao para descobrir:
            sobrev = _isolar_campos_subgrupo(vc, sample_cod, grupo, campos)
            if sobrev:
                valid_subs[grupo] = sobrev
                logger.info("  subgrupo %s: %d campos validos", grupo, len(sobrev))
            else:
                logger.info("  subgrupo %s: sem campos validos", grupo)
        else:
            logger.info("  subgrupo %s: bloqueado", grupo)

    return {
        "imoveis": valid_direct_final,
        "subgrupos": valid_subs,
        "raw_schema_keys": list(schema.keys()),
    }


def _isolar_campos_subgrupo(vc: VistaClient, codigo: str, grupo: str, campos: List[str]) -> List[str]:
    """Testa quais campos do subgrupo sobrevivem."""
    valid = list(campos)
    while valid:
        r = vc._get_raw("/imoveis/detalhes", {
            "imovel": codigo,
            "pesquisa": json.dumps({"fields": ["Codigo", {grupo: valid}]}, ensure_ascii=False),
        })
        if r.status_code == 200:
            return valid
        if r.status_code != 400:
            return []
        try: msgs = r.json().get("message", "")
        except Exception: return []
        if not isinstance(msgs, list): msgs = [msgs]
        bads: List[str] = []
        for m in msgs:
            mat = INVALID_GROUP_RE.search(str(m))
            if mat and mat.group(1) == grupo:
                bads.append(mat.group(2))
        if not bads:
            return []
        valid = [c for c in valid if c not in bads]
    return []


# ============================================================
# Buscar imovel completo (multiplas calls + merge)
# ============================================================

def fetch_imovel_completo(vc: VistaClient, codigo: str, validated: Dict[str, Any]) -> Dict[str, Any]:
    """Retorna o registro completo do imovel mergeando varias calls GET em PARALELO.

    Estrategia (descoberta empiricamente):
      - DIRECT: /imoveis/listar com filter por Codigo aceita os 276 fields em 1 call
                e funciona ate para imoveis Suspenso/Vendido (que /detalhes ignora)
      - SUBGRUPOS: 1 call /imoveis/detalhes por subgrupo, em paralelo
    """
    direct = validated.get("imoveis", [])
    subs = validated.get("subgrupos", {})
    result: Dict[str, Any] = {}

    grupo_flags = {
        "Foto": INCLUDE_FOTO,
        "FotoEmpreendimento": INCLUDE_FOTO_EMPREENDIMENTO,
        "Video": INCLUDE_VIDEO,
        "Anexo": INCLUDE_ANEXO,
        "Autorizacao": INCLUDE_AUTORIZACAO,
        "PontoInteresse": INCLUDE_PONTO_INTERESSE,
        "Corretor": True,
        "Agencia": True,
        "prontuarios": INCLUDE_PRONTUARIOS,
        "proprietarios": INCLUDE_PROPRIETARIOS,
    }

    # 1) Direct fields via /listar (1 call ou poucos chunks se URL ficar gigante)
    def _fetch_direct() -> Dict[str, Any]:
        # Tenta 1 call so
        d = vc.listar_imovel_unico(codigo, direct)
        if d:
            return d
        # Fallback: chunks (caso URL muito longa ou Vista trave)
        merged: Dict[str, Any] = {}
        for i in range(0, len(direct), FIELDS_CHUNK_SIZE):
            chunk = direct[i:i + FIELDS_CHUNK_SIZE]
            if "Codigo" not in chunk: chunk = ["Codigo"] + chunk
            d2 = vc.listar_imovel_unico(codigo, chunk)
            if d2:
                for k, v in d2.items():
                    if k not in merged: merged[k] = v
        return merged

    # 2) Subgrupos via /detalhes
    def _fetch_subgrupo(grupo: str, campos: List[str]) -> Tuple[str, Any]:
        status, j = vc.detalhes(codigo, ["Codigo", {grupo: campos}])
        if status == 200 and isinstance(j, dict) and grupo in j:
            return grupo, j[grupo]
        return grupo, None

    # Tarefas paralelas
    tasks = [("__direct__", _fetch_direct, ())]
    for grupo, flag in grupo_flags.items():
        if not flag: continue
        campos = subs.get(grupo)
        if not campos: continue
        tasks.append((grupo, _fetch_subgrupo, (grupo, campos)))

    with ThreadPoolExecutor(max_workers=SUBGROUP_WORKERS, thread_name_prefix=f"sg-{codigo}") as ex:
        futs = {}
        for tipo, fn, args in tasks:
            futs[ex.submit(fn, *args) if args else ex.submit(fn)] = tipo
        for fut in as_completed(futs):
            tipo = futs[fut]
            try:
                ret = fut.result()
            except Exception as e:
                logger.warning("subtask %s/%s falhou: %s", codigo, tipo, e)
                continue
            if tipo == "__direct__":
                if isinstance(ret, dict):
                    for k, v in ret.items():
                        if k not in result: result[k] = v
            else:
                grupo, valor = ret
                if valor is not None:
                    result[grupo] = valor

    return result


# ============================================================
# S3 client
# ============================================================

def build_s3_client():
    s3_addr = "path" if S3_ENDPOINT_URL else "auto"
    cfg = BotoConfig(
        retries={"max_attempts": 10, "mode": "adaptive"},
        max_pool_connections=MAX_WORKERS * 4,
        s3={"addressing_style": s3_addr},
        signature_version="s3v4",
    )
    kw: Dict[str, Any] = {"config": cfg, "region_name": AWS_REGION}
    if AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY:
        kw["aws_access_key_id"] = AWS_ACCESS_KEY_ID
        kw["aws_secret_access_key"] = AWS_SECRET_ACCESS_KEY
    if S3_ENDPOINT_URL:
        kw["endpoint_url"] = S3_ENDPOINT_URL
    return boto3.client("s3", **kw)


def s3_key(*parts: str) -> str:
    base = [p.strip("/") for p in (S3_PREFIX, *parts) if p]
    return "/".join(base)


def s3_object_exists(s3, bucket: str, key: str) -> bool:
    try:
        s3.head_object(Bucket=bucket, Key=key)
        return True
    except ClientError as e:
        code = e.response.get("Error", {}).get("Code", "")
        if code in ("404", "NoSuchKey", "NotFound"):
            return False
        raise


def _local_path_for(key: str) -> Optional[str]:
    """Resolve o caminho local pra um S3 key (espelhando a estrutura)."""
    if not LOCAL_BACKUP_DIR:
        return None
    # Remove o prefix do S3 para evitar duplicacao (ex: vista-export/imoveis/X -> imoveis/X)
    rel = key
    if S3_PREFIX and key.startswith(S3_PREFIX + "/"):
        rel = key[len(S3_PREFIX) + 1:]
    return os.path.join(LOCAL_BACKUP_DIR, *rel.split("/"))


def _write_local_file(key: str, data: bytes) -> None:
    path = _local_path_for(key)
    if not path: return
    try:
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path, "wb") as f:
            f.write(data)
    except Exception as e:
        logger.warning("Falha ao gravar local %s: %s", path, e)


def s3_put_json(s3, bucket: str, key: str, data: Any) -> None:
    body = json.dumps(data, ensure_ascii=False, indent=2).encode("utf-8")
    s3.put_object(Bucket=bucket, Key=key, Body=body, ContentType="application/json; charset=utf-8")
    _write_local_file(key, body)


def s3_put_bytes(s3, bucket: str, key: str, data: bytes, content_type: str) -> None:
    s3.put_object(Bucket=bucket, Key=key, Body=data, ContentType=content_type)
    _write_local_file(key, data)


# ============================================================
# Helpers de download
# ============================================================

def _safe_filename(url: str, fallback: str) -> str:
    try:
        path = urlparse(url).path
        name = os.path.basename(path) or fallback
    except Exception:
        name = fallback
    name = "".join(c for c in name if c.isalnum() or c in ("-", "_", ".")).strip(".")
    return name or fallback


def _guess_ctype(name: str, default: str = "application/octet-stream") -> str:
    ctype, _ = mimetypes.guess_type(name)
    return ctype or default


def _baixar_e_subir(url: str, key: str, http: requests.Session, s3, bucket: str) -> bool:
    """Baixa URL e sobe pro S3. Retorna True se subiu, False se falhou."""
    if not FORCE_PHOTOS and s3_object_exists(s3, bucket, key):
        return True
    try:
        r = http.get(url, timeout=REQUEST_TIMEOUT)
        r.raise_for_status()
    except Exception as e:
        logger.error("Falha download %s: %s", url, e)
        return False
    ctype = r.headers.get("Content-Type") or _guess_ctype(key)
    try:
        s3_put_bytes(s3, bucket, key, r.content, ctype)
        return True
    except Exception as e:
        logger.error("Falha upload S3 %s: %s", key, e)
        return False


def _normalizar_lista(payload: Any) -> List[Dict[str, Any]]:
    """Vista retorna fotos/anexos/etc como dict numerado OU lista vazia."""
    if isinstance(payload, dict):
        items: List[Tuple[int, Dict[str, Any]]] = []
        for k, v in payload.items():
            if not isinstance(v, dict): continue
            try: ord_key = int(v.get("Ordem") or k)
            except (TypeError, ValueError): ord_key = 10**9
            items.append((ord_key, v))
        items.sort(key=lambda t: t[0])
        return [v for _, v in items]
    if isinstance(payload, list):
        return [v for v in payload if isinstance(v, dict)]
    return []


# ============================================================
# Pipeline por imovel
# ============================================================

def coletar_codigos(vc: VistaClient, extra_filter: Optional[Dict[str, Any]]) -> List[str]:
    codigos: List[str] = []
    pagina = 1
    while True:
        payload = vc.listar_codigos(pagina, PAGE_SIZE, extra_filter)
        if not payload:
            break
        for k, v in payload.items():
            if k in ("total", "paginas", "pagina", "quantidade"):
                continue
            if isinstance(v, dict) and v.get("Codigo"):
                codigos.append(str(v["Codigo"]).strip())
        total_paginas = int(payload.get("paginas") or 1)
        total_itens = int(payload.get("total") or 0)
        logger.info("Listagem pag %d/%d (acumulado: %d/%d)",
                    pagina, total_paginas, len(codigos), total_itens)
        if pagina >= total_paginas: break
        pagina += 1

    seen, uniq = set(), []
    for c in codigos:
        if c not in seen:
            seen.add(c); uniq.append(c)
    return uniq


def listar_imoveis_existentes_no_s3(s3, bucket: str, min_data_size: int = 0) -> set:
    """Lista os codigos de imovel que ja tem data.json no bucket.
    Se min_data_size > 0, considera apenas data.json maior que esse tamanho
    (util para excluir placeholders 'sem_dados_vista' que tem ~50 bytes).
    """
    existentes = set()
    paginator = s3.get_paginator("list_objects_v2")
    prefix = s3_key("imoveis") + "/"
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []) or []:
            k = obj.get("Key", "")
            if k.endswith("/data.json"):
                if obj.get("Size", 0) < min_data_size:
                    continue
                parts = k.split("/")
                if len(parts) >= 4:
                    existentes.add(parts[-2])
    return existentes


def processar_imovel(codigo: str, vc: VistaClient, validated: Dict[str, Any],
                     s3, bucket: str, http: requests.Session,
                     codigos_existentes: Optional[set] = None) -> Dict[str, Any]:
    stats = {"codigo": codigo, "fotos": 0, "anexos": 0, "erros": 0, "skipped": False}

    data_key = s3_key("imoveis", codigo, "data.json")
    if SKIP_EXISTING:
        if codigos_existentes is not None:
            if codigo in codigos_existentes:
                stats["skipped"] = True
                return stats
        elif s3_object_exists(s3, bucket, data_key):
            stats["skipped"] = True
            return stats

    try:
        det = fetch_imovel_completo(vc, codigo, validated)
    except Exception as e:
        logger.exception("Falha ao buscar imovel %s: %s", codigo, e)
        det = {}

    # SEMPRE salva o imovel - mesmo que parcial - pra que SKIP_EXISTING
    # nao reprocesse na proxima rodada. Se vista nao retornou nada, gravamos
    # placeholder com o Codigo + flag pra reprocessar manualmente depois.
    if not isinstance(det, dict): det = {}
    det.setdefault("Codigo", codigo)
    if len(det) <= 1:
        det["_status"] = "sem_dados_vista"
        stats["erros"] += 1

    base_data = {k: v for k, v in det.items() if k not in ("prontuarios", "proprietarios")}
    s3_put_json(s3, bucket, data_key, base_data)

    if "prontuarios" in det:
        s3_put_json(s3, bucket, s3_key("imoveis", codigo, "prontuarios.json"), det["prontuarios"])
    if "proprietarios" in det:
        s3_put_json(s3, bucket, s3_key("imoveis", codigo, "proprietarios.json"), det["proprietarios"])

    # Download de fotos da galeria
    if DOWNLOAD_FOTOS:
        fotos = _normalizar_lista(det.get("Foto"))
        with ThreadPoolExecutor(max_workers=PHOTO_WORKERS, thread_name_prefix=f"foto-{codigo}") as ex:
            futs = []
            for idx, foto in enumerate(fotos, start=1):
                url = foto.get("FotoOriginal") or foto.get("Foto") or foto.get("FotoPequena")
                if not (isinstance(url, str) and url.startswith("http")): continue
                nome = _safe_filename(url, f"{idx}.jpg")
                key = s3_key("imoveis", codigo, "fotos", f"{idx:04d}-{nome}")
                futs.append(ex.submit(_baixar_e_subir, url, key, http, s3, bucket))
            for fut in as_completed(futs):
                if fut.result(): stats["fotos"] += 1
                else: stats["erros"] += 1

        # FotoEmpreendimento
        emp = _normalizar_lista(det.get("FotoEmpreendimento"))
        for idx, foto in enumerate(emp, start=1):
            url = foto.get("Foto") or foto.get("FotoPequena")
            if not (isinstance(url, str) and url.startswith("http")): continue
            nome = _safe_filename(url, f"emp_{idx}.jpg")
            key = s3_key("imoveis", codigo, "foto_empreendimento", f"{idx:04d}-{nome}")
            if _baixar_e_subir(url, key, http, s3, bucket): stats["fotos"] += 1

    # Download de anexos
    if DOWNLOAD_ANEXOS:
        anexos = _normalizar_lista(det.get("Anexo"))
        for idx, an in enumerate(anexos, start=1):
            url = an.get("Anexo") or an.get("Arquivo")
            if not (isinstance(url, str) and url.startswith("http")): continue
            nome = _safe_filename(url, f"anexo_{idx}")
            key = s3_key("imoveis", codigo, "anexos", f"{idx:04d}-{nome}")
            if _baixar_e_subir(url, key, http, s3, bucket): stats["anexos"] += 1

    return stats


# ============================================================
# Main
# ============================================================

def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser()
    p.add_argument("--codes-file", help="Arquivo texto com 1 codigo por linha")
    p.add_argument("--worker-id", default="")
    p.add_argument("--no-manifest", action="store_true")
    return p.parse_args()


def _load_codes_from_file(path: str) -> List[str]:
    with open(path, "r", encoding="utf-8") as f:
        return [ln.strip() for ln in f if ln.strip()]


def _load_or_discover_validated(vc: VistaClient, s3, bucket: str) -> Dict[str, Any]:
    """Carrega schema validado do S3 (cache) ou descobre via API."""
    cache_key = s3_key("_schema", "vista_validated.json")
    if not _WORKER_TAG:  # so o orquestrador descobre
        try:
            obj = s3.get_object(Bucket=bucket, Key=cache_key)
            cached = json.loads(obj["Body"].read())
            logger.info("Schema validado carregado do S3: %d campos diretos, %d subgrupos",
                        len(cached.get("imoveis", [])), len(cached.get("subgrupos", {})))
            return cached
        except ClientError:
            pass
    else:
        # Workers: tentam usar cache do S3
        try:
            obj = s3.get_object(Bucket=bucket, Key=cache_key)
            return json.loads(obj["Body"].read())
        except ClientError:
            pass

    validated = discover_schema(vc)
    s3_put_json(s3, bucket, cache_key, validated)
    return validated


def main() -> None:
    args = _parse_args()
    _validate_config()

    inicio = time.time()
    tag = f" worker={args.worker_id}" if args.worker_id else ""
    print("=" * 60)
    print(f"Vista -> S3 (TUDO){tag}")
    print(f"  Vista : {VISTA_API_HOST}")
    print(f"  Bucket: s3://{S3_BUCKET}/{S3_PREFIX}")
    if LOCAL_BACKUP_DIR:
        print(f"  Local : {LOCAL_BACKUP_DIR}")
    print(f"  Workers imovel/subgrupo/foto: {MAX_WORKERS}/{SUBGROUP_WORKERS}/{PHOTO_WORKERS}")
    print(f"  Inclui: prontuarios={INCLUDE_PRONTUARIOS} proprietarios={INCLUDE_PROPRIETARIOS}")
    print(f"  Download: fotos={DOWNLOAD_FOTOS} anexos={DOWNLOAD_ANEXOS}")
    print("=" * 60)

    http = build_http_session()
    vc = VistaClient(VISTA_API_HOST, VISTA_API_KEY, http)
    s3 = build_s3_client()

    try: s3.head_bucket(Bucket=S3_BUCKET)
    except ClientError as e: raise SystemExit(f"Bucket inacessivel: {e}")

    # Schema validado (com cache)
    validated = _load_or_discover_validated(vc, s3, S3_BUCKET)

    if args.codes_file:
        codigos = _load_codes_from_file(args.codes_file)
        print(f"Lidos {len(codigos)} codigos de {args.codes_file}")
    else:
        extra_filter = None
        if VISTA_FILTER_JSON:
            try: extra_filter = json.loads(VISTA_FILTER_JSON)
            except json.JSONDecodeError as e: raise SystemExit(f"VISTA_FILTER_JSON invalido: {e}")
        print("Coletando codigos...")
        codigos = coletar_codigos(vc, extra_filter)
        print(f"Total: {len(codigos)}")

    if not codigos:
        return

    totais = {"ok": 0, "skipped": 0, "erros": 0, "fotos": 0, "anexos": 0}

    # Pre-check em batch - lista o que ja foi processado pra evitar 1 head_object por imovel
    codigos_existentes: Optional[set] = None
    if SKIP_EXISTING:
        print(f"Listando imoveis ja completos (data.json > {MIN_DATA_SIZE} bytes)...")
        try:
            codigos_existentes = listar_imoveis_existentes_no_s3(s3, S3_BUCKET, MIN_DATA_SIZE)
            ja_feitos = sum(1 for c in codigos if c in codigos_existentes)
            placeholders = sum(1 for c in codigos if c not in codigos_existentes)
            print(f"  {len(codigos_existentes)} ja completos | {placeholders} pendentes/placeholder a reprocessar")
        except Exception as e:
            logger.warning("Falha no pre-check: %s. Caira para head_object por imovel.", e)
            codigos_existentes = None

    with ThreadPoolExecutor(max_workers=MAX_WORKERS, thread_name_prefix="imv") as ex:
        futs = {ex.submit(processar_imovel, c, vc, validated, s3, S3_BUCKET, http, codigos_existentes): c for c in codigos}
        feitos = 0
        for fut in as_completed(futs):
            cod = futs[fut]; feitos += 1
            try: stats = fut.result()
            except Exception as e:
                logger.exception("Erro fatal %s: %s", cod, e)
                totais["erros"] += 1; continue
            if stats["skipped"]: totais["skipped"] += 1
            else: totais["ok"] += 1
            totais["fotos"] += stats["fotos"]
            totais["anexos"] += stats["anexos"]
            totais["erros"] += stats["erros"]

            if feitos % 25 == 0 or feitos == len(codigos):
                logger.info("%d/%d | ok=%d skip=%d fotos=%d anexos=%d erros=%d",
                            feitos, len(codigos),
                            totais["ok"], totais["skipped"],
                            totais["fotos"], totais["anexos"], totais["erros"])

    if not args.no_manifest:
        suffix = f"-{args.worker_id}" if args.worker_id else ""
        manifest_key = s3_key("_manifest", f"imoveis-{datetime.now():%Y%m%d-%H%M%S}{suffix}.json")
        try:
            s3_put_json(s3, S3_BUCKET, manifest_key, {
                "geradoEm": datetime.now().astimezone().isoformat(),
                "host": VISTA_API_HOST,
                "workerId": args.worker_id,
                "totalCodigos": len(codigos),
                "estatisticas": totais,
                "codigos": codigos,
            })
        except Exception as e:
            logger.error("Falha manifest: %s", e)

    elapsed = time.time() - inicio
    print("=" * 60)
    print(f"Tempo: {elapsed:.1f}s ({elapsed/60:.1f} min)")
    print(f"  ok={totais['ok']}  skip={totais['skipped']}")
    print(f"  fotos={totais['fotos']}  anexos={totais['anexos']}")
    print(f"  erros={totais['erros']}")
    print("=" * 60)


if __name__ == "__main__":
    try: main()
    except KeyboardInterrupt:
        print("\nInterrompido.")
        sys.exit(130)
