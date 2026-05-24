"""
Entrypoint do worker Railway.

1. Detecta se ha Volume montado em /data (recomendado para persistencia).
   Se nao tiver, cai no diretorio atual (perde estado em redeploy).
2. Faz SEED do checkpoint commitado no repo para /data se /data estiver vazio.
3. Executa inserir_lote_vista.py em modo --retomar usando o checkpoint do volume.

Env vars opcionais:
  WORKERS=16             # paralelismo do lote
  STATUS=                # se setado, filtra por status (ex: "Venda")
  APENAS_ATIVOS=         # se "1", filtra Suspenso/Inativo
"""
from __future__ import annotations

import os
import shutil
import sys


def _resolve_data_dir() -> str:
    # Railway Volume mount tipico: /data
    if os.path.isdir("/data") and os.access("/data", os.W_OK):
        return "/data"
    # Fallback: diretorio atual (efemero em redeploy)
    cwd_data = os.path.abspath(".")
    print(f"[entrypoint] AVISO: /data nao montado. Usando {cwd_data} (efemero!)", flush=True)
    return cwd_data


def main() -> int:
    data_dir = _resolve_data_dir()
    os.makedirs(data_dir, exist_ok=True)

    checkpoint_filename = "_checkpoint_full.json"
    repo_checkpoint = os.path.abspath(checkpoint_filename)
    dest_checkpoint = os.path.join(data_dir, checkpoint_filename)

    if os.path.exists(dest_checkpoint):
        size = os.path.getsize(dest_checkpoint)
        print(f"[entrypoint] usando checkpoint existente: {dest_checkpoint} ({size:,} bytes)", flush=True)
    elif os.path.exists(repo_checkpoint) and os.path.abspath(repo_checkpoint) != os.path.abspath(dest_checkpoint):
        print(f"[entrypoint] semeando checkpoint a partir do repo: {repo_checkpoint} -> {dest_checkpoint}", flush=True)
        shutil.copyfile(repo_checkpoint, dest_checkpoint)
    else:
        print(f"[entrypoint] iniciando do zero - nenhum checkpoint encontrado", flush=True)

    workers = os.getenv("WORKERS", "16")
    args = [sys.executable, "-u", "inserir_lote_vista.py", "--todos",
            "--workers", workers,
            "--checkpoint", dest_checkpoint, "--retomar"]

    status = os.getenv("STATUS")
    if status:
        args += ["--status", status]
    if os.getenv("APENAS_ATIVOS") == "1":
        args += ["--apenas-ativos"]

    print(f"[entrypoint] exec: {' '.join(args)}", flush=True)
    os.execv(sys.executable, args)


if __name__ == "__main__":
    sys.exit(main())
