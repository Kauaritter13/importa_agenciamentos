"""
Zera TODAS as tabelas relacionadas a imoveis no Postgres
(preservando users, categorias, statuses, owners).

Uso:
  python zerar_imoveis.py --confirm

Sem --confirm so mostra o que faria.
"""

from __future__ import annotations

import argparse
import os
import sys

try:
    from dotenv import load_dotenv
    load_dotenv(os.path.join(os.path.dirname(os.path.abspath(__file__)), ".env"))
except ImportError:
    pass

import psycopg

DATABASE_URL = os.getenv("DATABASE_PUBLIC_URL") or os.getenv("DATABASE_URL")


# Ordem importa: subordinadas primeiro, properties por ultimo.
# Apos refactor do backend: property_videos e property_documents foram removidas.
TABELAS_LIMPAR = [
    "property_field_values",
    "property_photos",
    "property_history",
    "property_comments",
    "property_owner_properties",
    "property_agents",
    "properties",
]


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--confirm", action="store_true",
                    help="Executa de fato (sem isso, so faz dry-run)")
    args = ap.parse_args()

    if not DATABASE_URL:
        print("ERRO: DATABASE_URL nao configurada no .env")
        return 1

    print("=" * 60)
    print(f"{'EXECUTANDO' if args.confirm else 'DRY-RUN'} - Zerar imoveis no Postgres")
    print("=" * 60)

    with psycopg.connect(DATABASE_URL, connect_timeout=15) as conn:
        with conn.cursor() as cur:
            # Mostra contagem atual
            print("\nContagem antes:")
            for t in TABELAS_LIMPAR:
                cur.execute(f"SELECT COUNT(*) FROM {t}")
                total = cur.fetchone()[0]
                print(f"  {t:35s}: {total:>8,}")

            if not args.confirm:
                print("\n[dry-run] Use --confirm para executar de fato.")
                return 0

            print("\nDeletando...")
            for t in TABELAS_LIMPAR:
                cur.execute(f"DELETE FROM {t}")
                print(f"  {t:35s}: {cur.rowcount:>8,} linhas removidas")
            conn.commit()

            print("\nContagem depois:")
            for t in TABELAS_LIMPAR:
                cur.execute(f"SELECT COUNT(*) FROM {t}")
                total = cur.fetchone()[0]
                print(f"  {t:35s}: {total:>8,}")

    print("\nOK: imoveis zerados.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
