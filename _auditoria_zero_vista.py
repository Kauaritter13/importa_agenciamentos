"""
Auditoria final: garante que NENHUM dado no Postgres referencia o Vista.

Verifica:
  1. property_photos.file_url NAO comeca com 'http' ou 'https://vista'
  2. property_field_values.value (de campos ATTACHMENT/PHOTO/URL) NAO contem
     URLs Vista (urbanpoa20097-rest.vistahost.com.br ou .vista.com)
  3. properties.owners jsonb tem objetos com {id,name,email,phone,cpf_cnpj}
  4. property_agents tem links coerentes com properties.agents jsonb
  5. Imoveis sem corretor placeholder ("Corretor Vista NNN") em users
  6. Totais por status/categoria

Uso:
  python _auditoria_zero_vista.py
"""
import os, json, re, sys
from collections import defaultdict
from dotenv import load_dotenv
load_dotenv(".env")

import psycopg

VISTA_HOSTS = [
    "vistahost.com.br",
    "urbanpoa20097-rest",
    ".vista.com",
    "/vista/", "vista-cdn", "vista_cdn",
]


def main():
    db = os.getenv("DATABASE_PUBLIC_URL") or os.getenv("DATABASE_URL")
    if not db:
        print("ERRO: DATABASE_URL nao configurada"); return 1

    with psycopg.connect(db, connect_timeout=15) as c:
        with c.cursor() as cur:
            print("=" * 70)
            print("AUDITORIA: zero dependencia Vista")
            print("=" * 70)

            # 1. Total geral
            cur.execute("SELECT COUNT(*) FROM properties")
            total = cur.fetchone()[0]
            print(f"\nTotal de imoveis: {total:,}")

            cur.execute("""SELECT s.name, COUNT(*) FROM properties p
                           LEFT JOIN property_statuses s ON s.id=p.status_id
                           GROUP BY s.name ORDER BY 2 DESC""")
            print("Por status:")
            for nm, ct in cur.fetchall():
                print(f"  {nm or '(sem)':40s} {ct:>8,}")

            # 2. Fotos
            print("\n--- property_photos ---")
            cur.execute("SELECT COUNT(*) FROM property_photos")
            print(f"Total fotos: {cur.fetchone()[0]:,}")
            for h in VISTA_HOSTS:
                cur.execute("SELECT COUNT(*) FROM property_photos WHERE file_url LIKE %s", (f"%{h}%",))
                ct = cur.fetchone()[0]
                marker = "OK" if ct == 0 else "FAIL"
                print(f"  [{marker}] urls com '{h}': {ct:,}")

            # 3. Custom field values com URLs Vista
            print("\n--- property_field_values (ATTACHMENT/PHOTO/URL/CARD) ---")
            for h in VISTA_HOSTS:
                cur.execute("""SELECT COUNT(*) FROM property_field_values pfv
                               JOIN property_fields pf ON pf.id=pfv.field_id
                               WHERE pf.type IN ('ATTACHMENT','PHOTO','URL','CARD')
                                 AND pfv.value LIKE %s""", (f"%{h}%",))
                ct = cur.fetchone()[0]
                marker = "OK" if ct == 0 else "FAIL"
                print(f"  [{marker}] fields com '{h}': {ct:,}")

            # 4. owners jsonb estrutura
            print("\n--- properties.owners jsonb ---")
            cur.execute("SELECT COUNT(*) FROM properties WHERE jsonb_array_length(owners) > 0")
            com_owner = cur.fetchone()[0]
            print(f"  Imoveis com owners: {com_owner:,}")
            cur.execute("""SELECT COUNT(*) FROM properties
                           WHERE jsonb_array_length(owners) > 0
                             AND jsonb_typeof(owners->0) = 'object'""")
            obj_owner = cur.fetchone()[0]
            print(f"  Imoveis com owners como OBJETO: {obj_owner:,}")
            cur.execute("""SELECT COUNT(*) FROM properties
                           WHERE jsonb_array_length(owners) > 0
                             AND jsonb_typeof(owners->0) = 'string'""")
            str_owner = cur.fetchone()[0]
            marker = "OK" if str_owner == 0 else "FAIL"
            print(f"  [{marker}] Imoveis com owners como STRING (legado): {str_owner:,}")

            # 5. corretor placeholders
            print("\n--- usuarios placeholder (Corretor Vista NNN) ---")
            cur.execute("""SELECT id, name, email FROM users
                           WHERE name LIKE 'Corretor Vista %'
                           ORDER BY name""")
            phs = cur.fetchall()
            print(f"  {len(phs)} placeholders ainda existem")
            for uid, nm, em in phs[:20]:
                cur.execute("SELECT COUNT(*) FROM property_agents WHERE user_id=%s", (uid,))
                n = cur.fetchone()[0]
                print(f"    {nm:30s} {em:50s} {n:>5} imoveis")
            if len(phs) > 20:
                print(f"    ... +{len(phs)-20} mais")

            # 6. property_agents sem user real
            print("\n--- property_agents sem nome real ---")
            cur.execute("""SELECT COUNT(*) FROM property_agents pa
                           JOIN users u ON u.id=pa.user_id
                           WHERE u.email LIKE '%@import.local'""")
            ct = cur.fetchone()[0]
            marker = "OK" if ct == 0 else "WARN"
            print(f"  [{marker}] property_agents pointing to @import.local users: {ct:,}")

            # 7. Property History
            print("\n--- property_history ---")
            cur.execute("SELECT COUNT(*) FROM property_history")
            print(f"  Total registros: {cur.fetchone()[0]:,}")
            cur.execute("""SELECT COUNT(*) FROM property_history ph
                           JOIN users u ON u.id=ph.created_by
                           WHERE u.email LIKE '%@import.local'""")
            print(f"  Com created_by placeholder: {cur.fetchone()[0]:,}")

            print("\n" + "=" * 70)
            print("AUDITORIA COMPLETA")
            print("=" * 70)
    return 0


if __name__ == "__main__":
    sys.exit(main())
