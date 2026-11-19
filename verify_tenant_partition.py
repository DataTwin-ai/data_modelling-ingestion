"""
Check (and optionally migrate) tenant partitioning of the shared schema.

All tenants share one schema (config.ini: db_name / table_schema). Every
tenant-scoped table (active_*, inactive_*, theme_mapping, transaction_log)
must have a tenant_id column that leads its primary key.

Usage:
    python verify_tenant_partition.py
        Read-only report: tenant_id column, primary key, rows per tenant and
        rows with an empty/NULL or non-normalized tenant_id.

    python verify_tenant_partition.py --apply
        Add tenant_id column / make it the leading PK column where missing.

    python verify_tenant_partition.py --apply --backfill DM00005
        Additionally assign rows with an empty/NULL tenant_id to DM00005 and
        upper-case/trim existing tenant ids (one transaction per table).
"""
import argparse
import sys

from psycopg2 import sql

import app


def scoped_tables(conn):
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT table_name FROM information_schema.tables
            WHERE table_schema = current_schema()
              AND (table_name LIKE 'active\\_%%' OR table_name LIKE 'inactive\\_%%' OR table_name = ANY(%s))
            ORDER BY table_name
            """,
            (sorted(app.TENANT_SCOPED_TABLES),),
        )
        return [r[0] for r in cur.fetchall()]


def table_report(conn, tbl):
    ident = sql.Identifier(tbl)
    with conn.cursor() as cur:
        cur.execute(
            "SELECT 1 FROM information_schema.columns WHERE table_schema = current_schema() AND table_name = %s AND column_name = 'tenant_id'",
            (tbl,),
        )
        has_col = cur.fetchone() is not None
        cur.execute(
            """
            SELECT array_agg(a.attname::text ORDER BY k.ord)
            FROM pg_constraint c
            CROSS JOIN LATERAL unnest(c.conkey) WITH ORDINALITY AS k(attnum, ord)
            JOIN pg_attribute a ON a.attrelid = c.conrelid AND a.attnum = k.attnum
            WHERE c.conrelid = to_regclass(%s) AND c.contype = 'p'
            """,
            (tbl,),
        )
        row = cur.fetchone()
        pk_cols = list(row[0]) if row and row[0] else []
        per_tenant, empty, unnormalized = [], 0, 0
        if has_col:
            cur.execute(sql.SQL("SELECT tenant_id, count(*) FROM {} GROUP BY tenant_id ORDER BY 2 DESC").format(ident))
            per_tenant = cur.fetchall()
            for t, n in per_tenant:
                if t is None or str(t).strip() == "":
                    empty += n
                elif str(t) != app.normalize_tenant_id(t):
                    unnormalized += n
    conn.rollback()
    return has_col, pk_cols, per_tenant, empty, unnormalized


def backfill(conn, tbl, tenant):
    ident = sql.Identifier(tbl)
    try:
        with conn.cursor() as cur:
            cur.execute(
                sql.SQL("UPDATE {} SET tenant_id = %s WHERE tenant_id IS NULL OR TRIM(tenant_id) = ''").format(ident),
                (tenant,),
            )
            filled = cur.rowcount
            cur.execute(
                sql.SQL("UPDATE {} SET tenant_id = UPPER(TRIM(tenant_id)) WHERE tenant_id <> UPPER(TRIM(tenant_id))").format(ident)
            )
            normalized = cur.rowcount
        conn.commit()
        return f"filled {filled}, normalized {normalized}"
    except Exception as e:
        conn.rollback()
        return f"FAILED (rolled back): {str(e).splitlines()[0]}"


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--apply", action="store_true", help="add tenant_id column / leading PK where missing")
    ap.add_argument("--backfill", metavar="TENANT_ID", help="assign rows with empty tenant_id to this tenant (needs --apply)")
    args = ap.parse_args()

    if args.backfill and not args.apply:
        ap.error("--backfill requires --apply")
    backfill_tenant = app.sql_tenant_literal(args.backfill) if args.backfill else None

    if not app.SHARED_SCHEMA:
        print("db_name is empty (one database per tenant): tenant partitioning is not used, nothing to check.")
        return 0
    print(f"Database: {app.db_name} | schema: {app.table_schema} | host: {app.ec2IP}:{app.port}")
    conn = app.ec2confic(app.COMMON_DB_KEY)
    try:
        tables = scoped_tables(conn)
        conn.rollback()
        if not tables:
            print("No tenant-scoped tables found in this schema.")
            return 0

        if args.apply:
            print("\nApplying tenant partitioning...")
            for tbl in tables:
                app.ensure_tenant_partition(conn, tbl)
            if backfill_tenant:
                print(f"\nBackfilling empty tenant_id -> {backfill_tenant}")
                for tbl in tables:
                    print(f"  {tbl:<40} {backfill(conn, tbl, backfill_tenant)}")

        problems = 0
        print("\nTable report:")
        for tbl in tables:
            has_col, pk_cols, per_tenant, empty, unnormalized = table_report(conn, tbl)
            ok_pk = (not pk_cols) or (pk_cols[0] == app.TENANT_COLUMN)
            ok = has_col and ok_pk and empty == 0 and unnormalized == 0
            problems += 0 if ok else 1
            status = "OK " if ok else "FIX"
            tenants = ", ".join(f"{t or '<empty>'}={n}" for t, n in per_tenant[:6]) or "no rows"
            print(f"  [{status}] {tbl}")
            print(f"        tenant_id column: {'yes' if has_col else 'NO'} | PK: ({', '.join(pk_cols) or 'none'})")
            print(f"        rows: {tenants}")
            if empty:
                print(f"        {empty} row(s) have an empty/NULL tenant_id")
            if unnormalized:
                print(f"        {unnormalized} row(s) have a non upper-case/trimmed tenant_id")
        print(f"\n{len(tables)} table(s) checked, {problems} need attention.")
        if problems and not args.apply:
            print("Run with --apply (and --backfill <TENANT_ID> for empty rows) to fix.")
        return 1 if problems else 0
    finally:
        conn.close()


if __name__ == "__main__":
    sys.exit(main())
