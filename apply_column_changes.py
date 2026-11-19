"""
Apply a column-change spec workbook (e.g. new_columns.xlsx) to the shared schema.

Each sheet is named after an active table (active_fs, active_taxsys, ...). Supported layouts:
  - "Category | Old name | New field | Modified field name | Updated name"
      Category 'Modified Field Name' -> rename Old name to Modified field name
      Category 'New Field'           -> add column (New field / Updated name)
  - "field name | New field"
      'New Field' in the second column -> add column

Changes are applied to active_<x> and inactive_<x>. Renames also update
theme_mapping.targetfieldname and the process catalog short_text/domain, so
reverse-mapped exports keep working.

Usage:
    python apply_column_changes.py new_columns.xlsx            # dry run: show the plan
    python apply_column_changes.py new_columns.xlsx --apply    # execute
    ... --add-missing   also add columns listed in the sheet that the table lacks
"""
import argparse
import sys

import openpyxl
from psycopg2 import sql

import app


def _norm(v):
    return str(v).strip() if v is not None else ""


def read_spec(path):
    """Return {table: {"renames": [(old, new)], "new": [col], "listed": [col]}}."""
    wb = openpyxl.load_workbook(path, data_only=True, read_only=True)
    spec = {}
    for ws in wb.worksheets:
        table = ws.title.strip().lower()
        rows = list(ws.iter_rows(values_only=True))
        if not rows:
            continue
        header = [_norm(h).lower() for h in rows[0]]
        idx = {h: i for i, h in enumerate(header) if h}

        def cell(row, name):
            i = idx.get(name)
            return _norm(row[i]) if i is not None and i < len(row) else ""

        entry = {"renames": [], "new": [], "listed": []}
        for row in rows[1:]:
            if not row or all(v is None for v in row):
                continue
            category = cell(row, "category").lower()
            old = cell(row, "old name").lower()
            modified = cell(row, "modified field name").lower()
            updated = cell(row, "updated name").lower()
            field = cell(row, "field name").lower()
            new_field = cell(row, "new field")

            final_name = updated or modified or field or old or new_field.lower()
            if final_name:
                entry["listed"].append(final_name)
            if category == "modified field name" or (old and modified and old != modified):
                if old and (modified or updated) and old != (modified or updated):
                    entry["renames"].append((old, modified or updated))
            elif category == "new field" or new_field.lower() == "new field":
                name = (new_field if new_field.lower() != "new field" else "") or updated or field
                if name:
                    entry["new"].append(name.lower())
        spec[table] = entry
    return spec


def table_columns(cur, table):
    cur.execute(
        "SELECT column_name FROM information_schema.columns WHERE table_schema = current_schema() AND table_name = %s",
        (table,),
    )
    return {r[0] for r in cur.fetchall()}


def plan_table(cur, table, entry, add_missing):
    base = table[len("active_"):] if table.startswith("active_") else table
    targets = [t for t in (f"active_{base}", f"inactive_{base}") if table_columns(cur, t)]
    steps, notes = [], []
    if not targets:
        notes.append(f"table '{table}' not found in schema - skipped")
        return steps, notes, base
    for tbl in targets:
        cols = table_columns(cur, tbl)
        for old, new in entry["renames"]:
            if old in cols and new not in cols:
                steps.append(("rename", tbl, old, new))
                cols = (cols - {old}) | {new}
            elif old in cols and new in cols:
                notes.append(f"{tbl}: both '{old}' and '{new}' exist - rename skipped, merge manually")
            elif new in cols:
                notes.append(f"{tbl}: '{new}' already present")
            else:
                notes.append(f"{tbl}: '{old}' not found - nothing to rename")
        wanted = list(entry["new"])
        if add_missing:
            wanted += [c for c in entry["listed"] if c not in wanted]
        for col in wanted:
            if col not in cols:
                steps.append(("add", tbl, col, None))
                cols.add(col)
            elif col in entry["new"]:
                notes.append(f"{tbl}: new column '{col}' already exists")
        if not add_missing:
            missing = [c for c in entry["listed"] if c not in cols and c not in entry["new"]]
            if missing:
                notes.append(f"{tbl}: listed but missing (use --add-missing): {', '.join(missing)}")
    return steps, notes, base


def apply_steps(conn, common_conn, steps, renames, process_code):
    tenant_match = "x.tenant_id IS NOT DISTINCT FROM t.tenant_id AND " if app.SHARED_SCHEMA else ""
    with conn.cursor() as cur:
        for kind, tbl, a, b in steps:
            if kind == "rename":
                cur.execute(sql.SQL("ALTER TABLE {} RENAME COLUMN {} TO {}").format(
                    sql.Identifier(tbl), sql.Identifier(a), sql.Identifier(b)))
            else:
                cur.execute(sql.SQL("ALTER TABLE {} ADD COLUMN IF NOT EXISTS {} text DEFAULT ''").format(
                    sql.Identifier(tbl), sql.Identifier(a)))
        meta = []
        for old, new in renames:
            cur.execute(
                """
                UPDATE theme_mapping t SET targetfieldname = %s
                WHERE UPPER(t.processcode) = %s AND LOWER(t.targetfieldname) = %s
                  AND NOT EXISTS (
                    SELECT 1 FROM theme_mapping x
                    WHERE """ + tenant_match + """x.productcode = t.productcode
                      AND x.processcode = t.processcode AND x.themename = t.themename
                      AND LOWER(x.targetfieldname) = %s)
                """,
                (new, process_code, old, new),
            )
            meta.append(f"theme_mapping {old}->{new}: {cur.rowcount} row(s)")
    with common_conn.cursor() as ccur:
        for old, new in renames:
            ccur.execute(
                "UPDATE process SET short_text = %s, domain = %s WHERE UPPER(process_cd) = %s AND LOWER(short_text) = %s",
                (new, new, process_code, old),
            )
            meta.append(f"process {old}->{new}: {ccur.rowcount} row(s)")
    return meta


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("spec", help="spec workbook, e.g. new_columns.xlsx")
    ap.add_argument("--apply", action="store_true", help="execute the plan (default: dry run)")
    ap.add_argument("--add-missing", action="store_true", help="also add listed columns the table lacks")
    ap.add_argument("--only", help="comma-separated sheet/table names to process (e.g. active_taxsys)")
    ap.add_argument("--tenant", help="tenant database to change (required in per-tenant mode, e.g. DM00005)")
    args = ap.parse_args()
    if not app.SHARED_SCHEMA and not args.tenant:
        ap.error("per-tenant database mode (db_name empty): pass --tenant <TENANT_ID>")

    spec = read_spec(args.spec)
    if args.only:
        keep = {t.strip().lower() for t in args.only.split(",")}
        spec = {k: v for k, v in spec.items() if k in keep}
    if not spec:
        print("Nothing to do: no matching sheets.")
        return 1

    target = args.tenant if args.tenant else app.COMMON_DB_KEY
    conn = app.ec2confic(target)
    common_conn = app.ec2confic(app.COMMON_DB_KEY)
    with conn.cursor() as c:
        c.execute("SELECT current_database(), current_schema()")
        db, schema = c.fetchone()
    conn.rollback()
    print(f"Host: {app.ec2IP}:{app.port} | database: {db} | schema: {schema} | mode: {'APPLY' if args.apply else 'DRY RUN'}")
    failures = 0
    try:
        for table, entry in spec.items():
            print(f"\n== {table}: {len(entry['renames'])} rename(s), {len(entry['new'])} new column(s)")
            with conn.cursor() as cur:
                steps, notes, base = plan_table(cur, table, entry, args.add_missing)
            conn.rollback()
            for kind, tbl, a, b in steps:
                print(f"   {kind:<6} {tbl}.{a}" + (f" -> {b}" if b else " (text)"))
            for n in notes:
                print(f"   note   {n}")
            renames = [(o, n) for o, n in entry["renames"]]
            if not args.apply or (not steps and not renames):
                continue
            try:
                meta = apply_steps(conn, common_conn, steps, renames, base.upper())
                conn.commit()
                common_conn.commit()
                for m in meta:
                    print(f"   meta   {m}")
                print("   applied.")
            except Exception as e:
                conn.rollback()
                common_conn.rollback()
                failures += 1
                print(f"   FAILED (rolled back): {str(e).splitlines()[0]}")
        if not args.apply:
            print("\nDry run only. Re-run with --apply to execute.")
        return 1 if failures else 0
    finally:
        conn.close()
        common_conn.close()


if __name__ == "__main__":
    sys.exit(main())
