import argparse
import json
import os
import sys

import psycopg
from dotenv import load_dotenv
from tqdm import tqdm


def fetch_barycenter_type_id(cur):
    cur.execute("SELECT id FROM body_types WHERE name = 'Barycenter';")
    row = cur.fetchone()
    if not row:
        tqdm.write("Error: 'Barycenter' entry missing from body_types lookup")
        sys.exit(1)
    return row[0]


def load_parents(raw):
    if raw is None:
        return []
    if isinstance(raw, (list, tuple)):
        return list(raw)
    try:
        parsed = json.loads(raw)
        return parsed if isinstance(parsed, list) else []
    except (TypeError, json.JSONDecodeError):
        return []


def is_null_referenced(parents, body_id):
    return any(
        isinstance(entry, dict) and entry.get("Null") == body_id for entry in parents
    )


def find_corrupted_rows(cur, barycenter_type_id):
    """Bodies classified as something else while a sibling's parents
    reference them as {"Null": body_id}: per EDDN, those references only
    ever point at barycenters."""
    cur.execute(
        """
        SELECT DISTINCT b.system_id64, b.body_id
        FROM bodies b
        JOIN bodies child ON child.system_id64 = b.system_id64
        WHERE b.body_type_id <> %s
          AND b.body_type_id IS NOT NULL
          AND child.parents::text LIKE '%%Null%%'
        ORDER BY b.system_id64, b.body_id
        """,
        (barycenter_type_id,),
    )
    return cur.fetchall()


def repair_system(conn, system_id64, body_ids, barycenter_type_id, dry_run=False):
    cur = conn.cursor()
    fixed = []
    cur.execute(
        """
        SELECT body_id, body_name, body_type_id, parents
        FROM bodies
        WHERE system_id64 = %s AND body_id = ANY(%s)
        """,
        (system_id64, body_ids),
    )
    for body_id, body_name, body_type_id, parents in cur.fetchall():
        if body_type_id == barycenter_type_id:
            continue
        if not is_null_referenced(load_parents(parents), body_id):
            continue
        fixed.append((body_id, body_name, body_type_id))
        if dry_run:
            tqdm.write(
                f"[DRY-RUN] {system_id64} body {body_id} '{body_name}': "
                f"type {body_type_id} -> {barycenter_type_id} (Barycenter)"
            )
        else:
            cur.execute(
                """
                UPDATE bodies
                SET body_type_id = %s
                WHERE system_id64 = %s AND body_id = %s
                """,
                (barycenter_type_id, system_id64, body_id),
            )
            tqdm.write(
                f"{system_id64} body {body_id} '{body_name}': "
                f"type {body_type_id} -> {barycenter_type_id} (Barycenter)"
            )
    cur.close()
    if fixed and not dry_run:
        conn.commit()
    return fixed


def main():
    parser = argparse.ArgumentParser(
        description=(
            "Re-classify bodies referenced as {\"Null\": body_id} in sibling "
            "parent chains back to Barycenter: EDDN parent chains only ever "
            "reference barycenters that way, but dump re-ingests can overwrite "
            "the type with a plain star classification."
        )
    )
    parser.add_argument(
        "system_id64",
        nargs="?",
        type=int,
        help="Process only this system_id64 when provided",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show the planned re-classifications without touching the database",
    )
    args = parser.parse_args()

    load_dotenv()

    DB_HOST = os.getenv("DB_HOST")
    DB_NAME = os.getenv("DB_NAME")
    DB_USER = os.getenv("DB_USER")
    DB_PASSWORD = os.getenv("DB_PASSWORD")

    if not DB_PASSWORD:
        tqdm.write("Error: DB_PASSWORD environment variable not set")
        sys.exit(1)

    conn = psycopg.connect(
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
        host=DB_HOST,
        port=5432,
    )

    with conn.cursor() as cur:
        barycenter_type_id = fetch_barycenter_type_id(cur)

    if args.system_id64 is not None:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT body_id
            FROM bodies
            WHERE system_id64 = %s AND body_type_id <> %s
            ORDER BY body_id
            """,
            (args.system_id64, barycenter_type_id),
        )
        body_ids = [row[0] for row in cur.fetchall()]
        cur.close()
        candidates = (
            [(args.system_id64, body_ids)] if body_ids else []
        )
    else:
        candidates = find_corrupted_rows(conn.cursor(), barycenter_type_id)

    if not candidates:
        tqdm.write("No misclassified barycenter rows found.")
        conn.close()
        return

    systems: dict[int, list[int]] = {}
    for system_id64, body_id in candidates:
        systems.setdefault(system_id64, []).append(body_id)

    total_fixed = 0
    for system_id64, body_ids in systems.items():
        total_fixed += len(
            repair_system(
                conn, system_id64, body_ids, barycenter_type_id, args.dry_run
            )
        )

    tqdm.write(
        f"{'Would fix' if args.dry_run else 'Fixed'} {total_fixed} body(ies)."
    )
    conn.close()


if __name__ == "__main__":
    main()
