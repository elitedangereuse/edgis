import json
import os
import psycopg
import re
import sys
from tqdm import tqdm
from dotenv import load_dotenv


def insert_barycenter_first_pass(conn, system_id, debug=False):
    cur = conn.cursor()

    # Look for BodyID=1 with parents [{"Null": 0}]
    cur.execute(
        "SELECT body_id, body_name, type, parents FROM bodies WHERE system_id64 = %s AND body_id = 1",
        (system_id,),
    )
    row = cur.fetchone()

    if not row:
        if debug:
            tqdm.write("No BodyID=1 found, skipping first pass.")
        cur.close()
        return

    body_id, body_name, body_type, parents = row
    parents = parents if parents else []

    if any(isinstance(p, dict) and p.get("Null") == 0 for p in parents):
        # Check if BodyID=0 already exists (avoid duplicate insert)
        cur.execute(
            "SELECT 1 FROM bodies WHERE system_id64 = %s AND body_id = 0",
            (system_id,),
        )
        if cur.fetchone():
            if debug:
                tqdm.write("Barycenter0 already exists, skipping insert.")
        else:
            if debug:
                tqdm.write(f"Inserting Barycenter0 into system {system_id}")
            cur.execute(
                """
                INSERT INTO bodies (system_id64, body_id, body_name, type, parents)
                VALUES (%s, %s, %s, %s, %s)
                """,
                (system_id, 0, "Barycenter0", "Barycenter", json.dumps([])),
            )

    cur.close()
    conn.commit()


def rename_barycenters_second_pass(conn, system_id, debug=False):
    cur = conn.cursor()

    cur.execute(
        "SELECT body_id, body_name, type, parents FROM bodies WHERE system_id64 = %s ORDER BY body_id",
        (system_id,),
    )
    rows = cur.fetchall()

    bodies = {
        row[0]: {
            "body_id": row[0],
            "body_name": row[1],
            "type": row[2],
            "parents": row[3] if row[3] else [],
        }
        for row in rows
    }

    renamed_count = 0

    # Helper: extract suffix and base name
    def get_suffix_and_base(name: str):
        parts = name.split(" ")
        if len(parts) < 2:
            return None, name
        suffix = parts[-1]
        base = " ".join(parts[:-1])
        return suffix, base

    for body_id, body in bodies.items():
        if body["type"] != "Barycenter" or not re.match(
            r"Barycenter\d+", body["body_name"]
        ):
            continue

        # === Step 1: Get immediate next body (body_id + 1) ===
        candidate_child1_id = body_id + 1
        child1 = bodies.get(candidate_child1_id)

        if not child1:
            if debug:
                tqdm.write(
                    f"Skipping {body['body_name']} – no body with ID {candidate_child1_id}"
                )
            continue

        # Check if child1 is actually a child (has Null: body_id in parents)
        parent_nulls = [p for p in child1["parents"] if "Null" in p]
        if not any(p["Null"] == body_id for p in parent_nulls):
            if debug:
                tqdm.write(
                    f"Skipping {body['body_name']} – next body (ID {candidate_child1_id}) is not a child"
                )
            continue

        # Must be a planet or star or barycenter
        if child1["type"] not in ("Planet", "Star", "Barycenter"):
            if debug:
                tqdm.write(
                    f"Skipping {body['body_name']} – child1 is a {child1['type']}"
                )
            continue

        suffix1, base1 = get_suffix_and_base(child1["body_name"])
        if not suffix1:
            if debug:
                tqdm.write(
                    f"Skipping {body['body_name']} – child1 has no suffix: {child1['body_name']}"
                )
            continue

        type1 = get_suffix_type(suffix1)
        # === Step 2: Find second child ===
        # Look through ALL bodies for another planet/star with:
        # - Same barycenter parent
        # - Same suffix type
        # - Same base name (optional, but preferred)
        candidates = []
        for b_id, b in bodies.items():
            if debug:
                tqdm.write(f"processing {b_id}")
            if b_id == candidate_child1_id:  # skip the first child
                if debug:
                    tqdm.write(
                        f"skip first child {b_id} (type: {type1} / suffix: {suffix1})"
                    )
                continue
            if b["type"] not in ("Planet", "Star", "Barycenter"):
                if debug:
                    tqdm.write(f"not in Planet or Star or Barycenter {b_id}")
                continue

            b_parent_nulls = [p for p in b["parents"] if "Null" in p]
            if not any(p["Null"] == body_id for p in b_parent_nulls):
                if debug:
                    tqdm.write(f"not any Null {b_id}")
                continue

            # Check if the VERY FIRST parent is {"Null": body_id}
            parent_list = b["parents"]
            if not parent_list or not isinstance(parent_list, list):
                if debug:
                    tqdm.write(f"skip {b_id} - invalid or missing parents list")
                continue

            first_parent = parent_list[0]
            if (
                not isinstance(first_parent, dict)
                or "Null" not in first_parent
                or first_parent["Null"] != body_id
            ):
                if debug:
                    tqdm.write(
                        f"skip {b_id} - first parent is not {{'Null': {body_id}}}"
                    )
                continue

            suffix2, base2 = get_suffix_and_base(b["body_name"])
            if not suffix2:
                if debug:
                    tqdm.write(f"skip suffix issue {b_id}")
                continue

            type2 = get_suffix_type(suffix2)
            if type2 != type1:
                if debug:
                    tqdm.write(f"skip different suffix types {b_id}: {type2} - {type1}")
                continue  # must match suffix type

            # Prefer same base name, but allow fallback
            if base2 == base1:
                candidates.insert(0, (b, suffix2))  # prioritize matching base
            else:
                candidates.append((b, suffix2))

        if not candidates:
            if debug:
                tqdm.write(
                    f"Skipping {body['body_name']} – no valid second child found"
                )
            continue

        child2, suffix2 = candidates[0]  # pick best candidate
        type2 = get_suffix_type(suffix2)
        if debug:
            tqdm.write(f"candidate: {suffix2} ({type2} vs {type1})")

        # === Step 3: Generate new name ===
        if type1 == "UPPER" and type2 == "UPPER":
            new_name = f"{base1} {suffix1}{suffix2}"
        elif type1 in ("DIGIT", "LOWER"):
            new_name = f"{base1} {suffix1}+{suffix2}"
        else:
            new_name = f"{base1} {suffix1}+{suffix2}"

        tqdm.write(f"{new_name}")
        bodies[candidate_child1_id]["parents"].pop(0)
        # Fix parents of the barycenter
        cur.execute(
            "UPDATE bodies SET body_name = %s, parents = %s WHERE system_id64 = %s AND body_id = %s",
            (
                new_name,
                json.dumps(bodies[candidate_child1_id]["parents"]),
                system_id,
                body_id,
            ),
        )
        renamed_count += 1

    conn.commit()
    cur.close()
    return renamed_count


def get_suffix_type(suffix: str) -> str:
    parts = suffix.split("+")
    if all(re.fullmatch(r"[A-Z]+", p) for p in parts):
        return "UPPER"
    elif all(re.fullmatch(r"[a-z]+", p) for p in parts):
        return "LOWER"
    elif all(re.fullmatch(r"\d+", p) for p in parts):
        return "DIGIT"
    else:
        return "OTHER"


def get_last_suffix(name: str) -> str:
    """Extracts the last suffix (A, B, C, 1, 2, 10, a, b, etc.) from a body name."""
    parts = name.split(" ")
    return parts[-1]


def main():
    # === Parse Command Line Arguments ===
    if len(sys.argv) != 1 and len(sys.argv) != 2:
        tqdm.write("Usage: python script.py [system_id64]")
        tqdm.write("  - No argument: process all systems from 'new_barycenters.txt'")
        tqdm.write("  - With argument: process only that system_id64")
        sys.exit(1)

    system_id_arg = sys.argv[1] if len(sys.argv) == 2 else None

    # === Database Connection ===
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

    debug = False
    # specific system
    if system_id_arg:
        debug = True
        system_ids = [system_id_arg]
    # all candidates from the database
    else:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT DISTINCT system_id64
                FROM bodies
                WHERE body_name LIKE 'Barycenter%'
                ORDER BY system_id64
            """
            )
            system_ids = [str(row[0]) for row in cur.fetchall()]
        tqdm.write(f"Found {len(system_ids)} systems with barycenters to fix")

    # === Process Each System ===
    for system_id in tqdm(system_ids, desc="Processing systems", unit="system"):
        try:
            insert_barycenter_first_pass(conn, system_id, debug)
            while True:
                renamed_count = rename_barycenters_second_pass(conn, system_id, debug)
                if renamed_count == 0:
                    break
        except Exception as e:
            tqdm.write(f"Error processing system {system_id}: {e}")
            conn.rollback()  # Reset transaction on error

    conn.close()


if __name__ == "__main__":
    main()
