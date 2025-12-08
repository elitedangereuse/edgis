import argparse
import json
import os
import psycopg
import re
import sys
from collections import defaultdict
from string import ascii_lowercase, ascii_uppercase
from typing import Optional, Tuple
from tqdm import tqdm
from dotenv import load_dotenv


def insert_barycenter_first_pass(conn, system_id, debug=False, dry_run=False):
    cur = conn.cursor()

    # Look for BodyID=1 with parents [{"Null": 0}]
    cur.execute(
        "SELECT body_id, body_name, body_type_id, parents FROM bodies WHERE system_id64 = %s AND body_id = 1",
        (system_id,),
    )
    row = cur.fetchone()

    if not row:
        if debug:
            tqdm.write("No BodyID=1 found, skipping first pass.")
        cur.close()
        return

    _, _, _, parents = row
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
            if dry_run:
                if debug:
                    tqdm.write(
                        f"[DRY-RUN] Would insert Barycenter0 into system {system_id}"
                    )
            else:
                if debug:
                    tqdm.write(f"Inserting Barycenter0 into system {system_id}")
                cur.execute(
                    """
                    INSERT INTO bodies (system_id64, body_id, body_name, body_type_id, parents)
                    VALUES (%s, %s, %s, %s, %s)
                    """,
                    (system_id, 0, "Barycenter0", 2, json.dumps([])),
                )

    cur.close()
    if not dry_run:
        conn.commit()


def rename_barycenters_second_pass(
    conn, system_id, debug=False, dry_run=False, reset=False
):
    cur = conn.cursor()

    cur.execute(
        "SELECT body_id, body_name, body_type_id, parents FROM bodies WHERE system_id64 = %s",
        (system_id,),
    )
    rows = cur.fetchall()

    if not rows:
        cur.close()
        return []

    system_name = fetch_system_name(conn, system_id)

    if reset:
        updates = []
        for body_id, name, type_id, _ in rows:
            if type_id != 2:
                continue
            new_name = f"Barycenter{body_id}"
            if new_name != name:
                if dry_run or debug:
                    prefix = "[DRY-RUN] " if dry_run else ""
                    tqdm.write(f"{prefix}{name} -> {new_name}")
                updates.append((new_name, system_id, body_id))
        if not dry_run and updates:
            for new_name, sys_id, body_id in updates:
                cur.execute(
                    "UPDATE bodies SET body_name = %s WHERE system_id64 = %s AND body_id = %s",
                    (new_name, sys_id, body_id),
                )
            conn.commit()
        cur.close()
        return updates

    nodes: dict[int, dict] = {}
    barycenter_children: dict[int, list[int]] = defaultdict(list)

    for body_id, name, type_id, parents in rows:
        if isinstance(parents, str):
            try:
                parents = json.loads(parents)
            except json.JSONDecodeError:
                parents = []
        null_parent, body_parent = parse_parents(parents)
        node = {
            "body_id": body_id,
            "body_name": name,
            "body_type_id": type_id,
            "parents": parents if parents else [],
            "null_parent": null_parent,
            "body_parent": body_parent,
            "parent_id": None,
            "parent_resolved": False,
            "level": None,
            "token": None,
        }
        nodes[body_id] = node
        if null_parent is not None:
            barycenter_children[null_parent].append(body_id)

    def resolve_parent(body_id: int) -> Optional[int]:
        node = nodes[body_id]
        if node["parent_resolved"]:
            return node["parent_id"]
        if node["body_parent"] is not None:
            node["parent_id"] = node["body_parent"]
            node["parent_resolved"] = True
            return node["body_parent"]
        null_parent = node["null_parent"]
        if null_parent is None:
            node["parent_id"] = None
            node["parent_resolved"] = True
            return None
        parent_node = nodes.get(null_parent)
        if not parent_node:
            node["parent_id"] = None
            node["parent_resolved"] = True
            return None
        resolved = resolve_parent(null_parent)
        node["parent_id"] = resolved
        node["parent_resolved"] = True
        return resolved

    def compute_level(body_id: int) -> int:
        node = nodes[body_id]
        if node["level"] is not None:
            return node["level"]
        parent_id = resolve_parent(body_id)
        if parent_id is None:
            node["level"] = 0
        else:
            parent = nodes.get(parent_id)
            parent_level = compute_level(parent_id) if parent else 0
            node["level"] = parent_level + 1
        return node["level"]

    for body_id in nodes:
        resolve_parent(body_id)
        compute_level(body_id)

    groups: dict[tuple[Optional[int], int], list[int]] = defaultdict(list)
    TOKEN_ELIGIBLE_TYPES = {3, 5}

    for body_id, node in nodes.items():
        if node["body_type_id"] not in TOKEN_ELIGIBLE_TYPES:
            continue
        level = node["level"] if node["level"] is not None else 0
        parent_key = node.get("parent_id")
        groups[(parent_key, level)].append(body_id)

    for (parent_key, level), child_ids in groups.items():
        sorted_ids = sorted(child_ids)
        tokens = generate_tokens_for_level(level, len(sorted_ids))
        for idx, body_id in enumerate(sorted_ids):
            nodes[body_id]["token"] = tokens[idx]

    primary_stars = [
        node
        for node in nodes.values()
        if node["body_type_id"] == 5 and node["level"] == 0
    ]
    if len(primary_stars) == 1:
        primary_stars[0]["token"] = ""

    barycenter_token_cache: dict[int, Optional[str]] = {}

    def compute_barycenter_token(body_id: int) -> Optional[str]:
        if body_id in barycenter_token_cache:
            return barycenter_token_cache[body_id]
        children = sorted(barycenter_children.get(body_id, []))
        collected = []
        bary_parent = nodes[body_id].get("parent_id")
        target_parent = bary_parent
        if target_parent is None:
            for child_id in children:
                child_parent = nodes.get(child_id, {}).get("parent_id")
                if child_parent is not None:
                    target_parent = child_parent
                    break
        for child_id in children:
            child = nodes[child_id]
            if target_parent is not None and child.get("parent_id") != target_parent:
                continue
            token: Optional[str]
            if child["body_type_id"] == 2:
                token = compute_barycenter_token(child_id)
            else:
                token = child.get("token")
            if token:
                collected.append((child_id, token))
        if len(collected) < 2:
            barycenter_token_cache[body_id] = None
            return None
        if len(collected) > 2:
            barycenter_token_cache[body_id] = None
            return None
        tokens = [token for _, token in collected]
        if all(is_uppercase_token(t) for t in tokens):
            combined = "".join(tokens)
        else:
            combined = "+".join(tokens)
        barycenter_token_cache[body_id] = combined
        return combined

    path_cache: dict[int, list[str]] = {}

    def collect_path_tokens(body_id: Optional[int]) -> list[str]:
        if body_id is None:
            return []
        if body_id in path_cache:
            return path_cache[body_id]
        node = nodes.get(body_id)
        if not node:
            return []
        parent_tokens = collect_path_tokens(node.get("parent_id"))
        if node["body_type_id"] == 2:
            tokens = parent_tokens
        else:
            token = node.get("token")
            tokens = parent_tokens + [token] if token else parent_tokens
        path_cache[body_id] = tokens
        return tokens

    updates = []

    for body_id, node in nodes.items():
        if node["body_type_id"] != 2 or body_id == 0:
            continue
        if not re.match(r"^Barycenter\s*\d+$", node["body_name"] or ""):
            continue
        token = compute_barycenter_token(body_id)
        if not token:
            if debug:
                tqdm.write(
                    f"Skipping {node['body_name']} – unable to compute child tokens"
                )
            continue
        parent_tokens = collect_path_tokens(node.get("parent_id"))
        all_tokens = parent_tokens + [token]
        suffix = " ".join(t for t in all_tokens if t)
        new_name = f"{system_name} {suffix}".strip() if suffix else system_name
        if new_name != node["body_name"]:
            if dry_run or debug:
                prefix = "[DRY-RUN] " if dry_run else ""
                tqdm.write(f"{prefix}{node['body_name']} -> {new_name}")
            updates.append((new_name, system_id, body_id))

    if not dry_run and updates:
        for new_name, sys_id, body_id in updates:
            cur.execute(
                "UPDATE bodies SET body_name = %s WHERE system_id64 = %s AND body_id = %s",
                (new_name, sys_id, body_id),
            )
        conn.commit()

    cur.close()
    return updates


def fetch_system_name(conn, system_id: int) -> str:
    with conn.cursor() as cur:
        cur.execute(
            "SELECT name FROM systems_big WHERE id64 = %s",
            (system_id,),
        )
        row = cur.fetchone()
    return row[0] if row and row[0] else str(system_id)


def parse_parents(parents) -> Tuple[Optional[int], Optional[int]]:
    if not parents or not isinstance(parents, list):
        return None, None
    null_parent: Optional[int] = None
    body_parent: Optional[int] = None
    for parent in parents:
        if not isinstance(parent, dict):
            continue
        for key, value in parent.items():
            try:
                parent_id = int(value)
            except (TypeError, ValueError):
                parent_id = None
            if parent_id is None:
                continue
            if key == "Null":
                if null_parent is None:
                    null_parent = parent_id
            else:
                if body_parent is None:
                    body_parent = parent_id
        if null_parent is not None and body_parent is not None:
            break
    return null_parent, body_parent


def generate_tokens_for_level(level: int, count: int) -> list[str]:
    if count <= 0:
        return []
    if level == 0:
        alphabet = ascii_uppercase
        return [letters_from_index(i, alphabet) for i in range(count)]
    if level % 2 == 1:
        return [str(i) for i in range(1, count + 1)]
    alphabet = ascii_lowercase
    return [letters_from_index(i, alphabet) for i in range(count)]


def letters_from_index(index: int, alphabet: str) -> str:
    base = len(alphabet)
    value = index
    token = ""
    while True:
        token = alphabet[value % base] + token
        value = value // base - 1
        if value < 0:
            break
    return token


def is_uppercase_token(token: str) -> bool:
    return bool(token) and token.replace(" ", "").isalpha() and token.upper() == token


def main():
    parser = argparse.ArgumentParser(
        description="Rename barycenters using structural positions instead of child names."
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
        help="Show the planned renames without touching the database",
    )
    parser.add_argument(
        "--reset",
        action="store_true",
        help="Reset barycenter names to 'Barycenter <body_id>' instead of computing tree-based names",
    )
    args = parser.parse_args()

    system_id_arg = args.system_id64

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
    if system_id_arg is not None:
        debug = True
        system_ids = [system_id_arg]
    # all candidates from the database
    else:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT system_id64
                FROM bodies
                WHERE body_name LIKE 'Barycenter%'
                AND body_type_id = 2
                LIMIT 100000
            """
            )
            system_ids = [int(row[0]) for row in cur.fetchall()]
        tqdm.write(f"Found {len(system_ids)} systems with barycenters to fix")

    # === Process Each System ===
    for system_id in tqdm(system_ids, desc="Processing systems", unit="system"):
        try:
            insert_barycenter_first_pass(conn, system_id, debug, dry_run=args.dry_run)
            updates = rename_barycenters_second_pass(
                conn,
                system_id,
                debug,
                dry_run=args.dry_run,
                reset=args.reset,
            )
            if updates:
                action_word = "reset" if args.reset else "renamed"
                tqdm.write(
                    f"System {system_id}: {len(updates)} barycenter(s) "
                    f"{'would be ' if args.dry_run else ''}{action_word}"
                )
        except Exception as e:
            tqdm.write(f"Error processing system {system_id}: {e}")
            conn.rollback()  # Reset transaction on error

    conn.close()


if __name__ == "__main__":
    main()
