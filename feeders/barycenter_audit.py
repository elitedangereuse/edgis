#!/usr/bin/env python3
"""Read-only audit of barycenter names: find the ones that are wrong or missing.

NO WRITES -- only SELECTs. For each barycenter it recomputes the name from its
DIRECT children's real designations (the authoritative method the renamer uses)
and compares against the stored ``body_name``, bucketing each into:

  unrenamed   -- still ``Barycenter<N>``: the renamer never resolved it.
  mismatch    -- renamed, but != the name derived from the children now. The
                 common cause is v1 only ever pairing TWO children (body_id+1 and
                 one match), so a 3+ body barycenter keeps a two-body name
                 (e.g. ``Foo 1+2`` where the members are 1, 2 and 3).
  underivable -- children carry no designation suffix (named bodies like
                 Pluto/Charon), so no name can be derived from the body names
                 alone. Reported, not "fixed" -- a correct name needs the
                 system's planet/star ordering, which is not in the body name.
  ok          -- renamed and matches the derived name.

Run on the feeder box (needs the DB env, same as the other feeders). Bounded by
``--limit`` so it never scans the whole barycenter set unbounded; ``--system``
audits one system id64 verbosely.

  python barycenter_audit.py --limit 2000
  python barycenter_audit.py --system 10477373803
"""
import argparse
import os
import re
import sys

import psycopg
from dotenv import load_dotenv

# Lookup id for the 'Barycenter' body type (2 in the current schema); over' able
# via env for safety if the lookup table ever shifts.
BARYCENTER_TYPE_ID = int(os.getenv("BARYCENTER_TYPE_ID", "2"))
# Body types that can be a named member of a barycenter: Barycenter, Planet, Star.
MEMBER_TYPE_IDS = (2, 3, 5)


def suffix_and_base(name):
    """Split 'Sol 9' -> ('9', 'Sol'); a name with no space -> (None, name)."""
    parts = (name or "").split(" ")
    if len(parts) < 2:
        return None, name
    return parts[-1], " ".join(parts[:-1])


def suffix_type(suffix):
    parts = (suffix or "").split("+")
    if parts and all(re.fullmatch(r"[A-Z]+", p) for p in parts):
        return "UPPER"
    if parts and all(re.fullmatch(r"[a-z]+", p) for p in parts):
        return "LOWER"
    if parts and all(re.fullmatch(r"\d+", p) for p in parts):
        return "DIGIT"
    return "OTHER"


def direct_children(barycenter_id, bodies):
    """Members whose FIRST parent is this barycenter (matches the renamer's
    membership rule), as (body_id, name)."""
    out = []
    for b in bodies.values():
        parents = b["parents"] or []
        if not parents or not isinstance(parents[0], dict):
            continue
        if parents[0].get("Null") != barycenter_id:
            continue
        if b["body_type_id"] not in MEMBER_TYPE_IDS:
            continue
        out.append((b["body_id"], b["body_name"]))
    out.sort(key=lambda c: c[0])  # body_id ~ designation order
    return out


def derive_name(barycenter_id, bodies):
    """Expected name from the children's real designations, or a reason it can't
    be derived. Returns (name, None) or (None, reason).

    A barycenter is designated by its co-orbiting members of ONE type: stars give
    letters ('BC'), planets give numbers ('9+10'). Circumbinary planets share the
    barycenter as a parent too but are a DIFFERENT suffix type, so combine only
    the type of the first (primary) member -- otherwise 'BC' becomes 'B+C+1'.
    Named satellites with no designation suffix (Pluto) are skipped, not failed
    on; a barycenter of ONLY named bodies is 'underivable' (the number comes from
    the system's planet order, which is not in the body name)."""
    children = direct_children(barycenter_id, bodies)
    typed = []  # (body_id, suffix, base, type) for suffix-bearing children
    for bid, name in children:
        suf, base = suffix_and_base(name)
        if not suf:
            continue  # named satellite (e.g. Pluto) -- not part of the designation
        typed.append((bid, suf, base, suffix_type(suf)))
    if len(typed) < 2:
        return None, "fewer than 2 children with a designation suffix"
    primary_type = typed[0][3]
    members = [t for t in typed if t[3] == primary_type]
    if len(members) < 2:
        return None, f"only 1 member of the primary type {primary_type}"
    base = members[0][2]
    sufs = [m[1] for m in members]
    combined = "".join(sufs) if primary_type == "UPPER" else "+".join(sufs)
    return f"{base} {combined}", None


def _load_system_bodies(cur, system_id):
    cur.execute(
        "SELECT body_id, body_name, body_type_id, parents "
        "FROM bodies WHERE system_id64 = %s",
        (system_id,),
    )
    return {
        r[0]: {"body_id": r[0], "body_name": r[1],
               "body_type_id": r[2], "parents": r[3] or []}
        for r in cur.fetchall()
    }


def audit_system(cur, system_id, verbose=False):
    """Return a list of (bucket, body_id, stored, derived_or_reason) for every
    barycenter in the system."""
    bodies = _load_system_bodies(cur, system_id)
    findings = []
    for b in bodies.values():
        if b["body_type_id"] != BARYCENTER_TYPE_ID:
            continue
        stored = b["body_name"]
        if stored and re.fullmatch(r"Barycenter\s*\d+", stored):
            findings.append(("unrenamed", b["body_id"], stored, None))
            continue
        derived, reason = derive_name(b["body_id"], bodies)
        if derived is None:
            findings.append(("underivable", b["body_id"], stored, reason))
        elif derived == stored:
            findings.append(("ok", b["body_id"], stored, derived))
        else:
            findings.append(("mismatch", b["body_id"], stored, derived))
    if verbose:
        for bucket, bid, stored, extra in findings:
            print(f"  [{bucket:11}] body {bid}: {stored!r}"
                  + (f"  -> expected {extra!r}" if bucket == "mismatch" else
                     f"  ({extra})" if extra and bucket != "ok" else ""))
    return findings


def main():
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--limit", type=int, default=2000,
                    help="max RENAMED barycenters to sample (bounded read)")
    ap.add_argument("--system", type=int, default=None,
                    help="audit one system id64 verbosely")
    ap.add_argument("--show", type=int, default=40,
                    help="how many mismatch examples to print")
    args = ap.parse_args()

    load_dotenv()
    conn = psycopg.connect(
        host=os.getenv("DB_HOST"), port=5432, dbname=os.getenv("DB_NAME"),
        user=os.getenv("DB_USER"), password=os.getenv("DB_PASSWORD"),
    )
    conn.read_only = True  # defence in depth: this tool never writes
    cur = conn.cursor()

    if args.system is not None:
        print(f"System {args.system}:")
        audit_system(cur, args.system, verbose=True)
        conn.close()
        return

    # Sample RENAMED barycenters (already resolved, so no longer 'Barycenter%').
    # Plain LIMIT (no DISTINCT) streams and stops early -- renamed barycenters are
    # common, so the first --limit matching rows are found without a full scan.
    # Dedupe the system ids in Python, then audit each whole system.
    cur.execute(
        "SELECT system_id64 FROM bodies "
        "WHERE body_type_id = %s AND body_name NOT LIKE 'Barycenter%%' "
        "LIMIT %s",
        (BARYCENTER_TYPE_ID, args.limit),
    )
    seen = set()
    system_ids = []
    for (sid,) in cur.fetchall():
        if sid not in seen:
            seen.add(sid)
            system_ids.append(sid)
    print(f"Auditing {len(system_ids)} systems with renamed barycenters...\n")

    counts = {"ok": 0, "mismatch": 0, "underivable": 0, "unrenamed": 0}
    ex = {"mismatch": [], "underivable": [], "unrenamed": []}
    for sid in system_ids:
        for bucket, bid, stored, extra in audit_system(cur, sid):
            counts[bucket] += 1
            if bucket in ex and len(ex[bucket]) < args.show:
                ex[bucket].append((sid, bid, stored, extra))
    conn.close()

    total = sum(counts.values())
    print(f"Barycenters seen: {total}")
    for k in ("ok", "mismatch", "underivable", "unrenamed"):
        pct = (100.0 * counts[k] / total) if total else 0.0
        print(f"  {k:12} {counts[k]:7}  ({pct:4.1f}%)")
    if ex["mismatch"]:
        print("\nMISMATCH (renamed wrong; stored -> derived-from-children):")
        for sid, bid, stored, derived in ex["mismatch"]:
            print(f"  sys {sid} body {bid}: {stored!r} -> {derived!r}")
    if ex["underivable"]:
        print("\nUNDERIVABLE (named members; no suffix to derive from):")
        for sid, bid, stored, reason in ex["underivable"]:
            print(f"  sys {sid} body {bid}: {stored!r}  ({reason})")
    if ex["unrenamed"]:
        print("\nUNRENAMED (still placeholder; v1 never resolved these):")
        for sid, bid, stored, _ in ex["unrenamed"]:
            print(f"  sys {sid} body {bid}: {stored!r}")


if __name__ == "__main__":
    main()
