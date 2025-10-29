import json
import os
import shutil
import sys
import time
from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parent.parent
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

import psycopg
from dotenv import load_dotenv

from feeders import feederbodies

ERROR_LOG_FILE = "error_log.jsonl"
TEMP_LOG_FILE = "error_log.jsonl.tmp"
FK_VIOLATION = "fkey"

if hasattr(feederbodies, "conn"):
    try:
        feederbodies.conn.close()
    except Exception:
        pass


def _open_connection() -> psycopg.Connection:
    load_dotenv()
    host = os.getenv("DB_HOST")
    name = os.getenv("DB_NAME")
    user = os.getenv("DB_USER")
    password = os.getenv("DB_PASSWORD")
    return psycopg.connect(
        host=host, port=5432, dbname=name, user=user, password=password
    )


def _load_error_lines() -> list[str]:
    if not Path(ERROR_LOG_FILE).exists():
        return []
    with open(ERROR_LOG_FILE, "r", encoding="utf-8") as handle:
        return [line.strip() for line in handle if line.strip()]


def reprocess() -> None:
    error_lines = _load_error_lines()
    if not error_lines:
        print(f"No error log found: {ERROR_LOG_FILE}")
        return

    success_count = 0
    remaining: list[str] = []

    with _open_connection() as conn, open(
        TEMP_LOG_FILE, "w", encoding="utf-8"
    ) as temp_out:
        for index, raw in enumerate(error_lines, start=1):
            try:
                entry = json.loads(raw)
                error_msg = entry.get("error", "")
                header = entry.get("header", {})
                payload = entry.get("message", {})
                event = payload.get("event")

                if FK_VIOLATION not in error_msg:
                    print(
                        f"[{index}/{len(error_lines)}] Skipping non-FK error: {event}"
                    )
                    remaining.append(raw)
                    continue

                message = {"header": header, "message": payload}
                outcome = feederbodies.process_message(
                    message, connection=conn, verbose=False
                )

                if outcome.status != "success":
                    reason = outcome.reason or "unknown_reason"
                    print(f"[{index}/{len(error_lines)}] Skipped: {reason}")
                    remaining.append(raw)
                    continue

                success_count += 1
            except Exception as exc:
                print(f"[{index}/{len(error_lines)}] REPROCESS ERROR: {exc}")
                conn.rollback()
                remaining.append(raw)
            finally:
                time.sleep(0.01)

        for line in remaining:
            temp_out.write(f"{line}\n")

    shutil.move(TEMP_LOG_FILE, ERROR_LOG_FILE)
    print(
        f"Reprocessing complete. {success_count} succeeded, {len(remaining)} still failed."
    )


if __name__ == "__main__":
    reprocess()
