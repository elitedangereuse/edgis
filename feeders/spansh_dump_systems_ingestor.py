import os
import gzip
import json
import psycopg2
from psycopg2.extras import execute_values
from dotenv import load_dotenv
from tqdm import tqdm
import ijson

load_dotenv()

DB_HOST = os.getenv("DB_HOST")
DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
BATCH_SIZE = 5000  # Larger batch size for fewer commits


def import_systems(file_path):
    conn = psycopg2.connect(
        dbname=DB_NAME, user=DB_USER, password=DB_PASSWORD, host=DB_HOST
    )
    cursor = conn.cursor()

    is_gz = file_path.endswith(".gz")
    opener = gzip.open if is_gz else open

    total_bytes = os.path.getsize(file_path)
    batch = []

    with opener(file_path, "rb") as file, tqdm(
        unit="systems", desc="Ingesting systems"
    ) as pbar:
        for system in ijson.items(file, "item"):
            id64 = system.get("id64")
            name = system.get("name")
            main_star = system.get("mainStar")
            coords = system.get("coords", {})
            update_time = system.get("updateTime")

            if not coords:
                continue

            pointz = f"POINTZ({coords.get('x', 0)} {coords.get('y', 0)} {coords.get('z', 0)})"
            batch.append((id64, name, main_star, pointz, update_time))

            if len(batch) >= BATCH_SIZE:
                insert_batch(cursor, batch)
                conn.commit()
                batch.clear()
            pbar.update(1)

        if batch:
            insert_batch(cursor, batch)
            conn.commit()

    cursor.close()
    conn.close()


def insert_batch(cursor, batch):
    query = """
        INSERT INTO systems_big (id64, name, mainstar, coords, updatetime)
        VALUES %s
        ON CONFLICT (id64) DO UPDATE
        SET mainstar = COALESCE(systems_big.mainstar, EXCLUDED.mainstar),
            name = EXCLUDED.name,
            coords = EXCLUDED.coords,
            updatetime = EXCLUDED.updatetime;
    """
    try:
        execute_values(
            cursor,
            query,
            batch,
            template="(%s, %s, %s, ST_SetSRID(ST_GeomFromText(%s), 0), %s)",
        )
    except Exception as e:
        print(f"Error inserting batch: {e}")


if __name__ == "__main__":
    import_systems("systems.json.gz")
