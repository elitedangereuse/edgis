import json
import os
import gzip

import psycopg2
from dotenv import load_dotenv
from tqdm import tqdm

load_dotenv()

DB_HOST = os.getenv("DB_HOST")
DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
BATCH_SIZE = 100000  # Define your batch size


def import_systems(file_path):
    # Connect to your PostgreSQL database
    conn = psycopg2.connect(
        dbname=DB_NAME, user=DB_USER, password=DB_PASSWORD, host=DB_HOST
    )
    cursor = conn.cursor()
    total_bytes = os.path.getsize(file_path)
    is_gz = file_path.endswith(".gz")

    opener = gzip.open if is_gz else open

    # Open the JSON file and show ingestion progress based on bytes read
    with opener(file_path, "rb") as file, tqdm(
        total=total_bytes,
        unit="B",
        unit_scale=True,
        unit_divisor=1024,
        desc="Ingesting systems",
    ) as progress:
        # Read the opening bracket of the JSON array
        if is_gz:
            prev_pos = file.fileobj.tell()
            header = file.readline()
            current_pos = file.fileobj.tell()
            progress.update(current_pos - prev_pos)
            prev_pos = current_pos
        else:
            progress.update(
                len(file.readline())
            )  # Skip the first line which is '['
            prev_pos = None

        batch = []  # List to hold records for the current batch
        for raw_line in file:
            if is_gz:
                current_pos = file.fileobj.tell()
                progress.update(current_pos - prev_pos)
                prev_pos = current_pos
            else:
                progress.update(len(raw_line))

            line = raw_line.strip()
            if not line:
                continue

            if (
                line == b"]"
            ):  # Stop processing when we reach the closing bracket
                break

            if line.endswith(b","):  # Remove the trailing comma if present
                line = line[:-1]

            # Load JSON object
            try:
                system = json.loads(line.decode("utf-8"))
            except json.JSONDecodeError as e:
                printable_line = line.decode("utf-8", errors="replace")
                print(
                    f"Error decoding JSON line: {printable_line}. Error: {e}"
                )
                continue

            # Extract necessary data
            id64 = system.get("id64")
            name = system.get("name")
            main_star = system.get(
                "mainStar", None
            )  # Default to None if not found
            coords = system.get("coords", {})
            update_time = system.get("updateTime")

            # Create geometry POINTZ
            if coords:
                pointz = f"POINTZ({coords.get('x', 0)} {coords.get('y', 0)} {coords.get('z', 0)})"
            else:
                print(f"Missing coordinates for system {id64}")
                continue  # Skip this entry if no coordinates

            # Add this record to the batch
            batch.append((id64, name, main_star, pointz, update_time))

            # If the batch reaches the specified size, insert and commit
            if len(batch) >= BATCH_SIZE:
                try:
                    # Execute batch insert
                    cursor.executemany(
                        """
                        INSERT INTO systems_big (id64, name, mainstar, coords, updatetime)
                        VALUES (%s, %s, %s, ST_SetSRID(ST_GeomFromText(%s), 0), %s)
                        ON CONFLICT (id64) DO UPDATE
                        SET mainstar = COALESCE(systems_big.mainstar, EXCLUDED.mainstar),
                        name = EXCLUDED.name,
                        coords = EXCLUDED.coords,
                        updatetime = EXCLUDED.updatetime;
                        """,
                        batch,
                    )
                    conn.commit()
                    print(f"Inserted {len(batch)} records.")
                except Exception as e:
                    print(f"Error inserting batch: {e}")
                    conn.rollback()  # Rollback on error
                batch.clear()  # Clear the batch

        # Insert any remaining records in the last batch
        if batch:
            try:
                cursor.executemany(
                    """
                    INSERT INTO systems_big (id64, name, mainstar, coords, updatetime)
                    VALUES (%s, %s, %s, ST_SetSRID(ST_GeomFromText(%s), 0), %s)
                    ON CONFLICT (id64) DO UPDATE
                    SET mainstar = COALESCE(systems_big.mainstar, EXCLUDED.mainstar),
                    name = EXCLUDED.name,
                    coords = EXCLUDED.coords,
                    updatetime = EXCLUDED.updatetime;
                    """,
                    batch,
                )
                conn.commit()
                print(f"Inserted {len(batch)} remaining records.")
            except Exception as e:
                print(f"Error inserting final batch: {e}")
                conn.rollback()  # Rollback on error

    # Close the connection
    cursor.close()
    conn.close()


if __name__ == "__main__":
    import_systems("systems.json")
