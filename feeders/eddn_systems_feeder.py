import os
import zmq
import zlib
import json
import psycopg
from datetime import datetime
from dotenv import load_dotenv

# === Trusted Clients List ===
TRUSTED_CLIENTS = {
    "EDDI",
    "EDDiscovery",
    "EDDLite",
    "E:D Market Connector [Linux]",
    "E:D Market Connector [Windows]",
    "EDO Materials Helper",
}

# === Coordinate Bounds ===
MAX_XYZ = 70000  # Light years
MAX_Y = 35000


def is_valid_coordinates(x, y, z):
    """Check if coordinates are within plausible bounds and not suspicious test values."""
    if not (-MAX_XYZ <= x <= MAX_XYZ):
        return False
    if not (-MAX_Y <= y <= MAX_Y):
        return False
    if not (-MAX_XYZ <= z <= MAX_XYZ):
        return False

    # Reject common test coordinates near origin that aren't Sol
    if abs(x) <= 1 and abs(y) <= 1 and abs(z) <= 1:
        return x == 0 and y == 0 and z == 0  # Only allow (0,0,0) = Sol

    return True


def is_valid_system_name(name):
    """Reject obviously fake or placeholder system names."""
    if not name or not name.strip():
        return False
    bad_names = {
        "Test",
        "test",
        "TEST",
        "Dummy",
        "Unknown",
        "null",
        "None",
        "",
    }
    if name in bad_names:
        return False
    return True


def is_trusted_source(software_name):
    """Check if the data comes from a trusted software client."""
    return software_name in TRUSTED_CLIENTS


# === Database Connection ===
load_dotenv()

DB_HOST = os.getenv("DB_HOST")
DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")

conn = psycopg.connect(
    host=DB_HOST, port=5432, dbname=DB_NAME, user=DB_USER, password=DB_PASSWORD
)

# === UPSERT Query ===
UPSERT_QUERY = """
    INSERT INTO systems_big (id64, name, mainstar, updatetime, coords)
    VALUES (%s, %s, %s, %s, ST_MakePoint(%s, %s, %s)::geometry(PointZ))
    ON CONFLICT (id64) DO UPDATE SET
        name      = COALESCE(EXCLUDED.name, systems_big.name),
        mainstar  = COALESCE(EXCLUDED.mainstar, systems_big.mainstar),
        updatetime= EXCLUDED.updatetime,
        coords    = EXCLUDED.coords;
"""

# === ZMQ Setup ===
context = zmq.Context()
socket = context.socket(zmq.SUB)
socket.connect("tcp://eddn.edcd.io:9500")
socket.setsockopt_string(zmq.SUBSCRIBE, "")

print("✅ Listening for EDDN star scans and NavRoutes from trusted clients...")

# === Main Loop ===
while True:
    try:
        # Receive and decompress message
        compressed = socket.recv()
        decompressed = zlib.decompress(compressed)
        message = json.loads(decompressed.decode("utf-8"))

        # Extract header and message
        header = message.get("header", {})
        msg_data = message.get("message", {})
        event = msg_data.get("event")

        # --- SECURITY FILTERS ---
        software_name = header.get("softwareName")
        if not is_trusted_source(software_name):
            # Optional: log untrusted sources occasionally
            print(f"Warning: Untrusted source ignored: {software_name}")
            continue

        # --- Handle Scan Events (Stars) ---
        if event == "Scan" and "StarType" in msg_data:
            system_address = msg_data.get("SystemAddress")
            star_system = msg_data.get("StarSystem")
            star_type = msg_data.get("StarType")
            star_pos = msg_data.get("StarPos")
            timestamp = msg_data.get("timestamp")

            if not all([system_address, star_system, star_pos, timestamp]):
                continue

            if not is_valid_system_name(star_system):
                print(f"Warning: Invalid system name, skipping: {star_system}")
                continue

            if len(star_pos) != 3:
                continue

            x, y, z = (
                float(star_pos[0]),
                float(star_pos[1]),
                float(star_pos[2]),
            )
            if not is_valid_coordinates(x, y, z):
                print(
                    f"Warning: Invalid or suspicious coordinates, skipping: {star_system} [{x}, {y}, {z}]"
                )
                continue

            try:
                updatetime = datetime.fromisoformat(timestamp.replace("Z", "+00:00"))
            except ValueError:
                print(f"Warning: Invalid timestamp format: {timestamp}")
                continue

            # Upsert star scan
            with conn.cursor() as cur:
                cur.execute(
                    UPSERT_QUERY,
                    (
                        system_address,
                        star_system,
                        star_type,
                        updatetime,
                        x,
                        y,
                        z,
                    ),
                )
            conn.commit()
            print(f"Scan: {star_system} [{system_address}] | Type: {star_type}")

        # --- Handle NavRoute Events ---
        elif event == "NavRoute" and "Route" in msg_data:
            route = msg_data["Route"]
            timestamp = msg_data.get("timestamp")

            try:
                updatetime = datetime.fromisoformat(timestamp.replace("Z", "+00:00"))
            except (ValueError, TypeError):
                print(f"Warning: Invalid timestamp in NavRoute: {timestamp}")
                continue

            systems_added = 0
            for star in route:
                system_address = star.get("SystemAddress")
                star_system = star.get("StarSystem")
                star_class = star.get("StarClass")  # e.g., "K", "N", "M"
                star_pos = star.get("StarPos")  # [x, y, z]

                if not all([system_address, star_system, star_pos]):
                    continue

                if not is_valid_system_name(star_system):
                    continue

                if len(star_pos) != 3:
                    continue

                x, y, z = (
                    float(star_pos[0]),
                    float(star_pos[1]),
                    float(star_pos[2]),
                )
                if not is_valid_coordinates(x, y, z):
                    print(
                        f"Warning: NavRoute: Invalid coords, skipping: {star_system} [{x}, {y}, {z}]"
                    )
                    continue

                # Upsert this system
                with conn.cursor() as cur:
                    cur.execute(
                        UPSERT_QUERY,
                        (
                            system_address,
                            star_system,
                            star_class,
                            updatetime,
                            x,
                            y,
                            z,
                        ),
                    )
                conn.commit()
                systems_added += 1
                print(
                    f"NavRoute: {star_system} [{system_address}] | Class: {star_class}"
                )

            print(f"NavRoute completed: {systems_added} systems upserted")

        # --- Handle FSDJump Events ---
        elif event == "FSDJump":
            system_address = msg_data.get("SystemAddress")
            star_system = msg_data.get("StarSystem")
            star_pos = msg_data.get("StarPos")
            timestamp = msg_data.get("timestamp")

            if not all([system_address, star_system, star_pos, timestamp]):
                continue

            if not is_valid_system_name(star_system):
                print(f"Warning: Invalid system name in FSDJump: {star_system}")
                continue

            if len(star_pos) != 3:
                continue

            x, y, z = (
                float(star_pos[0]),
                float(star_pos[1]),
                float(star_pos[2]),
            )
            if not is_valid_coordinates(x, y, z):
                print(
                    f"Warning: FSDJump: Invalid coords, skipping: {star_system} [{x}, {y}, {z}]"
                )
                continue

            try:
                updatetime = datetime.fromisoformat(timestamp.replace("Z", "+00:00"))
            except ValueError:
                print(f"Warning: Invalid timestamp format in FSDJump: {timestamp}")
                continue

            # Upsert the system (mainstar unknown here)
            with conn.cursor() as cur:
                cur.execute(
                    UPSERT_QUERY,
                    (system_address, star_system, None, updatetime, x, y, z),
                )
            conn.commit()
            print(f"FSDJump: {star_system} [{system_address}] | Pos: {x}, {y}, {z}")

    except Exception as e:
        print(f"Error processing message: {e}")
        continue

# === Cleanup (unreachable in infinite loop, but good practice) ===
conn.close()
