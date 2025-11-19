import os
import zmq
import zlib
import json
import psycopg
from datetime import datetime, timezone
from typing import Any, Optional, Callable
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
INACTIVITY_TIMEOUT_SECONDS = int(os.getenv("EDDN_INACTIVITY_TIMEOUT", "900"))

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

def record_systems_processed(cur, amount=1, is_new=False):
    minute_bucket = datetime.now(timezone.utc).replace(second=0, microsecond=0)
    if is_new:
        cur.execute(
            """
            INSERT INTO eddn_systems_metrics (bucket, systems_processed, systems_new)
            VALUES (%s, 0, %s)
            ON CONFLICT (bucket) DO UPDATE
            SET systems_new = eddn_systems_metrics.systems_new + EXCLUDED.systems_new;
        """,
            (minute_bucket, amount),
        )
    else:
        cur.execute(
            """
            INSERT INTO eddn_systems_metrics (bucket, systems_processed, systems_new)
            VALUES (%s, %s, 0)
            ON CONFLICT (bucket) DO UPDATE
            SET systems_processed = eddn_systems_metrics.systems_processed + EXCLUDED.systems_processed;
        """,
            (minute_bucket, amount),
        )

# === UPSERT Query ===
UPSERT_QUERY = """
    INSERT INTO systems_big (id64, name, mainstar, updatetime, coords)
    VALUES (%s, %s, %s, %s, ST_MakePoint(%s, %s, %s)::geometry(PointZ))
    ON CONFLICT (id64) DO UPDATE SET
        name      = COALESCE(EXCLUDED.name, systems_big.name),
        mainstar  = COALESCE(EXCLUDED.mainstar, systems_big.mainstar),
        updatetime= EXCLUDED.updatetime,
        coords    = EXCLUDED.coords
    RETURNING (xmax = 0) AS is_new;
"""

# === ZMQ Setup ===
context = zmq.Context()
socket = context.socket(zmq.SUB)
socket.connect("tcp://eddn.edcd.io:9500")
socket.setsockopt_string(zmq.SUBSCRIBE, "")

print("✅ Listening for EDDN FSDJumps and NavRoutes from trusted clients...")

class StreamStalledError(RuntimeError):
    """Raised when no events are received for an extended period."""

def recv_with_watchdog(sock: zmq.Socket, timeout_seconds: int) -> bytes:
    if timeout_seconds <= 0:
        return sock.recv()
    poller = zmq.Poller()
    poller.register(sock, zmq.POLLIN)
    events = dict(poller.poll(timeout_seconds * 1000))
    if events.get(sock) == zmq.POLLIN:
        return sock.recv()
    raise StreamStalledError(
        f"No EDDN systems events received in {timeout_seconds} seconds"
    )

def _parse_timestamp(raw: Optional[str], context: str) -> Optional[datetime]:
    if not raw:
        print(f"Warning: {context}: Missing timestamp")
        return None
    try:
        return datetime.fromisoformat(raw.replace("Z", "+00:00"))
    except (ValueError, TypeError):
        print(f"Warning: {context}: Invalid timestamp: {raw}")
        return None

def _coords_from_starpos(
    star_pos: Optional[list[Any]],
    star_system: Optional[str],
    *,
    warning_prefix: str,
    warning_suffix: str,
) -> Optional[tuple[float, float, float]]:
    if not star_pos or len(star_pos) != 3:
        return None
    try:
        coords = tuple(float(value) for value in star_pos)
    except (TypeError, ValueError):
        return None

    if not is_valid_coordinates(*coords):
        prefix = f"{warning_prefix}: " if warning_prefix else ""
        print(
            f"Warning: {prefix}{warning_suffix}: {star_system}"
            f" [{coords[0]}, {coords[1]}, {coords[2]}]"
        )
        return None
    return coords

def _upsert_system(
    system_address: int,
    star_system: str,
    star_type: Optional[str],
    updatetime: datetime,
    coords: tuple[float, float, float],
) -> None:
    with conn.cursor() as cur:
        cur.execute(
            UPSERT_QUERY,
            (
                system_address,
                star_system,
                star_type,
                updatetime,
                coords[0],
                coords[1],
                coords[2],
            ),
        )
        result = cur.fetchone()
        is_new = result[0] if result else False
        record_systems_processed(cur, amount=1, is_new=is_new)
    conn.commit()

def _handle_scan_event(msg_data: dict) -> None:
    system_address = msg_data.get("SystemAddress")
    star_system = msg_data.get("StarSystem")
    star_type = msg_data.get("StarType")
    distance_from_arrival = msg_data.get("DistanceFromArrivalLS")
    star_pos = msg_data.get("StarPos")
    timestamp = msg_data.get("timestamp")

    if not all([system_address, star_system, star_pos, timestamp]):
        return

    if not is_valid_system_name(star_system):
        print(f"Warning: Invalid system name, skipping: {star_system}")
        return

    coords = _coords_from_starpos(
        star_pos,
        star_system,
        warning_prefix="",
        warning_suffix="Invalid or suspicious coordinates, skipping",
    )
    if not coords:
        return

    updatetime = _parse_timestamp(timestamp, "Scan event")
    if not updatetime:
        return

    mainstar_type = star_type if distance_from_arrival == 0 else None

    _upsert_system(system_address, star_system, mainstar_type, updatetime, coords)
    print(f"Scan: {star_system} [{system_address}] | Type: {star_type}")

def _handle_navroute_event(msg_data: dict) -> None:
    route = msg_data.get("Route")
    if not isinstance(route, list):
        return

    updatetime = _parse_timestamp(
        msg_data.get("timestamp"), "Invalid timestamp in NavRoute"
    )
    if not updatetime:
        return

    systems_added = 0
    for star in route:
        system_address = star.get("SystemAddress")
        star_system = star.get("StarSystem")
        star_class = star.get("StarClass")
        star_pos = star.get("StarPos")

        if not all([system_address, star_system, star_pos]):
            continue
        if not is_valid_system_name(star_system):
            continue

        coords = _coords_from_starpos(
            star_pos,
            star_system,
            warning_prefix="NavRoute",
            warning_suffix="Invalid coords, skipping",
        )
        if not coords:
            continue

        _upsert_system(system_address, star_system, star_class, updatetime, coords)
        systems_added += 1
        print(f"NavRoute: {star_system} [{system_address}] | Class: {star_class}")

    if systems_added:
        print(f"NavRoute completed: {systems_added} systems upserted")

def _handle_fsdjump_event(msg_data: dict) -> None:
    system_address = msg_data.get("SystemAddress")
    star_system = msg_data.get("StarSystem")
    star_pos = msg_data.get("StarPos")
    timestamp = msg_data.get("timestamp")

    if not all([system_address, star_system, star_pos, timestamp]):
        return

    if not is_valid_system_name(star_system):
        print(f"Warning: Invalid system name in FSDJump: {star_system}")
        return

    coords = _coords_from_starpos(
        star_pos,
        star_system,
        warning_prefix="FSDJump",
        warning_suffix="Invalid coords, skipping",
    )
    if not coords:
        return

    updatetime = _parse_timestamp(timestamp, "FSDJump")
    if not updatetime:
        return

    _upsert_system(system_address, star_system, None, updatetime, coords)
    print(
        f"FSDJump: {star_system} [{system_address}] | Pos: {coords[0]}, {coords[1]}, {coords[2]}"
    )

EVENT_HANDLERS: dict[str, Callable[[dict], None]] = {
    "Scan": _handle_scan_event,
    "NavRoute": _handle_navroute_event,
    "FSDJump": _handle_fsdjump_event,
}

def _process_event(message: dict) -> None:
    header = message.get("header", {})
    msg_data = message.get("message", {})
    event = msg_data.get("event")

    software_name = header.get("softwareName")
    if not is_trusted_source(software_name):
        print(f"Warning: Untrusted source ignored: {software_name}")
        return

    handler = EVENT_HANDLERS.get(event)
    if handler is None:
        return

    if event == "Scan" and "StarType" not in msg_data:
        return

    handler(msg_data)

def stream_events() -> None:
    try:
        while True:
            try:
                compressed = recv_with_watchdog(socket, INACTIVITY_TIMEOUT_SECONDS)
                decompressed = zlib.decompress(compressed)
                message = json.loads(decompressed.decode("utf-8"))
                _process_event(message)
            except Exception as e:
                print(f"Error processing message: {e}")
                continue
    except StreamStalledError as stalled:
        print(f"Watchdog detected stalled EDDN systems feed: {stalled}")
        raise SystemExit(2) from stalled
    except KeyboardInterrupt:
        print("Stopping systems feeder listener...")
    finally:
        conn.close()
        socket.close(0)
        context.term()

if __name__ == "__main__":
    stream_events()
