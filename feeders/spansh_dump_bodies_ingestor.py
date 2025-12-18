import gzip
import ijson
import json
import psycopg
from psycopg import Connection as PGConnection
from psycopg import Cursor as PGCursor
from typing import Sequence
from collections.abc import Callable
from datetime import datetime
import os
import re
import time
from tqdm import tqdm
import decimal
from dotenv import load_dotenv


def json_default(obj):
    if isinstance(obj, decimal.Decimal):
        return float(obj)
    raise TypeError


# === Database Connection ===
load_dotenv()

DB_HOST = os.getenv("DB_HOST")
DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")

RECONNECT_MAX_ATTEMPTS = int(os.getenv("DB_RECONNECT_MAX_ATTEMPTS", "5"))
# Set DB_RECONNECT_MAX_ATTEMPTS=0 to retry indefinitely.
RECONNECT_BACKOFF_SECONDS = float(os.getenv("DB_RECONNECT_BACKOFF_SECONDS", "5"))


def create_connection() -> PGConnection:
    return psycopg.connect(
        host=DB_HOST, port=5432, dbname=DB_NAME, user=DB_USER, password=DB_PASSWORD
    )


conn = create_connection()


def execute_batch(cursor: PGCursor, sql: str, argslist: Sequence[Sequence], page_size: int) -> None:
    """Execute statements in batches using psycopg3's executemany."""
    if not argslist:
        return
    for start in range(0, len(argslist), page_size):
        chunk = argslist[start : start + page_size]
        cursor.executemany(sql, chunk)

# === UPSERT Query for bodies (41 fields) ===
UPSERT_BODY = """
    INSERT INTO bodies (
        system_id64, body_id, body_name, body_type_id, planet_class_id, terraform_state_id,
        atmosphere_type_id, atmosphere_id, volcanism_id, radius, mass_em,
        surface_gravity, surface_temperature, surface_pressure, axial_tilt,
        semi_major_axis, eccentricity, orbital_inclination, periapsis,
        mean_anomaly, orbital_period, rotation_period, ascending_node,
        distance_from_arrival_ls, age_my, absolute_magnitude, luminosity_id,
        star_type_id, subclass, stellar_mass, composition_ice, composition_metal,
        composition_rock, parents, tidally_locked, landable, updatetime,
        ring_class_id, ring_inner_rad, ring_outer_rad, ring_mass_mt
    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
              %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
              %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
              %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
              %s)
    ON CONFLICT (system_id64, body_id) DO UPDATE SET
        body_type_id            = EXCLUDED.body_type_id,
        planet_class_id         = COALESCE(EXCLUDED.planet_class_id, bodies.planet_class_id),
        terraform_state_id      = COALESCE(EXCLUDED.terraform_state_id, bodies.terraform_state_id),
        atmosphere_type_id      = COALESCE(EXCLUDED.atmosphere_type_id, bodies.atmosphere_type_id),
        atmosphere_id           = COALESCE(EXCLUDED.atmosphere_id, bodies.atmosphere_id),
        volcanism_id            = COALESCE(EXCLUDED.volcanism_id, bodies.volcanism_id),
        radius                  = COALESCE(EXCLUDED.radius, bodies.radius),
        mass_em                 = COALESCE(EXCLUDED.mass_em, bodies.mass_em),
        surface_gravity         = COALESCE(EXCLUDED.surface_gravity, bodies.surface_gravity),
        surface_temperature     = COALESCE(EXCLUDED.surface_temperature, bodies.surface_temperature),
        surface_pressure        = COALESCE(EXCLUDED.surface_pressure, bodies.surface_pressure),
        axial_tilt              = COALESCE(EXCLUDED.axial_tilt, bodies.axial_tilt),
        semi_major_axis         = COALESCE(EXCLUDED.semi_major_axis, bodies.semi_major_axis),
        eccentricity            = COALESCE(EXCLUDED.eccentricity, bodies.eccentricity),
        orbital_inclination     = COALESCE(EXCLUDED.orbital_inclination, bodies.orbital_inclination),
        periapsis               = COALESCE(EXCLUDED.periapsis, bodies.periapsis),
        mean_anomaly            = COALESCE(EXCLUDED.mean_anomaly, bodies.mean_anomaly),
        orbital_period          = COALESCE(EXCLUDED.orbital_period, bodies.orbital_period),
        rotation_period         = COALESCE(EXCLUDED.rotation_period, bodies.rotation_period),
        ascending_node          = COALESCE(EXCLUDED.ascending_node, bodies.ascending_node),
        distance_from_arrival_ls= COALESCE(EXCLUDED.distance_from_arrival_ls, bodies.distance_from_arrival_ls),
        age_my                  = COALESCE(EXCLUDED.age_my, bodies.age_my),
        absolute_magnitude      = COALESCE(EXCLUDED.absolute_magnitude, bodies.absolute_magnitude),
        luminosity_id              = COALESCE(EXCLUDED.luminosity_id, bodies.luminosity_id),
        star_type_id               = COALESCE(EXCLUDED.star_type_id, bodies.star_type_id),
        subclass                = COALESCE(EXCLUDED.subclass, bodies.subclass),
        stellar_mass            = COALESCE(EXCLUDED.stellar_mass, bodies.stellar_mass),
        composition_ice         = COALESCE(EXCLUDED.composition_ice, bodies.composition_ice),
        composition_metal       = COALESCE(EXCLUDED.composition_metal, bodies.composition_metal),
        composition_rock        = COALESCE(EXCLUDED.composition_rock, bodies.composition_rock),
        parents                 = CASE
                                    WHEN EXCLUDED.parents IS NOT NULL THEN EXCLUDED.parents
                                    ELSE bodies.parents
                                  END,
        tidally_locked          = EXCLUDED.tidally_locked,
        landable                = EXCLUDED.landable,
        updatetime              = EXCLUDED.updatetime,
        ring_class_id           = EXCLUDED.ring_class_id,
        ring_inner_rad          = EXCLUDED.ring_inner_rad,
        ring_outer_rad          = EXCLUDED.ring_outer_rad,
        ring_mass_mt            = EXCLUDED.ring_mass_mt
    -- Removed RETURNING clause entirely
"""

UPSERT_MATERIAL = """
    INSERT INTO body_materials (system_id64, body_id, material_id, percent)
    VALUES (%s, %s, %s, %s)
    ON CONFLICT (system_id64, body_id, material_id) DO NOTHING;
"""

UPSERT_ATMOSPHERE_GAS = """
    INSERT INTO body_atmospheres (system_id64, body_id, gas_id, percent)
    VALUES (%s, %s, %s, %s)
    ON CONFLICT (system_id64, body_id, gas_id) DO NOTHING;
"""

# === Batch Configuration ===
MATERIAL_BATCH_SIZE = 512
GAS_BATCH_SIZE = 512

# === Lookup Cache ===
lookup_cache = {
    "body_types": {},
    "planet_classes": {},
    "atmosphere_types": {},
    "atmospheres": {},
    "terraform_states": {},
    "volcanisms": {},
    "luminosities": {},
    "star_types": {},
    "ring_classes": {},
    "material_names": {},
    "atmosphere_gases": {},
}

LOOKUP_TABLES = {
    "body_types": "body_types",
    "planet_classes": "planet_classes",
    "atmosphere_types": "atmosphere_types",
    "atmospheres": "atmospheres",
    "terraform_states": "terraform_states",
    "volcanisms": "volcanisms",
    "luminosities": "luminosities",
    "star_types": "star_types",
    "ring_classes": "ring_classes",
    "material_names": "material_names",
    "atmosphere_gases": "atmosphere_gases",
}


def initialize_lookup_cache(connection: PGConnection) -> None:
    """Eagerly load lookup tables so repeated ids never hit the database."""
    for cache_key, table in LOOKUP_TABLES.items():
        cache = lookup_cache[cache_key]
        with connection.cursor() as cur:
            cur.execute(f"SELECT id, name FROM {table};")
            for lookup_id, name in cur.fetchall() or []:
                cache[name] = lookup_id


initialize_lookup_cache(conn)


lookup_cursors: dict[str, PGCursor] = {}


def get_lookup_cursor(table: str, connection: PGConnection) -> PGCursor:
    cursor = lookup_cursors.get(table)
    if cursor is None or cursor.closed:
        cursor = connection.cursor()
        lookup_cursors[table] = cursor
    return cursor



def parse_timestamp(ts_str):
    if not ts_str:
        return None
    try:
        return datetime.fromisoformat(ts_str.replace("Z", "+00:00"))
    except Exception:
        return None


TOKEN_CANONICAL_MAP = {
    "O": "O",
    "B": "B",
    "A": "A",
    "F": "F",
    "G": "G",
    "K": "K",
    "M": "M",
    "L": "L",
    "T": "T",
    "Y": "Y",
    "N": "N",
    "H": "H",
    "AE": "AeBe",
    "AEBE": "AeBe",
    "TTS": "TTS",
    "MS": "MS",
    "CH": "CH",
    "CHD": "CHd",
    "CJ": "CJ",
    "CN": "CN",
    "CS": "CS",
    "WC": "WC",
    "WN": "WN",
    "WNC": "WNC",
    "WO": "WO",
    "W": "W",
    "C": "C",
    "S": "S",
    "DB": "DB",
    "DBV": "DBV",
    "DBZ": "DBZ",
    "DA": "DA",
    "DAB": "DAB",
    "DAO": "DAO",
    "DAV": "DAV",
    "DAZ": "DAZ",
    "DC": "DC",
    "DCV": "DCV",
    "DQ": "DQ",
    "DO": "DO",
    "DOV": "DOV",
    "DX": "D",
    "BH": "H",
    "SMBH": "SupermassiveBlackHole",
    "SUPERMASSIVEBLACKHOLE": "SupermassiveBlackHole",
    "A_BLUEWHITESUPERGIANT": "A_BlueWhiteSuperGiant",
    "B_BLUEWHITESUPERGIANT": "B_BlueWhiteSuperGiant",
    "F_WHITESUPERGIANT": "F_WhiteSuperGiant",
    "G_WHITESUPERGIANT": "G_WhiteSuperGiant",
    "K_ORANGEGIANT": "K_OrangeGiant",
    "M_REDGIANT": "M_RedGiant",
    "M_REDSUPERGIANT": "M_RedSuperGiant",
    "X": "X",
}

WHITE_DWARF_TYPES = {
    "D": "D",
    "DA": "DA",
    "DAB": "DAB",
    "DAO": "DAO",
    "DAZ": "DAZ",
    "DAV": "DAV",
    "DB": "DB",
    "DBV": "DBV",
    "DBZ": "DBZ",
    "DC": "DC",
    "DCV": "DCV",
    "DO": "DO",
    "DOV": "DOV",
    "DQ": "DQ",
    "DX": "D",
}


def _map_star_type_value(raw_value: str | None) -> str | None:
    if not raw_value or not isinstance(raw_value, str):
        return None

    candidate = raw_value.strip()
    if not candidate:
        return None

    if candidate in TOKEN_CANONICAL_MAP.values():
        return candidate

    upper_candidate = candidate.replace("/", "").upper()
    if upper_candidate in TOKEN_CANONICAL_MAP:
        return TOKEN_CANONICAL_MAP[upper_candidate]

    lower_candidate = candidate.lower()

    if "neutron" in lower_candidate:
        return "N"

    if "black hole" in lower_candidate:
        return "SupermassiveBlackHole" if "supermassive" in lower_candidate else "H"

    if "herbig" in lower_candidate or "ae" in lower_candidate:
        return "AeBe"

    if "t tauri" in lower_candidate:
        return "TTS"

    if "ms-type" in lower_candidate or "ms type" in lower_candidate:
        return "MS"

    if "wolf-rayet" in lower_candidate or "wolf rayet" in lower_candidate:
        if "nc" in lower_candidate:
            return "WNC"
        if "wn" in lower_candidate:
            return "WN"
        if "wc" in lower_candidate:
            return "WC"
        if "wo" in lower_candidate:
            return "WO"
        return "W"

    if "white dwarf" in lower_candidate:
        subtype_match = re.search(r"\(([^)]+)\)", candidate)
        if subtype_match:
            subtype = subtype_match.group(1).replace("/", "").upper()
            return WHITE_DWARF_TYPES.get(subtype, "D")
        return "D"

    if "brown dwarf" in lower_candidate and candidate:
        first_letter = candidate[0].upper()
        if first_letter in {"L", "T", "Y"}:
            return first_letter

    if "giant" in lower_candidate and candidate:
        first_letter = candidate[0].upper()
        if "super" in lower_candidate:
            if first_letter == "A":
                return "A_BlueWhiteSuperGiant"
            if first_letter == "B":
                return "B_BlueWhiteSuperGiant"
            if first_letter == "F":
                return "F_WhiteSuperGiant"
            if first_letter == "G":
                return "G_WhiteSuperGiant"
            if first_letter == "M":
                return "M_RedSuperGiant"
        else:
            if first_letter == "K":
                return "K_OrangeGiant"
            if first_letter == "M":
                return "M_RedGiant"

    match = re.match(r"([A-Za-z]+)", candidate)
    if match:
        token = match.group(1).replace("/", "")
        upper_token = token.upper()
        if upper_token in TOKEN_CANONICAL_MAP:
            return TOKEN_CANONICAL_MAP[upper_token]

    return None


def resolve_star_type(body: dict) -> str | None:
    if not isinstance(body, dict):
        return None

    for key in ("subType", "spectralClass", "starType"):
        star_type = _map_star_type_value(body.get(key))
        if star_type:
            return star_type

    return None


def to_seconds(value, multiplier=86400, precision=6):
    """
    Convert value to float then multiply by multiplier, or return None.
    Handles int, float, Decimal, numeric strings. Returns None for None/invalid.
    Optionally rounds to a given precision (default 6 decimal places).
    """
    if value is None:
        return None
    try:
        return round(float(value) * multiplier, precision)
    except (TypeError, ValueError):
        return None


def parse_subclass(spectral):
    if not spectral or not isinstance(spectral, str):
        return None
    m = re.match(r"[A-Za-z]+(\d+)", spectral)
    return int(m.group(1)) if m else None


def count_lines(path):
    with gzip.open(path, "rt", encoding="utf-8") as f:
        return sum(1 for _ in f)


def convert_atmosphere_type(
    description: str, subType: str, name
) -> str | None:
    if subType and "earth-like world" in subType.strip().lower():
        return "EarthLike"
    if not description:  # handles None, empty string, etc.
        return None

    # normalize input
    desc = (
        description.lower()
        .replace("-rich", " rich")
        .replace("atmosphere", "")
        .strip()
    )
    desc = re.sub(r"\bsulfur\b", "sulphur", desc)
    # remove qualifiers
    qualifiers = {"hot", "cold", "thin", "thick"}
    parts = [word for word in desc.split() if word not in qualifiers]

    # detect if "rich" is present
    is_rich = "rich" in parts
    if is_rich:
        parts.remove("rich")

    # join the remaining words into the main component
    component = " ".join(parts)

    # capitalize properly (CarbonDioxide, SulphurDioxide, SilicateVapour, etc.)
    component_camel = "".join(word.capitalize() for word in component.split())

    # append Rich if necessary
    if is_rich:
        component_camel += "Rich"

    return component_camel or None


class SpanshBodyIngestSession:
    """Shared ingestion helpers for Spansh body data."""

    def __init__(
        self,
        connection: PGConnection | None = None,
        log_func: Callable[[str], None] | None = None,
        verbose: bool = False,
    ) -> None:
        self.conn = connection or conn
        self.log = log_func or print
        self.verbose = verbose
        self.body_cursor = self.conn.cursor()
        self.material_cursor = self.conn.cursor()
        self.gas_cursor = self.conn.cursor()
        self.material_batch: list[tuple[int, int, int, float]] = []
        self.gas_batch: list[tuple[int, int, int, float]] = []

    def _safe_close_cursor(self, cursor: PGCursor | None) -> None:
        if not cursor:
            return
        try:
            if not cursor.closed:
                cursor.close()
        except Exception:
            return

    def _safe_rollback(self) -> None:
        try:
            self.conn.rollback()
        except Exception:
            return

    def _reconnect(self, reason: str, attempt: int) -> None:
        if RECONNECT_MAX_ATTEMPTS <= 0:
            attempt_label = f"attempt {attempt}/unlimited"
        else:
            attempt_label = f"attempt {attempt}/{RECONNECT_MAX_ATTEMPTS}"
        self._log(f"{reason} ({attempt_label}); reconnecting...")
        self._safe_close_cursor(self.body_cursor)
        self._safe_close_cursor(self.material_cursor)
        self._safe_close_cursor(self.gas_cursor)
        for cursor in lookup_cursors.values():
            self._safe_close_cursor(cursor)
        lookup_cursors.clear()
        try:
            self.conn.close()
        except Exception:
            pass
        time.sleep(RECONNECT_BACKOFF_SECONDS * attempt)
        self.conn = create_connection()
        # Refresh lookup caches after reconnect to avoid stale IDs.
        for cache in lookup_cache.values():
            cache.clear()
        initialize_lookup_cache(self.conn)
        self.body_cursor = self.conn.cursor()
        self.material_cursor = self.conn.cursor()
        self.gas_cursor = self.conn.cursor()

    def _run_with_retry(self, action: Callable[[], object], label: str) -> object:
        last_exc: Exception | None = None
        attempt = 0
        while RECONNECT_MAX_ATTEMPTS <= 0 or attempt < RECONNECT_MAX_ATTEMPTS:
            attempt += 1
            try:
                return action()
            except psycopg.OperationalError as exc:
                last_exc = exc
                self._safe_rollback()
                self._reconnect(f"{label} failed: {exc}", attempt)
        if last_exc:
            raise last_exc
        return None

    def get_lookup_id(self, table: str, name: str | None) -> int | None:
        """Get the ID for a lookup value, inserting it if needed."""
        if not name:
            return None

        # Use cache first
        cache = lookup_cache.get(table, {})
        if name in cache:
            return cache[name]

        def _fetch_lookup_id() -> int:
            cur = get_lookup_cursor(table, self.conn)
            cur.execute(f"SELECT id FROM {table} WHERE name = %s;", (name,))
            row = cur.fetchone()
            if row:
                cache[name] = row[0]
                return row[0]

            cur.execute(
                f"INSERT INTO {table} (name) VALUES (%s) RETURNING id;", (name,)
            )
            new_id = cur.fetchone()[0]
            cache[name] = new_id
            return new_id

        return self._run_with_retry(_fetch_lookup_id, f"Lookup {table}")  # type: ignore[return-value]

    def flush_batches(self) -> None:
        if self.material_batch:
            def _flush_materials() -> None:
                execute_batch(
                    self.material_cursor,
                    UPSERT_MATERIAL,
                    self.material_batch,
                    page_size=min(len(self.material_batch), MATERIAL_BATCH_SIZE),
                )

            self._run_with_retry(_flush_materials, "Flush materials")
            self.material_batch.clear()
        if self.gas_batch:
            def _flush_gases() -> None:
                execute_batch(
                    self.gas_cursor,
                    UPSERT_ATMOSPHERE_GAS,
                    self.gas_batch,
                    page_size=min(len(self.gas_batch), GAS_BATCH_SIZE),
                )

            self._run_with_retry(_flush_gases, "Flush gases")
            self.gas_batch.clear()

    def reset_cursors(self) -> None:
        self._safe_close_cursor(self.body_cursor)
        self._safe_close_cursor(self.material_cursor)
        self._safe_close_cursor(self.gas_cursor)
        for cursor in lookup_cursors.values():
            self._safe_close_cursor(cursor)
        lookup_cursors.clear()
        self.material_batch.clear()
        self.gas_batch.clear()
        for cache in lookup_cache.values():
            cache.clear()
        initialize_lookup_cache(self.conn)
        self.body_cursor = self.conn.cursor()
        self.material_cursor = self.conn.cursor()
        self.gas_cursor = self.conn.cursor()

    def close(self) -> None:
        self._safe_close_cursor(self.body_cursor)
        self._safe_close_cursor(self.material_cursor)
        self._safe_close_cursor(self.gas_cursor)
        for cursor in lookup_cursors.values():
            self._safe_close_cursor(cursor)
        lookup_cursors.clear()
        try:
            self.conn.close()
        except Exception:
            return

    def _log(self, message: str) -> None:
        if self.log:
            self.log(message)
        else:
            print(message)

    def process_body(
        self,
        body: dict,
        sys_id: int,
        updatetime: datetime | None,
    ) -> None:
        star_type_name = (
            resolve_star_type(body) if body.get("type") == "Star" else None
        )
        if body.get("type") == "Planet":
            r = body.get("radius")
            radius = r * 1000 if r is not None else None
        elif body.get("type") == "Star":
            sr = body.get("solarRadius")
            radius = round(sr * 695500000) if sr is not None else None
        else:
            radius = None
        sg = body.get("gravity")
        gravity = round(float(sg) * 9.807, 6) if sg is not None else None
        sma = body.get("semiMajorAxis")
        semi_major_axis = (
            round(sma * 149597870700, 6) if sma is not None else None
        )
        sp = body.get("surfacePressure")
        pressure = round(sp * 101325, 6) if sp is not None else None
        composition_ice = body.get("solidComposition", {}).get("Ice")
        composition_metal = body.get("solidComposition", {}).get("Metal")
        composition_rock = body.get("solidComposition", {}).get("Rock")

        def _normalize_fraction(value):
            if value is None:
                return None
            try:
                return round(float(value) / 100.0, 6)
            except (TypeError, ValueError):
                return None

        row = {
            "system_id64": sys_id,
            "body_id": body.get("bodyId"),
            "body_name": (
                f"Barycenter{body.get('bodyId')}"
                if body.get("type") == "Barycentre"
                else body.get("name")
            ),
            "body_type_id": self.get_lookup_id("body_types", "Barycenter")
            if body.get("type") == "Barycentre"
            else self.get_lookup_id("body_types", body.get("type")),
            "planet_class_id": (
                self.get_lookup_id("planet_classes", "Earthlike body")
                if body.get("type") == "Planet"
                and body.get("subType") == "Earth-like world"
                else (
                    self.get_lookup_id(
                        "planet_classes",
                        body.get("subType").replace(
                            "ammonia-based", "ammonia based"
                        )
                        if body.get("subType")
                        else None,
                    )
                    if body.get("type") == "Planet"
                    else None
                )
            ),
            "terraform_state_id": self.get_lookup_id(
                "terraform_states", body.get("terraformingState")
            ),
            "atmosphere_type_id": self.get_lookup_id(
                "atmosphere_types",
                convert_atmosphere_type(
                    body.get("atmosphereType"),
                    body.get("subType"),
                    body.get("name"),
                ),
            )
            if body.get("type") == "Planet"
            else None,
            "atmosphere_id": self.get_lookup_id(
                "atmospheres",
                body.get("atmosphereType")
                .lower()
                .replace("sulphur", "sulfur")
                .replace("-rich", " rich")
                .replace("no atmosphere", "no")
                + " atmosphere",
            )
            if body.get("atmosphereType")
            else None,
            "volcanism_id": (
                self.get_lookup_id(
                    "volcanisms",
                    (body.get("volcanismType")).lower() + " volcanism",
                )
                if body.get("volcanismType")
                else None
            ),
            "radius": radius,
            "mass_em": body.get("earthMasses"),
            "surface_gravity": gravity,
            "surface_temperature": body.get("surfaceTemperature"),
            "surface_pressure": pressure,
            "axial_tilt": body.get("axialTilt"),
            "semi_major_axis": semi_major_axis,
            "eccentricity": body.get("orbitalEccentricity"),
            "orbital_inclination": body.get("orbitalInclination"),
            "periapsis": body.get("argOfPeriapsis"),
            "mean_anomaly": body.get("meanAnomaly"),
            "orbital_period": to_seconds(body.get("orbitalPeriod")),
            "rotation_period": to_seconds(body.get("rotationalPeriod")),
            "ascending_node": body.get("ascendingNode"),
            "distance_from_arrival_ls": body.get("distanceToArrival"),
            "age_my": body.get("age"),
            "absolute_magnitude": body.get("absoluteMagnitude"),
            "luminosity_id": self.get_lookup_id(
                "luminosities", body.get("luminosity")
            ),
            "star_type_id": self.get_lookup_id(
                "star_types", star_type_name
            )
            if star_type_name
            else None,
            "subclass": parse_subclass(body.get("spectralClass"))
            if body.get("type") == "Star"
            else None,
            "stellar_mass": body.get("solarMasses"),
            "composition_ice": _normalize_fraction(composition_ice),
            "composition_metal": _normalize_fraction(composition_metal),
            "composition_rock": _normalize_fraction(composition_rock),
            "parents": json.dumps(body.get("parents"))
            if body.get("parents")
            else None,
            "tidally_locked": body.get("rotationalPeriodTidallyLocked"),
            "landable": body.get("isLandable"),
            "updatetime": parse_timestamp(body.get("updateTime")) or updatetime,
            "ring_class_id": None,
            "ring_inner_rad": None,
            "ring_outer_rad": None,
            "ring_mass_mt": None,
        }

        try:
            self._run_with_retry(
                lambda: self.body_cursor.execute(UPSERT_BODY, list(row.values())),
                "UPSERT_BODY",
            )
        except Exception as exc:
            bodyname = body.get("name")
            self._log(f"Error processing body {bodyname}: {exc}")
            self.conn.rollback()
            self.reset_cursors()
            return
        if self.verbose:
            self._log(f"UPSERT_BODY payload for {body.get('name')} -> {row}")

        materials = body.get("materials", {})
        if materials:
            for name, percent in materials.items():
                try:
                    mat_id = self.get_lookup_id("material_names", name.lower())
                except Exception as exc:
                    self._log(
                        f"Error inserting material {name.lower()} for body {body.get('name')}: {exc}"
                    )
                    self.conn.rollback()
                    self.reset_cursors()
                    continue

                self.material_batch.append(
                    (sys_id, body.get("bodyId"), mat_id, float(percent))
                )
                if len(self.material_batch) >= MATERIAL_BATCH_SIZE:
                    self.flush_batches()

        atmosphere_composition = body.get("atmosphereComposition", {})
        if atmosphere_composition:
            for raw_name, percent in sorted(
                atmosphere_composition.items(), key=lambda kv: kv[1], reverse=True
            ):
                try:
                    formatted_name = "".join(
                        word.capitalize() for word in raw_name.split()
                    )
                    gas_id = self.get_lookup_id("atmosphere_gases", formatted_name)
                except Exception as exc:
                    self._log(
                        f"Error inserting gas {raw_name} for body {body.get('name')}: {exc}"
                    )
                    self.conn.rollback()
                    self.reset_cursors()
                    continue

                self.gas_batch.append(
                    (sys_id, body.get("bodyId"), gas_id, float(percent))
                )
                if len(self.gas_batch) >= GAS_BATCH_SIZE:
                    self.flush_batches()

        for i, ring in enumerate(body.get("rings", []), start=1):
            ring_row = {
                "system_id64": sys_id,
                "body_id": body.get("bodyId") + i,
                "body_name": ring.get("name"),
                "body_type_id": self.get_lookup_id("body_types", "PlanetaryRing"),
                "planet_class_id": None,
                "terraform_state_id": None,
                "atmosphere_type_id": None,
                "atmosphere_id": None,
                "volcanism_id": None,
                "radius": None,
                "mass_em": None,
                "surface_gravity": None,
                "surface_temperature": None,
                "surface_pressure": None,
                "axial_tilt": None,
                "semi_major_axis": None,
                "eccentricity": None,
                "orbital_inclination": None,
                "periapsis": None,
                "mean_anomaly": None,
                "orbital_period": None,
                "rotation_period": None,
                "ascending_node": None,
                "distance_from_arrival_ls": body.get("distanceToArrival"),
                "age_my": body.get("age"),
                "absolute_magnitude": None,
                "luminosity_id": None,
                "star_type_id": None,
                "subclass": None,
                "stellar_mass": None,
                "composition_ice": None,
                "composition_metal": None,
                "composition_rock": None,
                "parents": json.dumps([{"parent_id": body.get("bodyId")}]),
                "tidally_locked": None,
                "landable": None,
                "updatetime": parse_timestamp(ring.get("updateTime"))
                or updatetime,
                "ring_class_id": self.get_lookup_id(
                    "ring_classes",
                    "eRingClass_" + ring.get("type", "").replace(" ", ""),
                )
                if ring.get("type")
                else None,
                "ring_inner_rad": ring.get("innerRadius"),
                "ring_outer_rad": ring.get("outerRadius"),
                "ring_mass_mt": ring.get("mass"),
            }
            try:
                self._run_with_retry(
                    lambda: self.body_cursor.execute(
                        UPSERT_BODY, list(ring_row.values())
                    ),
                    "UPSERT_BODY ring",
                )
            except Exception as exc:
                self._log(
                    f"Error processing ring {ring.get('name')}: {exc}"
                )
                self.conn.rollback()
                self.reset_cursors()

def ingest_streaming(path):
    total_bytes = os.path.getsize(path)
    count = 0

    session = SpanshBodyIngestSession(conn, log_func=tqdm.write)

    with gzip.open(path, "rb") as f:
        with tqdm(
            total=total_bytes, unit="B", unit_scale=True, desc="Ingesting"
        ) as pbar:
            for system in ijson.items(f, "item"):
                sys_id = system.get("id64")
                updatetime = parse_timestamp(system.get("date"))

                for body in system.get("bodies", []):
                    session.process_body(body, sys_id, updatetime)
                    count += 1
                    if count % 10000 == 0:
                        session.flush_batches()
                        session._run_with_retry(
                            lambda: session.conn.commit(), "Commit"
                        )

                # update progress bar based on file position
                pbar.update(f.tell() - pbar.n)

    session.flush_batches()
    session._run_with_retry(lambda: session.conn.commit(), "Commit")
    session.close()
    print(f"Done. Inserted/updated {count} bodies.")


if __name__ == "__main__":
    ingest_streaming("dump.json.gz")
    try:
        conn.close()
    except Exception:
        pass
