import os
import zmq
import zlib
import json
import psycopg
from datetime import datetime
import re
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

# === Coordinate Bounds (for validation) ===
MAX_XYZ = 70000  # Light years
MAX_Y = 35000


def is_valid_coordinates(x, y, z):
    """Check if coordinates are within plausible bounds."""
    if not (-MAX_XYZ <= x <= MAX_XYZ):
        return False
    if not (-MAX_Y <= y <= MAX_Y):
        return False
    if not (-MAX_XYZ <= z <= MAX_XYZ):
        return False
    # Allow (0,0,0) = Sol, reject suspicious near-zero non-Sol
    if abs(x) <= 1 and abs(y) <= 1 and abs(z) <= 1:
        return x == 0 and y == 0 and z == 0
    return True


def is_valid_system_name(name):
    """Reject placeholder or invalid system names."""
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
        "NULL",
    }
    return name not in bad_names


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

# === UPSERT Query for bodies (42 fields) ===
UPSERT_BODY = """
    INSERT INTO bodies (
        system_id64, body_id, body_name, body_type_id, planet_class_id, terraform_state_id,
        atmosphere_type_id, atmosphere_composition, atmosphere_id, volcanism_id, radius, mass_em,
        surface_gravity, surface_temperature, surface_pressure, axial_tilt,
        semi_major_axis, eccentricity, orbital_inclination, periapsis,
        mean_anomaly, orbital_period, rotation_period, ascending_node,
        distance_from_arrival_ls, age_my, absolute_magnitude, luminosity_id,
        star_type_id, subclass, stellar_mass, composition_ice, composition_metal,
        composition_rock, materials, parents, tidally_locked, landable, updatetime,
        ring_class_id, ring_inner_rad, ring_outer_rad, ring_mass_mt
    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
              %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
              %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
              %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
              %s, %s, %s)
    ON CONFLICT (system_id64, body_id) DO UPDATE SET
        body_type_id            = EXCLUDED.body_type_id,
        planet_class_id         = COALESCE(EXCLUDED.planet_class_id, bodies.planet_class_id),
        terraform_state_id      = EXCLUDED.terraform_state_id,
        atmosphere_type_id      = COALESCE(EXCLUDED.atmosphere_type_id, bodies.atmosphere_type_id),
        atmosphere_composition  = COALESCE(EXCLUDED.atmosphere_composition, bodies.atmosphere_composition),
        atmosphere_id           = COALESCE(EXCLUDED.atmosphere_id, bodies.atmosphere_id),
        volcanism_id            = EXCLUDED.volcanism_id,
        radius                  = EXCLUDED.radius,
        mass_em                 = EXCLUDED.mass_em,
        surface_gravity         = EXCLUDED.surface_gravity,
        surface_temperature     = EXCLUDED.surface_temperature,
        surface_pressure        = EXCLUDED.surface_pressure,
        axial_tilt              = EXCLUDED.axial_tilt,
        semi_major_axis         = COALESCE(EXCLUDED.semi_major_axis, bodies.semi_major_axis),
        eccentricity            = COALESCE(EXCLUDED.eccentricity, bodies.eccentricity),
        orbital_inclination     = COALESCE(EXCLUDED.orbital_inclination, bodies.orbital_inclination),
        periapsis               = COALESCE(EXCLUDED.periapsis, bodies.periapsis),
        mean_anomaly            = COALESCE(EXCLUDED.mean_anomaly, bodies.mean_anomaly),
        orbital_period          = COALESCE(EXCLUDED.orbital_period, bodies.orbital_period),
        rotation_period         = EXCLUDED.rotation_period,
        ascending_node          = COALESCE(EXCLUDED.ascending_node, bodies.ascending_node),
        distance_from_arrival_ls= COALESCE(EXCLUDED.distance_from_arrival_ls, bodies.distance_from_arrival_ls),
        age_my                  = EXCLUDED.age_my,
        absolute_magnitude      = EXCLUDED.absolute_magnitude,
        luminosity_id           = EXCLUDED.luminosity_id,
        star_type_id            = EXCLUDED.star_type_id,
        subclass                = EXCLUDED.subclass,
        stellar_mass            = EXCLUDED.stellar_mass,
        composition_ice         = COALESCE(EXCLUDED.composition_ice, bodies.composition_ice),
        composition_metal       = COALESCE(EXCLUDED.composition_metal, bodies.composition_metal),
        composition_rock        = COALESCE(EXCLUDED.composition_rock, bodies.composition_rock),
        materials               = COALESCE(EXCLUDED.materials, bodies.materials),
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

# === ZMQ Setup ===
context = zmq.Context()
socket = context.socket(zmq.SUB)
socket.connect("tcp://eddn.edcd.io:9500")
socket.setsockopt_string(zmq.SUBSCRIBE, "")

print("Listening for EDDN body events from trusted clients...")

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
}


def get_lookup_id(table, name, conn):
    """Get the ID for a lookup value, inserting it if needed."""
    if not name:
        return None

    # Use cache first
    cache = lookup_cache.get(table, {})
    if name in cache:
        return cache[name]

    with conn.cursor() as cur:
        # Try to fetch existing
        cur.execute(f"SELECT id FROM {table} WHERE name = %s;", (name,))
        row = cur.fetchone()
        if row:
            cache[name] = row[0]
            return row[0]

        # Insert new if not found
        cur.execute(f"INSERT INTO {table} (name) VALUES (%s) RETURNING id;", (name,))
        new_id = cur.fetchone()[0]
        conn.commit()
        cache[name] = new_id
        return new_id


# === Helper: Parse timestamp ===
def parse_timestamp(ts_str):
    try:
        return datetime.fromisoformat(ts_str.replace("Z", "+00:00"))
    except (ValueError, TypeError):
        return None


# === Infer canonical body type ===
def infer_body_type(msg_data, body_name):
    event = msg_data.get("event")
    if event == "ScanBaryCentre":
        return "Barycenter"
    if "Belt Cluster" in body_name:
        return "AsteroidCluster"
    if body_name.endswith("Belt"):
        return "StellarRing"
    if re.match(r".+ [A-Z] Ring$", body_name):
        return "PlanetaryRing"
    if msg_data.get("StarType"):
        return "Star"
    if msg_data.get("PlanetClass"):
        return "Planet"
    return "Unknown"


# === Main Loop ===
while True:
    try:
        # Receive and decompress message
        compressed = socket.recv()
        decompressed = zlib.decompress(compressed)
        message = json.loads(decompressed.decode("utf-8"))

        header = message.get("header", {})
        msg_data = message.get("message", {})
        event = msg_data.get("event")

        # --- Skip irrelevant events ---
        if event not in {"Scan", "Location", "ScanBaryCentre"}:
            continue

        # --- SECURITY FILTERS ---
        software_name = header.get("softwareName")
        if not is_trusted_source(software_name):
            print(f"/!\ Untrusted source ignored: {software_name}")
            continue

        system_address = msg_data.get("SystemAddress")
        body_id = msg_data.get("BodyID")
        if body_id is None:
            continue

        # --- Skip if system or body ID invalid ---
        if not system_address:
            continue

        # Validate StarSystem name
        star_system = msg_data.get("StarSystem")
        if not is_valid_system_name(star_system):
            print(f"/!\ Invalid system name, skipping: {star_system}")
            continue

        # Parse timestamp
        timestamp_str = msg_data.get("timestamp")
        updatetime = parse_timestamp(timestamp_str)
        if not updatetime:
            print(f"/!\ Invalid timestamp, skipping event: {event}")
            continue

        # === Initialize common fields ===
        body = {}
        body["system_id64"] = system_address
        body["body_id"] = body_id
        body["updatetime"] = updatetime
        body["ring_class_id"] = None
        body["ring_inner_rad"] = None
        body["ring_outer_rad"] = None
        body["ring_mass_mt"] = None

        # --- Normalize body name ---
        body["body_name"] = msg_data.get("BodyName") or msg_data.get("Body")
        if event == "ScanBaryCentre":
            body["body_name"] = f"Barycenter{msg_data.get('BodyID')}"
        if not body["body_name"]:
            print(f"/!\ Missing body name in event: {event}, BodyID: {body_id}")
            continue

        if msg_data.get("BodyType") == "Station":
            continue
        body["type"] = infer_body_type(msg_data, body["body_name"])
        if body["type"] == "Unknown":
            continue
        body["body_type_id"] = get_lookup_id("body_types", body["type"], conn)
        # --- Handle Scan (Planets, Stars, Belts) ---
        if event == "Scan":
            # Planet or Star
            body["planet_class_id"] = get_lookup_id(
                "planet_classes", msg_data.get("PlanetClass"), conn
            )
            body["terraform_state_id"] = get_lookup_id(
                "terraform_states", msg_data.get("TerraformState"), conn
            )
            body["atmosphere_type_id"] = get_lookup_id(
                "atmosphere_types", msg_data.get("AtmosphereType"), conn
            )
            body["atmosphere_composition"] = msg_data.get("AtmosphereComposition")
            body["atmosphere_id"] = get_lookup_id(
                "atmospheres", msg_data.get("Atmosphere"), conn
            )
            body["volcanism_id"] = get_lookup_id(
                "volcanisms", msg_data.get("Volcanism"), conn
            )
            body["radius"] = msg_data.get("Radius")
            body["mass_em"] = msg_data.get("MassEM")
            body["surface_gravity"] = msg_data.get("SurfaceGravity")
            body["surface_temperature"] = msg_data.get("SurfaceTemperature")
            body["surface_pressure"] = msg_data.get("SurfacePressure")
            body["axial_tilt"] = msg_data.get("AxialTilt")
            body["semi_major_axis"] = msg_data.get("SemiMajorAxis")
            body["eccentricity"] = msg_data.get("Eccentricity")
            body["orbital_inclination"] = msg_data.get("OrbitalInclination")
            body["periapsis"] = msg_data.get("Periapsis")
            body["mean_anomaly"] = msg_data.get("MeanAnomaly")
            body["orbital_period"] = msg_data.get("OrbitalPeriod")
            body["rotation_period"] = msg_data.get("RotationPeriod")
            body["ascending_node"] = msg_data.get("AscendingNode")
            body["distance_from_arrival_ls"] = msg_data.get("DistanceFromArrivalLS")
            body["tidally_locked"] = msg_data.get("TidalLock")
            body["landable"] = msg_data.get("Landable")

            # Composition
            comp = msg_data.get("Composition", {})
            body["composition_ice"] = comp.get("Ice")
            body["composition_metal"] = comp.get("Metal")
            body["composition_rock"] = comp.get("Rock")

            body["materials"] = msg_data.get("Materials")
            body["parents"] = msg_data.get("Parents", [])

            # Star-specific
            if body["type"] == "Star":
                body["age_my"] = msg_data.get("Age_MY")
                body["absolute_magnitude"] = msg_data.get("AbsoluteMagnitude")
                body["luminosity_id"] = get_lookup_id(
                    "luminosities", msg_data.get("Luminosity"), conn
                )
                body["star_type_id"] = get_lookup_id(
                    "star_types", msg_data.get("StarType"), conn
                )
                body["subclass"] = msg_data.get("Subclass")
                body["stellar_mass"] = msg_data.get("StellarMass")

            # --- Auto-create StellarRing from Belt Cluster ---
            match = re.match(r"^(.+) Belt Cluster \d+$", body["body_name"])
            if match:
                base_name = match.group(1)
                # get parent_body_id from first parent (Ring)
                parent_body_id = None
                if body.get("parents"):
                    first_parent = body["parents"][0]
                    if "Ring" in first_parent:
                        parent_body_id = first_parent["Ring"]

                if parent_body_id is not None:
                    # make a copy of parents list without { "Ring": id }
                    new_parents = [p for p in body["parents"] if "Ring" not in p]

                    ring_body = {
                        "system_id64": system_address,
                        "body_id": parent_body_id,
                        "body_name": f"{base_name} Belt",
                        "body_type_id": get_lookup_id(
                            "body_types", "StellarRing", conn
                        ),
                        "planet_class_id": None,
                        "terraform_state_id": None,
                        "atmosphere_type_id": None,
                        "atmosphere_composition": None,
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
                        "distance_from_arrival_ls": body["distance_from_arrival_ls"],
                        "age_my": None,
                        "absolute_magnitude": None,
                        "luminosity_id": None,
                        "star_type_id": None,
                        "subclass": None,
                        "stellar_mass": None,
                        "composition_ice": None,
                        "composition_metal": None,
                        "composition_rock": None,
                        "materials": None,
                        "parents": new_parents if new_parents else None,
                        "tidally_locked": None,
                        "landable": None,
                        "updatetime": updatetime,
                        "ring_class_id": None,
                        "ring_inner_rad": None,
                        "ring_outer_rad": None,
                        "ring_mass_mt": None,
                    }

                    with conn.cursor() as cur:
                        cur.execute(
                            UPSERT_BODY,
                            [
                                ring_body["system_id64"],
                                ring_body["body_id"],
                                ring_body["body_name"],
                                ring_body["body_type_id"],
                                ring_body["planet_class_id"],
                                ring_body["terraform_state_id"],
                                ring_body["atmosphere_type_id"],
                                ring_body["atmosphere_composition"],
                                ring_body["atmosphere_id"],
                                ring_body["volcanism_id"],
                                ring_body["radius"],
                                ring_body["mass_em"],
                                ring_body["surface_gravity"],
                                ring_body["surface_temperature"],
                                ring_body["surface_pressure"],
                                ring_body["axial_tilt"],
                                ring_body["semi_major_axis"],
                                ring_body["eccentricity"],
                                ring_body["orbital_inclination"],
                                ring_body["periapsis"],
                                ring_body["mean_anomaly"],
                                ring_body["orbital_period"],
                                ring_body["rotation_period"],
                                ring_body["ascending_node"],
                                ring_body["distance_from_arrival_ls"],
                                ring_body["age_my"],
                                ring_body["absolute_magnitude"],
                                ring_body["luminosity_id"],
                                ring_body["star_type_id"],
                                ring_body["subclass"],
                                ring_body["stellar_mass"],
                                ring_body["composition_ice"],
                                ring_body["composition_metal"],
                                ring_body["composition_rock"],
                                ring_body["materials"],
                                json.dumps(ring_body["parents"])
                                if ring_body["parents"]
                                else None,
                                ring_body["tidally_locked"],
                                ring_body["landable"],
                                ring_body["updatetime"],
                                ring_body["ring_class_id"],
                                ring_body["ring_inner_rad"],
                                ring_body["ring_outer_rad"],
                                ring_body["ring_mass_mt"],
                            ],
                        )
                    conn.commit()
                    print(
                        f"((O)) Inferred StellarRing: {ring_body['body_name']} | ID: {parent_body_id}"
                    )

            # --- Auto-create PlanetaryRings from Rings array ---
            if body["type"] == "Planet" and "Rings" in msg_data:
                parent_body_id = body_id
                for i, ring in enumerate(msg_data["Rings"]):
                    ring_name = ring["Name"]
                    ring_body_id = parent_body_id + (i + 1)
                    ring_body = {
                        "system_id64": system_address,
                        "body_id": ring_body_id,
                        "body_name": ring_name,
                        "body_type_id": get_lookup_id(
                            "body_types", "PlanetaryRing", conn
                        ),
                        "planet_class_id": None,
                        "terraform_state_id": None,
                        "atmosphere_type_id": None,
                        "atmosphere_composition": None,
                        "atmosphere_id": None,
                        "volcanism_id": None,
                        "radius": None,
                        "mass_em": ring.get("MassMT") / 5.972e20
                        if ring.get("MassMT")
                        else None,  # MT → Earth Masses
                        "surface_gravity": None,
                        "surface_temperature": None,
                        "surface_pressure": None,
                        "axial_tilt": None,
                        "semi_major_axis": (ring["InnerRad"] + ring["OuterRad"]) / 2.0,
                        "eccentricity": None,
                        "orbital_inclination": None,
                        "periapsis": None,
                        "mean_anomaly": None,
                        "orbital_period": None,
                        "rotation_period": None,
                        "ascending_node": None,
                        "distance_from_arrival_ls": body["distance_from_arrival_ls"],
                        "age_my": None,
                        "absolute_magnitude": None,
                        "luminosity_id": None,
                        "star_type_id": None,
                        "subclass": None,
                        "stellar_mass": None,
                        "composition_ice": None,
                        "composition_metal": None,
                        "composition_rock": None,
                        "materials": None,
                        "parents": [{"Planet": parent_body_id}],
                        "tidally_locked": None,
                        "landable": False,
                        "updatetime": updatetime,
                        "ring_class_id": get_lookup_id(
                            "ring_classes", ring.get("RingClass"), conn
                        ),
                        "ring_inner_rad": ring.get("InnerRad"),
                        "ring_outer_rad": ring.get("OuterRad"),
                        "ring_mass_mt": ring.get("MassMT"),
                    }

                    # === Log suspicious PlanetaryRing insertions ===
                    if not ring_body["parents"]:
                        try:
                            with open("suspicious_planetaryrings.log", "a") as f:
                                f.write(
                                    json.dumps(
                                        {
                                            "timestamp": datetime.now().isoformat(),
                                            "system_id64": ring_body["system_id64"],
                                            "body_id": ring_body["body_id"],
                                            "body_name": ring_body["body_name"],
                                            "parents": ring_body["parents"],
                                        }
                                    )
                                    + "\n"
                                )
                        except Exception as log_err:
                            print(f"Failed to log suspicious ring: {log_err}")

                    with conn.cursor() as cur:
                        cur.execute(
                            UPSERT_BODY,
                            [
                                ring_body["system_id64"],
                                ring_body["body_id"],
                                ring_body["body_name"],
                                ring_body["body_type_id"],
                                ring_body["planet_class_id"],
                                ring_body["terraform_state_id"],
                                ring_body["atmosphere_type_id"],
                                ring_body["atmosphere_composition"],
                                ring_body["atmosphere_id"],
                                ring_body["volcanism_id"],
                                ring_body["radius"],
                                ring_body["mass_em"],
                                ring_body["surface_gravity"],
                                ring_body["surface_temperature"],
                                ring_body["surface_pressure"],
                                ring_body["axial_tilt"],
                                ring_body["semi_major_axis"],
                                ring_body["eccentricity"],
                                ring_body["orbital_inclination"],
                                ring_body["periapsis"],
                                ring_body["mean_anomaly"],
                                ring_body["orbital_period"],
                                ring_body["rotation_period"],
                                ring_body["ascending_node"],
                                ring_body["distance_from_arrival_ls"],
                                ring_body["age_my"],
                                ring_body["absolute_magnitude"],
                                ring_body["luminosity_id"],
                                ring_body["star_type_id"],
                                ring_body["subclass"],
                                ring_body["stellar_mass"],
                                ring_body["composition_ice"],
                                ring_body["composition_metal"],
                                ring_body["composition_rock"],
                                ring_body["materials"],
                                json.dumps(ring_body["parents"])
                                if ring_body["parents"]
                                else None,
                                ring_body["tidally_locked"],
                                ring_body["landable"],
                                ring_body["updatetime"],
                                ring_body["ring_class_id"],
                                ring_body["ring_inner_rad"],
                                ring_body["ring_outer_rad"],
                                ring_body["ring_mass_mt"],
                            ],
                        )
                    conn.commit()
                    print(
                        f"(o) Inferred PlanetaryRing: {ring_name} | ID: {ring_body_id}"
                    )

        # --- Handle ScanBaryCentre ---
        elif event == "ScanBaryCentre":
            body["semi_major_axis"] = msg_data.get("SemiMajorAxis")
            body["eccentricity"] = msg_data.get("Eccentricity")
            body["orbital_inclination"] = msg_data.get("OrbitalInclination")
            body["periapsis"] = msg_data.get("Periapsis")
            body["mean_anomaly"] = msg_data.get("MeanAnomaly")
            body["orbital_period"] = msg_data.get("OrbitalPeriod")
            body["ascending_node"] = msg_data.get("AscendingNode")

        # --- Handle Location (Planet) ---
        elif event == "Location" and body["type"] == "Planet":
            body["landable"] = msg_data.get("Landable")

        # --- Fill remaining as None if not set ---
        for col in [
            "planet_class_id",
            "terraform_state_id",
            "atmosphere_type_id",
            "atmosphere_composition",
            "atmosphere_id",
            "volcanism_id",
            "radius",
            "mass_em",
            "surface_gravity",
            "surface_temperature",
            "surface_pressure",
            "axial_tilt",
            "semi_major_axis",
            "eccentricity",
            "orbital_inclination",
            "periapsis",
            "mean_anomaly",
            "orbital_period",
            "rotation_period",
            "ascending_node",
            "distance_from_arrival_ls",
            "age_my",
            "absolute_magnitude",
            "luminosity_id",
            "star_type_id",
            "subclass",
            "stellar_mass",
            "composition_ice",
            "composition_metal",
            "composition_rock",
            "materials",
            "parents",
            "tidally_locked",
            "landable",
            "ring_class_id",
            "ring_inner_rad",
            "ring_outer_rad",
            "ring_mass_mt",
        ]:
            if col not in body:
                body[col] = None

        # --- Execute UPSERT ---
        with conn.cursor() as cur:
            cur.execute(
                UPSERT_BODY,
                [
                    body["system_id64"],
                    body["body_id"],
                    body["body_name"],
                    body["body_type_id"],
                    body["planet_class_id"],
                    body["terraform_state_id"],
                    body["atmosphere_type_id"],
                    json.dumps(body["atmosphere_composition"])
                    if body["atmosphere_composition"] is not None
                    else None,
                    body["atmosphere_id"],
                    body["volcanism_id"],
                    body["radius"],
                    body["mass_em"],
                    body["surface_gravity"],
                    body["surface_temperature"],
                    body["surface_pressure"],
                    body["axial_tilt"],
                    body["semi_major_axis"],
                    body["eccentricity"],
                    body["orbital_inclination"],
                    body["periapsis"],
                    body["mean_anomaly"],
                    body["orbital_period"],
                    body["rotation_period"],
                    body["ascending_node"],
                    body["distance_from_arrival_ls"],
                    body["age_my"],
                    body["absolute_magnitude"],
                    body["luminosity_id"],
                    body["star_type_id"],
                    body["subclass"],
                    body["stellar_mass"],
                    body["composition_ice"],
                    body["composition_metal"],
                    body["composition_rock"],
                    json.dumps(body["materials"])
                    if body["materials"] is not None
                    else None,
                    json.dumps(body["parents"])
                    if body["parents"] is not None
                    else None,
                    body["tidally_locked"],
                    body["landable"],
                    body["updatetime"],
                    body["ring_class_id"],
                    body["ring_inner_rad"],
                    body["ring_outer_rad"],
                    body["ring_mass_mt"],
                ],
            )
        conn.commit()

        print(
            f"{event}: {body['body_name']} | Type: {body['type']} | System: {star_system}"
        )
    except Exception as e:
        print(f"Error processing message: {e}")
        # Log the problematic message for later review
        try:
            with open("error_log.jsonl", "a") as f:
                # Write the full message as a JSON line
                f.write(
                    json.dumps(
                        {
                            "timestamp": datetime.now().isoformat(),
                            "error": str(e),
                            "header": header,
                            "message": msg_data,
                        }
                    )
                    + "\n"
                )
        except Exception as log_error:
            print(f"Failed to log error: {log_error}")

        # Always rollback to avoid "transaction aborted" hang
        conn.rollback()
        continue

# === Cleanup ===
conn.close()
