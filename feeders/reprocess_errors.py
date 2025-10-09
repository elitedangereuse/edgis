import os
import json
import psycopg
from datetime import datetime
import time
import shutil
from pathlib import Path
import re
from dotenv import load_dotenv

# === Configuration ===
ERROR_LOG_FILE = "error_log.jsonl"
TEMP_LOG_FILE = "error_log.jsonl.tmp"
load_dotenv()

DB_HOST = os.getenv("DB_HOST")
DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")

# Foreign key constraint name we're handling
FK_VIOLATION = "bodies_system_id64_fkey"

# Connect to DB
conn = psycopg.connect(
    host=DB_HOST, port=5432, dbname=DB_NAME, user=DB_USER, password=DB_PASSWORD
)

# === UPSERT Query (same as your main script) ===
UPSERT_BODY = """
    INSERT INTO bodies (
        system_id64, body_id, body_name, type, planet_class, terraform_state,
        atmosphere_type, atmosphere_composition, atmosphere, volcanism, radius, mass_em,
        surface_gravity, surface_temperature, surface_pressure, axial_tilt,
        semi_major_axis, eccentricity, orbital_inclination, periapsis,
        mean_anomaly, orbital_period, rotation_period, ascending_node,
        distance_from_arrival_ls, age_my, absolute_magnitude, luminosity,
        star_type, subclass, stellar_mass, composition_ice, composition_metal,
        composition_rock, materials, parents, tidally_locked, landable, updatetime
    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
              %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
              %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
              %s, %s, %s, %s, %s, %s, %s, %s, %s)
    ON CONFLICT (system_id64, body_id) DO UPDATE SET
        type                    = EXCLUDED.type,
        planet_class            = COALESCE(EXCLUDED.planet_class, bodies.planet_class),
        terraform_state         = EXCLUDED.terraform_state,
        atmosphere_type         = EXCLUDED.atmosphere_type,
        atmosphere_composition  = COALESCE(EXCLUDED.atmosphere_composition, bodies.atmosphere_composition),
        atmosphere              = EXCLUDED.atmosphere,
        volcanism               = EXCLUDED.volcanism,
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
        luminosity              = EXCLUDED.luminosity,
        star_type               = EXCLUDED.star_type,
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
        updatetime              = EXCLUDED.updatetime
"""


# === Helper: Parse timestamp ===
def parse_timestamp(ts_str):
    try:
        return datetime.fromisoformat(ts_str.replace("Z", "+00:00"))
    except (ValueError, TypeError):
        return None


# === Infer canonical body type (same as main script) ===
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


# === Main reprocessing logic ===
def reprocess():
    if not Path(ERROR_LOG_FILE).exists():
        print(f"No error log found: {ERROR_LOG_FILE}")
        return

    lines = []
    with open(ERROR_LOG_FILE, "r") as f:
        lines = [line.strip() for line in f if line.strip()]

    print(f"Found {len(lines)} error entries to reprocess.")

    success_count = 0
    failed_count = 0

    with open(TEMP_LOG_FILE, "w") as temp_out:
        for i, line in enumerate(lines):
            try:
                log_entry = json.loads(line)
                error_msg = log_entry.get("error", "")
                msg_data = log_entry.get("message", {})
                header = log_entry.get("header", {})
                event = msg_data.get("event")

                # Only handle FK violations for bodies_system_id64_fkey
                if FK_VIOLATION not in error_msg:
                    print(f"[{i+1}/{len(lines)}] Skipping non-FK error: {event}")
                    temp_out.write(line + "\n")
                    failed_count += 1
                    continue

                system_address = msg_data.get("SystemAddress")
                body_id = msg_data.get("BodyID")

                if not system_address or body_id is None:
                    print(f"[{i+1}/{len(lines)}] Invalid system or body ID, skipping.")
                    temp_out.write(line + "\n")
                    failed_count += 1
                    continue

                # === Build body object (minimal version from message) ===
                body = {}
                body["system_id64"] = system_address
                body["body_id"] = body_id
                body["updatetime"] = parse_timestamp(msg_data.get("timestamp"))

                if not body["updatetime"]:
                    print(f"[{i+1}/{len(lines)}] Invalid timestamp, skipping.")
                    temp_out.write(line + "\n")
                    failed_count += 1
                    continue

                # Body name
                body["body_name"] = msg_data.get("BodyName") or msg_data.get("Body")
                if not body["body_name"]:
                    print(f"[{i+1}/{len(lines)}] Missing body name, skipping.")
                    temp_out.write(line + "\n")
                    failed_count += 1
                    continue

                # Infer type
                body["type"] = infer_body_type(msg_data, body["body_name"])
                if body["type"] == "Unknown":
                    print(f"[{i+1}/{len(lines)}] Unknown body type, skipping.")
                    temp_out.write(line + "\n")
                    failed_count += 1
                    continue

                # Skip stations
                if msg_data.get("BodyType") == "Station":
                    print(f"[{i+1}/{len(lines)}] Skipping station: {body['body_name']}")
                    success_count += 1  # Treat as resolved
                    continue

                # === Fill fields based on event ===
                # This is a simplified version. You may want to expand it like your main script.
                body["planet_class"] = msg_data.get("PlanetClass")
                body["terraform_state"] = msg_data.get("TerraformState") or ""
                body["atmosphere_type"] = msg_data.get("AtmosphereType") or ""
                body["atmosphere_composition"] = msg_data.get(
                    "AtmosphereComposition", []
                )
                body["atmosphere"] = msg_data.get("Atmosphere") or ""
                body["volcanism"] = msg_data.get("Volcanism") or ""
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
                body["age_my"] = msg_data.get("Age_MY")
                body["absolute_magnitude"] = msg_data.get("AbsoluteMagnitude")
                body["luminosity"] = msg_data.get("Luminosity")
                body["star_type"] = msg_data.get("StarType")
                body["subclass"] = msg_data.get("Subclass")
                body["stellar_mass"] = msg_data.get("StellarMass")
                body["tidally_locked"] = msg_data.get("TidalLock")
                body["landable"] = msg_data.get("Landable")

                comp = msg_data.get("Composition", {})
                body["composition_ice"] = comp.get("Ice")
                body["composition_metal"] = comp.get("Metal")
                body["composition_rock"] = comp.get("Rock")

                body["materials"] = msg_data.get("Materials", [])
                body["parents"] = msg_data.get("Parents", [])

                # Fill missing fields with None
                for col in [
                    "planet_class",
                    "terraform_state",
                    "atmosphere_type",
                    "atmosphere_composition",
                    "atmosphere",
                    "volcanism",
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
                    "luminosity",
                    "star_type",
                    "subclass",
                    "stellar_mass",
                    "composition_ice",
                    "composition_metal",
                    "composition_rock",
                    "materials",
                    "parents",
                    "tidally_locked",
                    "landable",
                ]:
                    if col not in body:
                        body[col] = None

                # === Attempt UPSERT ===
                with conn.cursor() as cur:
                    cur.execute(
                        UPSERT_BODY,
                        [
                            body["system_id64"],
                            body["body_id"],
                            body["body_name"],
                            body["type"],
                            body["planet_class"],
                            body["terraform_state"],
                            body["atmosphere_type"],
                            json.dumps(body["atmosphere_composition"])
                            if body["atmosphere_composition"] is not None
                            else None,
                            body["atmosphere"],
                            body["volcanism"],
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
                            body["luminosity"],
                            body["star_type"],
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
                        ],
                    )
                conn.commit()

                print(
                    f"[{i+1}/{len(lines)}] SUCCESS: Inserted {body['body_name']} (System: {system_address})"
                )
                success_count += 1

            except Exception as e:
                print(f"[{i+1}/{len(lines)}] REPROCESS ERROR: {e}")
                conn.rollback()
                # Write back to log
                temp_out.write(line + "\n")
                failed_count += 1

            # Optional: Rate limiting
            time.sleep(0.01)

    # Replace original log with filtered one
    shutil.move(TEMP_LOG_FILE, ERROR_LOG_FILE)
    print(
        f"Reprocessing complete. {success_count} succeeded, {failed_count} still failed."
    )
    conn.close()


if __name__ == "__main__":
    reprocess()
