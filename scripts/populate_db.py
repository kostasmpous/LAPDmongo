import concurrent.futures
import os
from datetime import datetime
import pandas as pd
import db_utils
from db import db, collection_name, collection

try:
    db.create_collection(collection_name, validator={
        "$jsonSchema": {
            "bsonType": "object",
            "properties": {
                "dr_no": {"bsonType": "string", "description": "DR number (required)"},
                "date_rptd": {"bsonType": "string", "description": "Date reported (required, format: MM/DD/YYYY)"},
                "date_occ": {"bsonType": "string", "description": "Date of occurrence (required, format: MM/DD/YYYY)"},
                "time_occ": {"bsonType": "string", "description": "Time of occurrence (required)"},
                "area": {"bsonType": "int", "description": "Area code (required)"},
                "area_name": {"bsonType": "string", "description": "Area name (required)"},
                "rpt_dist_no": {"bsonType": "string", "description": "Report district number (optional)"},

                "crm_codes": {
                    "bsonType": "object",
                    "required": ["crm_cd", "crm_cd_desc", "crm_cd_1"],
                    "properties": {
                        "crm_cd": {"bsonType": "string", "description": "Crime code (required)"},
                        "crm_cd_desc": {"bsonType": "string", "description": "Crime description (required)"},
                        "crm_cd_1": {"bsonType": "string", "description": "Primary crime code (required)"},
                        "crm_cd_2": {"bsonType": ["string", "null"], "description": "Secondary crime code (optional)"},
                        "crm_cd_3": {"bsonType": ["string", "null"], "description": "Tertiary crime code (optional)"},
                        "crm_cd_4": {"bsonType": ["string", "null"], "description": "Quaternary crime code (optional)"}
                    }
                },

                "mocodes": {
                    "bsonType": "array",
                    "items": {"bsonType": "string"},
                    "description": "List of modus operandi codes (optional)"
                },

                "victim": {
                    "bsonType": "object",
                    "required": ["vict_age", "vict_sex", "vict_descent"],
                    "properties": {
                        "vict_age": {"bsonType": "int", "description": "Victim age (required)"},
                        "vict_sex": {"bsonType": "string", "description": "Victim sex (required)"},
                        "vict_descent": {"bsonType": "string", "description": "Victim descent (required)"}
                    }
                },

                "premis": {
                    "bsonType": "object",
                    "required": ["premis_cd", "premis_desc"],
                    "properties": {
                        "premis_cd": {"bsonType": "string", "description": "Premise code (required)"},
                        "premis_desc": {"bsonType": "string", "description": "Premise description (required)"}
                    }
                },

                "weapon": {
                    "bsonType": "object",
                    "required": ["weapon_used_cd", "weapon_desc"],
                    "properties": {
                        "weapon_used_cd": {"bsonType": "string", "description": "Weapon used code (required)"},
                        "weapon_desc": {"bsonType": "string", "description": "Weapon description (required)"}
                    }
                },

                "location_info": {
                    "bsonType": "object",
                    "required": ["location", "lat", "lon"],
                    "properties": {
                        "location": {"bsonType": "string", "description": "Location address (required)"},
                        "lat": {"bsonType": "double", "description": "Latitude (required)"},
                        "lon": {"bsonType": "double", "description": "Longitude (required)"}
                    }
                },

                "status": {"bsonType": "string", "description": "Status code (optional)"},
                "status_desc": {"bsonType": "string", "description": "Status description (optional)"},

                "upvotes": {
        "bsonType": "object",
        "description": "Contains count and list of upvotes",
        "properties": {
            "count": {
                "bsonType": "int",
                "description": "Total number of upvotes",
                "minimum": 0
            },
            "list": {
                "bsonType": "array",
                "description": "List of officer upvotes",
                "items": {
                    "bsonType": "object",
                    "required": ["officer_name", "officer_email", "officer_badge_number"],
                    "properties": {
                        "officer_name": {"bsonType": "string", "description": "Officer name (required)"},
                        "officer_email": {"bsonType": "string", "description": "Officer email (required)"},
                        "officer_badge_number": {"bsonType": "string", "description": "Officer badge number (required)"}
                    }
                }
            }
        }
    }
            }
        }
    }, check_exists=True)
    print("✅ Collection schema updated to expect strings for all codes!")
except Exception as e:
    print(f"⚠️ Schema creation skipped (might already exist): {e}")


# Path to CSV file
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
CSV_FILE_PATH = os.path.join(BASE_DIR, "../files/cd.csv")


def transform_row(row):
    """Convert row into MongoDB-compatible format."""
    # Convert DATE OCC (Ensure it's in MM/DD/YYYY format)
    date_occ_str = row["DATE OCC"].split(" ")[0]  # Remove time portion if it exists
    date_occ = datetime.strptime(date_occ_str, "%m/%d/%Y")  # Convert to datetime

    # Convert TIME OCC (Ensure it's in HHMM format)
    time_occ_str = str(int(row["TIME OCC"])).zfill(4)  # Ensure 4-digit format
    time_occ = time_occ_str# Store as an integer (e.g., 2130 for 21:30)
    # Extract first crime description
    primary_crime_desc = str(row.get("Crm Cd Desc", "")).strip()

    # Convert all crime codes into a list of dictionaries
    crime_codes = []
    for i in range(1, 5):  # Assuming up to 4 crime codes
        crm_cd = str(row.get(f"Crm Cd {i}", "")).strip()

        if crm_cd and crm_cd.lower() != "nan":  # Ensure values are valid
            crime_codes.append(crm_cd[:3])
    final_crime_codes = {
        "crime_codes" : crime_codes,
        "crm_cd_desc": primary_crime_desc
    }
    # Merge DATE OCC + TIME OCC into a full datetime
    #full_datetime = datetime.combine(date_occ.date(), time_occ)
    return {
        "dr_no": str(row["DR_NO"]),
        "date_rptd": row["Date Rptd"],
        "date_occ": str(row["DATE OCC"]),
        "time_occ": time_occ,
        "area": int(row["AREA"]),
        "area_name": row["AREA NAME"],
        "rpt_dist_no": str(row["Rpt Dist No"]),

        "crm_codes": final_crime_codes,

        "mocodes": row["Mocodes"].split() if pd.notna(row["Mocodes"]) else [],

        "victim": {
            "vict_age": int(row["Vict Age"]) if pd.notna(row["Vict Age"]) else None,
            "vict_sex": db_utils.mapping_gender(row["Vict Sex"]),
            "vict_descent": db_utils.mapping_descent(row["Vict Descent"])
        },

        "premis": {
            "premis_cd": str(row["Premis Cd"]) if pd.notna(row["Premis Cd"]) else "",
            "premis_desc": str(row["Premis Desc"]) if pd.notna(row["Premis Desc"]) else ""
        },

        "weapon": {
            "weapon_used_cd": str(row["Weapon Used Cd"]) if pd.notna(row["Weapon Used Cd"]) else "",
            "weapon_desc": str(row["Weapon Desc"]) if pd.notna(row["Weapon Desc"]) else ""
        },

        "location_info": {
            "location": row["LOCATION"],
            "lat": float(row["LAT"]) if pd.notna(row["LAT"]) else None,
            "lon": float(row["LON"]) if pd.notna(row["LON"]) else None
        },

        "status": row["Status"],
        "status_desc": row["Status Desc"],
        "upvotes": db_utils.generate_random_upvotes()
    }


def bulk_insert(data):
    """Insert data into MongoDB in bulk."""
    if data:
        collection.insert_many(data)


def load_csv_to_mongodb(csv_file):
    """Loads CSV data efficiently into MongoDB."""
    CHUNK_SIZE = 10_000

    try:
        collection.drop()  # 🗑️ Drop old collection to start fresh
        print("🗑️ Collection dropped successfully.")

        with concurrent.futures.ThreadPoolExecutor(max_workers=4) as executor:
            futures = []

            for chunk in pd.read_csv(csv_file, chunksize=CHUNK_SIZE):
                chunk = chunk.where(pd.notna(chunk), None)  # Convert NaN to None
                json_data = [transform_row(row) for _, row in chunk.iterrows()]
                futures.append(executor.submit(bulk_insert, json_data))

            concurrent.futures.wait(futures)

        print("🎉 All data imported successfully!")

    except Exception as e:
        print(f"❌ Error occurred: {e}")


# Run the function
if __name__ == "__main__":
    load_csv_to_mongodb("/Users/kbousinis/PycharmProjects/lapdMongoDatabase/files/cd.csv")
