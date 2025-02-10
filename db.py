import concurrent.futures
import os
import certifi
import pandas as pd
from faker import Faker
from pymongo import MongoClient
import db_utils

# MongoDB connection
#connection_string = "mongodb+srv://kostasmpous:C1bLHNuvmSsmZH0U@lapd.t2ysg.mongodb.net/?retryWrites=true&w=majority&appName=Lapd"
#client = MongoClient(connection_string,tlsCAFile=certifi.where())

client = MongoClient("mongodb://localhost:27017/")
db_utils.generate_officers()
# Database instance
db = client['lapd']
# Define JSON Schema for the collection
collection_name = "reports"
collection = db[collection_name]
collection.drop()

try:
    db.create_collection(collection_name, validator={
        "$jsonSchema": {
            "bsonType": "object",
            "properties": {
                "dr_no": {"bsonType": "string", "description": "DR number (required)"},
                "date_rptd": {"bsonType": "string", "description": "Date reported (required, format: MM/DD/YYYY)"},
                "date_occ": {"bsonType": "string", "description": "Date of occurrence (required, format: MM/DD/YYYY)"},
                "time_occ": {"bsonType": "int", "description": "Time of occurrence (required)"},
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
    return {
        "dr_no": str(row["DR_NO"]),
        "date_rptd": row["Date Rptd"],
        "date_occ": row["DATE OCC"],
        "time_occ": int(row["TIME OCC"]),
        "area": int(row["AREA"]),
        "area_name": row["AREA NAME"],
        "rpt_dist_no": str(row["Rpt Dist No"]),

        "crm_codes": {
            "crm_cd": str(row["Crm Cd"]) if pd.notna(row["Crm Cd"]) else "",
            "crm_cd_desc": row["Crm Cd Desc"],
            "crm_cd_1": str(row["Crm Cd 1"]) if pd.notna(row["Crm Cd 1"]) else "",
            "crm_cd_2": str(row["Crm Cd 2"]) if pd.notna(row["Crm Cd 2"]) else "",
            "crm_cd_3": str(row["Crm Cd 3"]) if pd.notna(row["Crm Cd 3"]) else "",
            "crm_cd_4": str(row["Crm Cd 4"]) if pd.notna(row["Crm Cd 4"]) else "",
        },

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
        collection.insert_many(data)  # ✅ Faster batch insert


def load_csv_to_mongodb(csv_file):
    """Loads CSV data efficiently into MongoDB."""
    CHUNK_SIZE = 10_000  # ✅ Read 10,000 rows at a time

    try:
        collection.drop()  # 🗑️ Drop old collection to start fresh
        print("🗑️ Collection dropped successfully.")

        with concurrent.futures.ThreadPoolExecutor(max_workers=4) as executor:  # ✅ Parallel processing
            futures = []

            for chunk in pd.read_csv(csv_file, chunksize=CHUNK_SIZE):
                chunk = chunk.where(pd.notna(chunk), None)  # Convert NaN to None
                json_data = [transform_row(row) for _, row in chunk.iterrows()]
                futures.append(executor.submit(bulk_insert, json_data))  # ✅ Parallel insert

            concurrent.futures.wait(futures)  # ✅ Wait for all threads to complete

        print("🎉 All data imported successfully!")

    except Exception as e:
        print(f"❌ Error occurred: {e}")


# Run the function
if __name__ == "__main__":
    load_csv_to_mongodb("/Users/kbousinis/PycharmProjects/lapdMongoDatabase/files/cd.csv")
