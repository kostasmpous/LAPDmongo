import os

import certifi
import pandas as pd
from pymongo import MongoClient

# MongoDB connection
connection_string = "mongodb+srv://kostasmpous:C1bLHNuvmSsmZH0U@lapd.t2ysg.mongodb.net/?retryWrites=true&w=majority&appName=Lapd"
client = MongoClient(connection_string,tlsCAFile=certifi.where())

# Database instance
db = client['lapd']

# Define JSON Schema for the collection
collection_name = "reports"

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
                        "crm_cd": {"bsonType": "int", "description": "Crime code (required)"},
                        "crm_cd_desc": {"bsonType": "string", "description": "Crime description (required)"},
                        "crm_cd_1": {"bsonType": "int", "description": "Primary crime code (required)"},
                        "crm_cd_2": {"bsonType": ["int", "null"], "description": "Secondary crime code (optional)"},
                        "crm_cd_3": {"bsonType": ["int", "null"], "description": "Tertiary crime code (optional)"},
                        "crm_cd_4": {"bsonType": ["int", "null"], "description": "Quaternary crime code (optional)"}
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
                        "premis_cd": {"bsonType": "int", "description": "Premise code (required)"},
                        "premis_desc": {"bsonType": "string", "description": "Premise description (required)"}
                    }
                },
                "weapon": {
                    "bsonType": "object",
                    "required": ["weapon_used_cd", "weapon_desc"],
                    "properties": {
                        "weapon_used_cd": {"bsonType": "int", "description": "Weapon used code (required)"},
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
                    "bsonType": "array",
                    "items": {
                        "bsonType": "object",
                        "required": ["officer_name", "officer_email", "officer_badge_number"],
                        "properties": {
                            "officer_name": {"bsonType": "string", "description": "Officer name (required)"},
                            "officer_email": {"bsonType": "string", "description": "Officer email (required)"},
                            "officer_badge_number": {"bsonType": "string",
                                                     "description": "Officer badge number (required)"}
                        }
                    }
                }
            }
        }
    }, check_exists=True)
    print("✅ Collection created with schema validation!")
except Exception as e:
    print(f"⚠️ Schema creation skipped (might already exist): {e}")

# Path to CSV file
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
CSV_FILE_PATH = os.path.join(BASE_DIR, "../files/cd.csv")


# Function to load CSV and insert data into MongoDB
def load_csv_to_mongodb():
    try:
        # Read CSV file
        df = pd.read_csv('/Users/kbousinis/PycharmProjects/lapdMongoDatabase/files/cd.csv').head(100)
        for index, row in df.iterrows():

        records = df.to_dict(orient="records")

        # Insert into MongoDB
        collection = db[collection_name]
        collection.insert_many(records)

        print(f"✅ Successfully inserted {len(records)} records into MongoDB!")
    except Exception as e:
        print(f"❌ Error occurred: {e}")


# Call the function
if __name__ == "__main__":
    load_csv_to_mongodb()
