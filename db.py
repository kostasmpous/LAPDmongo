import os
import random
import certifi
import pandas as pd
from faker import Faker
from pymongo import MongoClient

# MongoDB connection
connection_string = "mongodb+srv://kostasmpous:C1bLHNuvmSsmZH0U@lapd.t2ysg.mongodb.net/?retryWrites=true&w=majority&appName=Lapd"
client = MongoClient(connection_string,tlsCAFile=certifi.where())
NUM_OFFICERS = 5000
officer_list = []
# Database instance
db = client['lapd']
#library to fake data for officers
faker = Faker()
#a dictionary to track how many upvotes an officer did
officer_upvote_tracker = {}

# Define JSON Schema for the collection
collection_name = "reports"
collection = db[collection_name]
collection.drop()
#generate random officers
def generate_officers():
    for _ in range(NUM_OFFICERS):
        officer_name = faker.name()
        officer_email = faker.email()
        officer_badge_number = str(random.randint(10000, 99999))
        officer_list.append({
            "officer_name": officer_name,
            "officer_email": officer_email,
            "officer_badge_number": officer_badge_number,
            "upvote_count": 0  # Track upvotes per officer
        })


def get_random_officer():
    """Select an officer from the pre-generated list while ensuring they don't exceed 1000 upvotes."""
    while True:
        officer = random.choice(officer_list)  # Pick a random officer

        # Unique identifier
        officer_id = f"{officer['officer_name']}_{officer['officer_badge_number']}"

        # Ensure officer has less than 1000 upvotes
        if officer_upvote_tracker.get(officer_id, 0) < 1000:
            # Increment officer's upvote count
            officer_upvote_tracker[officer_id] = officer_upvote_tracker.get(officer_id, 0) + 1
            return officer


generate_officers()


def generate_random_upvotes(force_upvote=False):
    """Generate upvotes ensuring at least ⅓ of reports have upvotes and no officer exceeds 1000 upvotes."""
    upvote_list = []

    # 33% of reports should have at least one upvote
    should_have_upvotes = force_upvote or (random.random() < 0.33)  # ⅓ probability

    if should_have_upvotes:
        num_upvotes = random.randint(1, 5)  # Between 1 to 5 upvotes per report

        for _ in range(num_upvotes):
            officer = get_random_officer()

            upvote_list.append({
                "officer_name": officer["officer_name"],
                "officer_email": officer["officer_email"],
                "officer_badge_number": officer["officer_badge_number"]
            })

    return {
        "count": len(upvote_list),
        "list": upvote_list
    }


def mapping_gender(gender):
    if gender == 'F':
        return 'Female'
    elif gender == 'M':
        return 'Male'
    else:
        return 'Unknown'

def mapping_descent(desc):
    match desc:
        case "A":
            return "Other Asian"
        case "B":
            return "Black"
        case "C":
            return "Chinese"
        case "D":
            return "Cambodian"
        case "F":
            return "Filipino"
        case "G":
            return "Guamanian"
        case "H":
            return "Hispanic/Latin/Mexican"
        case "I":
            return "American Indian/Alaskan Native"
        case "J":
            return "Japanese"
        case "K":
            return "Korean"
        case "L":
            return "Laotian"
        case "O":
            return "Other"
        case "P":
            return "Pacific Islander"
        case "S":
            return "Samoan"
        case "U":
            return "Hawaiian"
        case "V":
            return "Vietnamese"
        case "W":
            return "White"
        case "X":
            return "Unknown"
        case "Z":
            return "Asian Indian"

# Function to transform each row
def transform_row(row):
    return {
        "dr_no": str(row["DR_NO"]),
        "date_rptd": row["Date Rptd"],
        "date_occ": row["DATE OCC"],
        "time_occ": int(row["TIME OCC"]),
        "area": int(row["AREA"]),
        "area_name": row["AREA NAME"],
        "rpt_dist_no": str(row["Rpt Dist No"]),

        "crm_codes": {
            "crm_cd": str(row["Crm Cd"]) if pd.notna(row["Crm Cd"]) else None,
            "crm_cd_desc": row["Crm Cd Desc"],
            "crm_cd_1": str(row["Crm Cd 1"]) if pd.notna(row["Crm Cd 1"]) else None,
            "crm_cd_2": str(row["Crm Cd 2"]) if pd.notna(row["Crm Cd 2"]) else None,
            "crm_cd_3": str(row["Crm Cd 3"]) if pd.notna(row["Crm Cd 3"]) else None,
            "crm_cd_4": str(row["Crm Cd 4"]) if pd.notna(row["Crm Cd 4"]) else None,
        },

        "mocodes": str(row["Mocodes"]).split() if pd.notna(row["Mocodes"]) else [],

        "victim": {
            "vict_age": int(row["Vict Age"]) if pd.notna(row["Vict Age"]) else "",
            "vict_sex": mapping_gender(row["Vict Sex"])  if pd.notna(row["Vict Sex"]) else "",
            "vict_descent": mapping_descent(row["Vict Descent"]) if pd.notna(row["Vict Descent"]) else ""
        },

        "premis": {
            "premis_cd": str(row["Premis Cd"]) if pd.notna(row["Premis Cd"]) else None,
            "premis_desc": row["Premis Desc"]
        },

        "weapon": {
            "weapon_used_cd": str(row["Weapon Used Cd"]) if pd.notna(row["Weapon Used Cd"]) else "",
            "weapon_desc": str(row["Weapon Desc"]) if pd.notna(row["Weapon Desc"]) else ""  # FIX: Replace None with ""
        },

        "location_info": {
            "location": row["LOCATION"],
            "lat": float(row["LAT"]) if pd.notna(row["LAT"]) else None,
            "lon": float(row["LON"]) if pd.notna(row["LON"]) else None
        },

        "status": row["Status"],
        "status_desc": row["Status Desc"],
        "upvotes": generate_random_upvotes()
    }

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


# Function to load CSV and insert data into MongoDB
def load_csv_to_mongodb():
    try:
        # Read CSV file
        df = pd.read_csv('/Users/kbousinis/PycharmProjects/lapdMongoDatabase/files/cd.csv').head(100)

        df["DR_NO"] = df["DR_NO"].astype(str)
        df["Rpt Dist No"] = df["Rpt Dist No"].astype(str)
        df["Crm Cd"] = df["Crm Cd"].astype(str)
        df["Crm Cd 1"] = df["Crm Cd 1"].astype(str)
        df["Crm Cd 2"] = df["Crm Cd 2"].astype(str)
        df["Crm Cd 3"] = df["Crm Cd 3"].astype(str)
        df["Crm Cd 4"] = df["Crm Cd 4"].astype(str)
        df["Premis Cd"] = df["Premis Cd"].astype(str)
        df["Weapon Used Cd"] = df["Weapon Used Cd"].astype(str)
        # Convert all rows
        json_data = [transform_row(row) for _, row in df.iterrows()]

        #records = df.to_dict(orient="records")

        # Insert into MongoDB
        collection = db[collection_name]
        collection.insert_many(json_data)

        print(f"✅ Successfully inserted {len(json_data)} records into MongoDB!")
    except Exception as e:
        print(f"❌ Error occurred: {e}")


# Call the function
if __name__ == "__main__":
    load_csv_to_mongodb()

