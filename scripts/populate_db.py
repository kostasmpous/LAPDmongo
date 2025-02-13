import concurrent.futures
import os
from datetime import datetime
import pandas as pd
from db import db, collection_reports_name, collection_reports, collection_upvotes_name, collection_officers, \
    collection_officer_name, collection_upvotes
from scripts import db_utils


# Path to CSV file
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
CSV_FILE_PATH = os.path.join(BASE_DIR, "../files/cd.csv")


def transform_row(row):
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
        "upvotes":{
            "count":0,
            "list":[]
        }
    }


def bulk_insert(data):
    """Insert data into MongoDB in bulk."""
    if data:
        collection_reports.insert_many(data)


def load_csv_to_mongodb(csv_file):
    """Loads CSV data efficiently into MongoDB."""
    CHUNK_SIZE = 10_000

    try:
        collection_reports.drop()  # 🗑️ Drop old collection to start fresh
        collection_officers.drop()
        collection_upvotes.drop()
        print("🗑️ Collection dropped successfully.")

        try:
            db.create_collection(collection_upvotes_name)
            db.create_collection(collection_officer_name)
            db.create_collection(collection_reports_name)
            print("✅ Collection schema updated to expect strings for all codes!")
        except Exception as e:
            print(f"⚠️ Schema creation skipped (might already exist): {e}")
        list_officers = db_utils.generate_officers()
        list_records = []
        list_record_ids=[]
        for _, row in pd.read_csv(csv_file).iterrows():  # ✅ Use `.iterrows()`
            list_records.append(transform_row(row))
            list_record_ids.append(str(row["DR_NO"]))
        collection_reports.insert_many(list_records)
        print("🎉 All data imported successfully!")
        collection_reports.create_index("dr_no")
        print("index created")
        db_utils.generate_random_upvotes_bulk(list_record_ids,list_officers)
        print("🎉 Done all!")
    except Exception as e:
        print(f"❌ Error occurred: {e}")


# Run the function
if __name__ == "__main__":
    load_csv_to_mongodb("/Users/kbousinis/PycharmProjects/lapdMongoDatabase/files/cd.csv")
