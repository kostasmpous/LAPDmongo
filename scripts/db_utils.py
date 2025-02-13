from bson import ObjectId
from pymongo import MongoClient, UpdateOne
from datetime import datetime, timedelta
import pandas as pd
import random
from faker import Faker

from db import collection_officers, collection_upvotes, collection_reports
from models.officer_model import PoliceOfficer
from models.upvote_model import Upvote

faker = Faker()
NUM_OFFICERS = 5000
# List of police ranks
RANKS = ["Officer", "Detective", "Sergeant", "Lieutenant", "Captain", "Deputy Chief", "Chief of Police"]
# List of police departments
DEPARTMENTS = ["Homicide", "Narcotics", "Cyber Crime", "Traffic Control", "Patrol", "Forensics", "Special Ops"]
officer_list = []
officer_collection_list=[]
upvotes_collection_list=[]
officer_upvote_tracker = {}

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


def random_date_joined():
    """Generate a random `date_joined` within the last 30 years as a datetime object."""
    start_date = datetime.now() - timedelta(days=30 * 365)  # 30 years ago
    random_date = faker.date_between(start_date=start_date, end_date="today")

    # ✅ Convert `datetime.date` to `datetime.datetime`
    return datetime.combine(random_date, datetime.min.time())  # Converts date to datetime


def clean_text(value):
    """Ensure values are stored as strings or empty strings if missing."""
    if pd.notna(value):  # If value is not NaN
        return str(value).strip()  # Convert to string and strip whitespace
    return ""  # Return an empty string instead of None


def clean_crime_code(value):
    """Converts crime codes into clean strings without decimals."""
    if pd.notna(value):  # Check if value is not NaN
        value = str(value)  # Convert everything to string
        if value.endswith(".0"):  # Remove decimal if it's a whole number
            return value[:-2]  # Remove ".0"
        return value  # Return as is
    return None  # Return None for missing values

def generate_officers():
    """Generate random officer data and insert them in bulk into MongoDB."""
    officer_collection_list = []  # Clear this to avoid memory carryover

    for _ in range(NUM_OFFICERS):
        officer_name = faker.name()
        officer_email = faker.email()
        officer_badge_number = str(random.randint(10000, 99999))

        officer_data = {
            "badge_number": officer_badge_number,
            "name": officer_name,
            "email": officer_email,
            "rank": random.choice(RANKS),
            "department": random.choice(DEPARTMENTS),
            "date_joined": random_date_joined(),
            "active": random.choice([True, False])
        }

        officer_collection_list.append(officer_data)

    #  Bulk Insert Officers
    if officer_collection_list:
        collection_officers.insert_many(officer_collection_list, ordered=False)
        print(f"Inserted {len(officer_collection_list)} officers successfully!")
    return officer_collection_list


def get_random_officer(officer_list):
    """Select an officer from the pre-generated list while ensuring they don't exceed 1000 upvotes."""
    while True:
        officer = random.choice(officer_list)  # Pick a random officer

        # Unique identifier
        officer_id = f"{officer['name']}_{officer['badge_number']}"

        # Ensure officer has less than 1000 upvotes
        if officer_upvote_tracker.get(officer_id, 0) < 1000:
            # Increment officer's upvote count
            officer_upvote_tracker[officer_id] = officer_upvote_tracker.get(officer_id, 0) + 1
            return officer


import random
from datetime import datetime
from pymongo import UpdateOne
from bson import ObjectId  # ✅ Import to generate unique MongoDB ObjectIds


def generate_random_upvotes_bulk(report_ids, officer_list, force_upvote=False, batch_size=10000):
    """Efficiently bulk generate upvotes for multiple reports and update MongoDB."""

    upvotes_collection_list = []  # Stores bulk upvote data
    operations = []  # Stores MongoDB bulk update operations

    # ✅ Preload officers into a list for fast random selection
    officer_pool = [
        {
            "name": officer["name"],
            "email": officer["email"],
            "badge_number": officer["badge_number"]
        }
        for officer in officer_list
    ]

    for report_id in report_ids:
        should_have_upvotes = force_upvote or (random.random() < 0.20)  # ✅ Precompute probability once

        if should_have_upvotes:
            num_upvotes = random.randint(1, 3)  # ✅ Precompute random number

            for _ in range(num_upvotes):
                officer = random.choice(officer_pool)  # ✅ Faster than calling `get_random_officer()`

                upvote_data = {
                    "_id": ObjectId(),  # ✅ Ensure a unique MongoDB ObjectId
                    "officer_name": officer["name"],
                    "officer_email": officer["email"],
                    "officer_badge_number": officer["badge_number"],
                    "report_id": report_id,
                    "upvote_time": datetime.utcnow()
                }

                upvotes_collection_list.append(upvote_data)

                # ✅ Use `$addToSet` to prevent duplicate upvotes
                operations.append(UpdateOne(
                    {"dr_no": report_id},
                    {
                        "$inc": {"upvotes.count": 1},
                        "$addToSet": {"upvotes.list": upvote_data},  # ✅ Prevents duplicate upvotes
                        "$setOnInsert": {"dr_no": report_id}  # ✅ Ensures report exists
                    },
                    upsert=True
                ))

        # ✅ Insert & update in large batches to optimize performance
        if len(upvotes_collection_list) >= batch_size:
            try:
                collection_upvotes.insert_many(upvotes_collection_list, ordered=False)
                print(f"✅ Inserted {len(upvotes_collection_list)} upvotes successfully!")
            except Exception as e:
                print(f"⚠️ Bulk insert error: {e}")
            upvotes_collection_list.clear()  # Free memory

        if len(operations) >= batch_size:
            try:
                collection_reports.bulk_write(operations, ordered=False)
                print(f"✅ Updated {len(operations)} reports with upvotes!")
            except Exception as e:
                print(f"⚠️ Bulk update error: {e}")
            operations.clear()  # Free memory

    # ✅ Final batch insert & update (if anything remains)
    if upvotes_collection_list:
        try:
            collection_upvotes.insert_many(upvotes_collection_list, ordered=False)
            print(f"✅ Inserted {len(upvotes_collection_list)} upvotes successfully!")
        except Exception as e:
            print(f"⚠️ Bulk insert error: {e}")

    if operations:
        try:
            collection_reports.bulk_write(operations, ordered=False)
            print(f"✅ Updated {len(operations)} reports with upvotes!")
        except Exception as e:
            print(f"⚠️ Bulk update error: {e}")


def add_upvotes_to_reports(upvotes_mapping):
    """
    Safely updates reports in bulk to add upvotes without replacing existing content.

    Args:
        upvotes_mapping (dict): {report_id: [list of upvote dictionaries]}
    """
    operations = []

    for report_id, upvotes in upvotes_mapping.items():
        clean_upvotes = []  # ✅ Remove unnecessary fields like `report_id` and `_id`

        for upvote in upvotes:
            clean_upvotes.append({
                "officer_name": upvote["officer_name"],
                "officer_email": upvote["officer_email"],
                "officer_badge_number": upvote["officer_badge_number"],
                "upvote_time": upvote["upvote_time"]  # ✅ Keep timestamp
            })

        operations.append(UpdateOne(
            {"dr_no": report_id},  # ✅ Find the correct report
            {
                "$inc": {"upvotes.count": len(clean_upvotes)},  # ✅ Increment count
                "$push": {"upvotes.list": {"$each": clean_upvotes}}  # ✅ Append upvotes
            },
            upsert=False  # ❌ Prevent creating new reports if missing
        ))

    if operations:
        collection_reports.bulk_write(operations)  # ✅ Perform bulk update efficiently
        print(f"✅ {len(operations)} upvote records updated in reports collection.")

