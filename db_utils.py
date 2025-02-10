import pandas as pd
import random
from faker import Faker

officer_list = []
NUM_OFFICERS = 5000
faker = Faker()
officer_upvote_tracker = {}

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


