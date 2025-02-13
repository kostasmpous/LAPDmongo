from random import random

from bson import ObjectId

from db import collection_reports, collection_officers
from fastapi import APIRouter, Query, HTTPException
from datetime import datetime
from models.officer_model import PoliceOfficer
from scripts.global_constant import RANKS, DEPARTMENTS

router = APIRouter(prefix="/officers", tags=["Officers"])

@router.post("/add")
async def add_officer(officer: PoliceOfficer):
    # Ensure MongoDB connection is active
    if collection_officers is None:
        raise HTTPException(status_code=500, detail="Database connection failed.")

    # Check if the badge number is already assigned to another officer
    existing_officer = collection_officers.find_one({"badge_number": officer.badge_number})

    if existing_officer:
        raise HTTPException(status_code=400, detail="Badge number already assigned to another officer.")

    # Auto-assign rank & department if not provided
    officer_rank = officer.rank if officer.rank else random.choice(RANKS)
    officer_department = officer.department if officer.department else random.choice(DEPARTMENTS)

    # Create officer document
    officer_data = {
        "_id": ObjectId(),  # Generate a unique MongoDB ObjectId
        "name": officer.name,
        "email": officer.email,
        "badge_number": officer.badge_number,
        "rank": officer_rank,
        "department": officer_department,
        "date_joined": datetime.utcnow(),
        "active": officer.active
    }

    try:
        # Insert the officer into the `officers` collection (async)
        collection_officers.insert_one(officer_data)

        # Convert `_id` and `date_joined` to JSON-safe format
        officer_data["_id"] = str(officer_data["_id"])
        officer_data["date_joined"] = officer_data["date_joined"].isoformat()  #  Convert datetime to string
        return {"message": "Officer created successfully."}

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"An error occurred: {str(e)}")


@router.get("/find/")
async def find_officers(name:str):
    # Ensure MongoDB connection is active
    if collection_officers is None:
        raise HTTPException(status_code=500, detail="Database connection failed.")

    #  MongoDB Case-Insensitive Search (Using regex)
    query = {"name": {"$regex": name, "$options": "i"}}  # Case-insensitive search

    officers_cursor = collection_officers.find(query)
    officers =  officers_cursor.to_list(length=50)  # Limit results to 50

    # Convert MongoDB `_id` to string
    for officer in officers:
        officer["_id"] = str(officer["_id"])

    if not officers:
        raise HTTPException(status_code=404, detail="No officers found.")

    return {"message": "Officers found.", "officers": officers}
