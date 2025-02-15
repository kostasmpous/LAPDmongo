from bson import ObjectId

from db import collection_upvotes, collection_reports
from fastapi import APIRouter, Query, HTTPException
from datetime import datetime

from models.upvote_model import Upvote

router = APIRouter(prefix="/upvotes", tags=["Upvotes"])

@router.get("/Query7/")
def query7():
    try:
        pipeline = [
            {
                "$group": {
                    "_id": {
                        "officer_name": "$officer_name",
                        "officer_email": "$officer_email",
                        "officer_badge_number": "$officer_badge_number"
                    },
                    "total_upvotes": {"$sum": 1}  # Count total upvotes per officer
                }
            },
            {
                "$project": { #Format results
                    "_id": 0,
                    "officer_name": "$_id.officer_name",
                    "officer_email": "$_id.officer_email",
                    "officer_badge_number": "$_id.officer_badge_number",
                    "total_upvotes": "$total_upvotes"
                }
            },
            {"$sort": {"total_upvotes": -1}},  # Sort by highest upvotes
            {"$limit": 50}  # Limit to top 50 officers
        ]
        results = list(collection_upvotes.aggregate(pipeline))
        return {"status":"success","most_active_officers":results}
    except Exception as e:
        return {"status": "error", "message": str(e)}

@router.post("/add")
async def upvote(upvote: Upvote):
    # Check if this officer has already upvoted this report (async)
    existing_vote = collection_upvotes.find_one({
        "officer_badge_number": upvote.officer_badge_number,
        "report_id": upvote.report_id
    })

    if existing_vote:
        raise HTTPException(status_code=400, detail="Officer has already voted for this report.")

    #Create the upvote document
    upvote_data = {
        "_id": ObjectId(),  # Ensure unique ID
        "officer_name": upvote.officer_name,
        "officer_email": upvote.officer_email,
        "officer_badge_number": upvote.officer_badge_number,
        "report_id": upvote.report_id,
        "upvote_time": datetime.utcnow()
    }

    try:
        # Insert upvote into `upvotes` collection
        collection_upvotes.insert_one(upvote_data)

        # Update the report's upvote count
        collection_reports.update_one(
            {"dr_no": upvote.report_id},
            {
                "$inc": {"upvotes.count": 1},
                "$addToSet": {"upvotes.list": upvote_data}  # Prevent duplicate votes
            },
            upsert=True
        )
        upvote_data["_id"] = str(upvote_data["_id"])

        return {"message": "Upvote cast successfully.", "upvote": upvote_data}

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"An error occurred: {str(e)}")
