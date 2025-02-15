from db import collection_reports
from fastapi import APIRouter, Query
from datetime import datetime

from models.report_model import Report

router = APIRouter(prefix="/reports", tags=["Reports"])

@router.get("/Query1/")
async def query1(start_time: str, end_time: str):
    if not (start_time.isdigit() and end_time.isdigit() and len(start_time) == 4 and len(end_time) == 4):
        raise ValueError("Invalid time format. Use HHMM (e.g., 0000, 2330).")
    try:
        # Aggregation Pipeline
        pipeline = [
            {
                "$match": {  # Filter reports by time range
                    "time_occ": {"$gte": start_time, "$lte": end_time}
                }
            },
            {
                "$unwind": "$crm_codes.crime_codes"  # Separate each crime code into a new document
            },
            {
                "$group": {  #Count occurrences of each crime code
                    "_id": "$crm_codes.crime_codes",
                    "total_reports": {"$sum": 1}
                }
            },
            {
                "$sort": {"total_reports": -1}  #Sort in descending order
            },
            {
                "$project": {  #Format output
                    "crm_cd": "$_id",
                    "total_reports": 1,
                    "_id": 0
                }
            }
        ]
        results = list(collection_reports.aggregate(pipeline))
        #Format results
        starttm = start_time[:2] + ":" + start_time[2:4]
        endtm = end_time[:2] + ":" + end_time[2:4]
        return {"status": "success","Start_time":starttm,"End_time":endtm, "Number_of_reports_per_crmcd": results}

    except Exception as e:
        return {"status": "error", "message": str(e)}

@router.get("/Query2/")
def query2(
        crm_cd: str = Query(..., description="Crime Code (e.g., 510 for Vehicle Theft)"),
        start_time: str = Query(..., description="Start of time range (e.g., 1200 for 12:00)"),
        end_time: str = Query(..., description="End of time range (e.g., 1800 for 18:00)")
):
    try:
        # Validate time format (ensure it is four digits)
        if not (start_time.isdigit() and end_time.isdigit() and len(start_time) == 4 and len(end_time) == 4):
            raise ValueError("Invalid time format. Use HHMM (e.g., 0000, 2330).")

        # Aggregation Pipeline
        pipeline = [
            {
                "$match": {  # Filter by crime code and time range
                    "crm_codes.crime_codes": crm_cd,
                    "time_occ": {"$gte": start_time, "$lte": end_time}
                }
            },
            {
                "$group": {  # Group by date and count reports
                    "_id": "$date_occ",
                    "total_reports": {"$sum": 1}
                }
            },
            {
                "$sort": {"_id": 1}  # Sort by date (ascending)
            },
            {
                "$project": {  # Format output
                    "date_occ": "$_id",
                    "total_reports": 1,
                    "_id": 0
                }
            }
        ]

        # Execute Query
        results = list(collection_reports.aggregate(pipeline))
        starttm = start_time[:2] + ":" + start_time[2:4]
        endtm = end_time[:2] + ":" + end_time[2:4]
        return {"status": "success","crm_cd":crm_cd,"start_time":starttm,"end_time":endtm,f"Number of reports per day for {crm_cd}": results}

    except Exception as e:
        return {"status": "error", "message": str(e)}

@router.get("/Query3/")
def query3(
        date:str
):
    try:
        # Convert input date to match stored format (e.g., "03/01/2020 12:00:00 AM")
        formatted_date = datetime.strptime(date, "%m/%d/%Y").strftime("%m/%d/%Y")

        # Aggregation Pipeline
        pipeline = [
            {
                "$match": {
                    "date_occ": {"$regex": f"^{formatted_date}"}  # Match specific day
                }
            },
            {
                "$unwind": "$crm_codes.crime_codes"  # Separate each crime code into a new document
            },
            {
                "$group": {  # Count occurrences of each crime per area
                    "_id": {"area_name": "$area_name", "crime": "$crm_codes.crime_codes"},
                    "count": {"$sum": 1}
                }
            },
            {
                "$sort": {"count": -1}  # Sort by frequency (descending)
            },
            {
                "$group": {  # Group by area and keep top 3 crimes
                    "_id": "$_id.area_name",
                    "top_crimes": {"$push": {"crime_code": "$_id.crime", "count": "$count"}}
                }
            },
            {
                "$project": {  # Limit to top 3 crimes per area
                    "area_name": "$_id",
                    "top_crimes": {"$slice": ["$top_crimes", 3]},
                    "_id": 0
                }
            }
        ]

        # Execute Query
        results = list(collection_reports.aggregate(pipeline))
        return {"status":"success","date":date,"Three_most_common_crimes": results}
    except Exception as e:
        return {"status": "error", "message": str(e)}

@router.get("/Query4/")
def query4(start_time: str, end_time: str):
    try:
        # Validate time format (ensure it is four digits)
        if not (start_time.isdigit() and end_time.isdigit() and len(start_time) == 4 and len(end_time) == 4):
            raise ValueError("Invalid time format. Use HHMM (e.g., 0000, 2330).")
        # Aggregation Pipeline
        pipeline = [
            {
                "$match": {  # Filter reports by time range
                    "time_occ": {"$gte": start_time, "$lte": end_time}
                }
            },
            {
                "$unwind": "$crm_codes.crime_codes"  # Separate each crime code into a new document
            },
            {
                "$group": {  # Count occurrences of each crime
                    "_id": "$crm_codes.crime_codes",
                    "total_reports": {"$sum": 1}
                }
            },
            {
                "$sort": {"total_reports": 1}  #  Sort in ascending order (least common first)
            },
            {
                "$limit": 2  #  Keep only the 2 least common crimes
            },
            {
                "$project": {  #  Format output
                    "crm_cd": "$_id",
                    "total_reports": 1,
                    "_id": 0
                }
            }
        ]

        # Execute Query
        results = list(collection_reports.aggregate(pipeline))
        starttm = start_time[:2] + ":" + start_time[2:4]
        endtm = end_time[:2] + ":" + end_time[2:4]
        return {"status":"success","start_time":starttm,"end_time":endtm,"Two_least_common_crimes":results}
    except Exception as e:
        return {"status": "error", "message": str(e)}

@router.get("/Query5/")
def query5():
    try:
        pipeline = [
            {"$unwind": "$crm_codes.crime_codes"},  # Separate each crime code
            {
                "$group": {  # Group by crime code, weapon, and area
                    "_id": {
                        "crm_cd": "$crm_codes.crime_codes",
                        "weapon_desc": "$weapon.weapon_desc",
                        "area_name": "$area_name"
                    },
                    "count": {"$sum": 1}
                }
            },
            {
                "$group": {  # Collect all areas for each crime-weapon combination
                    "_id": {
                        "crm_cd": "$_id.crm_cd",
                        "weapon_desc": "$_id.weapon_desc"
                    },
                    "areas": {"$addToSet": "$_id.area_name"},  #  List of distinct areas
                    "total_reports": {"$sum": "$count"}
                }
            },
            {
                "$match": {
                    "areas.1": {"$exists": True},  # Ensure at least two different areas exist
                    "_id.weapon_desc": {"$ne": ""}  #  Ignore empty weapon descriptions
                }
            },
            {
                "$project": {
                    "_id": 0,
                    "crm_cd": "$_id.crm_cd",
                    "weapon_desc": "$_id.weapon_desc",
                    "area_count": {"$size": "$areas"},  # Count distinct areas
                    "total_reports": 1  # Total number of reports for the crime-weapon
                }
            },
            {
                "$sort": {"area_count": -1}  #  Sort by number of areas (descending)
            }
        ]

        results = list(collection_reports.aggregate(pipeline))
        return {"status":"success","Weapons_used_for_same_crime":results}
    except Exception as e:
        return {"status": "error", "message": str(e)}

@router.get("/Query6/")
def query6(date:str):
    try:
        # Convert date string to datetime object
        specific_date = datetime.strptime(date, "%m/%d/%Y").strftime("%m/%d/%Y")

        reports = collection_reports.find(
            {"date_occ": {"$regex": f"^{specific_date}"}},
            {"_id": 0, "dr_no": 1, "date_occ": 1, "area_name": 1, "upvotes.count": 1}  # Select only necessary fields
        ).sort("upvotes.count", -1).limit(50)
        # Execute Query
        results = [dict(report) for report in reports]
        return {"status":"success","date": date, "top_50_upvoted_reports": results}
    except Exception as e:
        return {"status": "error", "message": str(e)}



@router.get("/Query8/")
def query8():
    try:
        pipeline = pipeline = [
            {
                "$unwind": "$upvotes.list"  # Unwind upvotes to process each officer separately
            },
            {
                "$group": {  # Group by officer and collect unique areas
                    "_id": {
                        "officer_name": "$upvotes.list.officer_name",
                        "officer_email": "$upvotes.list.officer_email",
                        "officer_badge_number": "$upvotes.list.officer_badge_number"
                    },
                    "distinct_areas": {"$addToSet": "$area_name"}  #  Collect unique areas
                }
            },
            {
                "$project": {  # Count the number of distinct areas
                    "_id": 0,
                    "officer_name": "$_id.officer_name",
                    "officer_email": "$_id.officer_email",
                    "officer_badge_number": "$_id.officer_badge_number",
                    "area_count": {"$size": "$distinct_areas"},
                    "distinct_areas": 1
                }
            },
            {
                "$sort": {"area_count": -1}  # Sort by number of unique areas in descending order
            },
            {
                "$limit": 50  # Limit results to top 50 officers
            }
        ]

        results = list(collection_reports.aggregate(pipeline))
        return {"status":"success","Top_officers_according_to_total_areas":results}
    except Exception as e:
        return {"status": "error", "message": str(e)}

@router.get("/Query9/")
def query9():
    try:
        pipeline = [
            {
                "$unwind": "$upvotes.list"  # Separate each upvote into individual documents
            },
            {
                "$group": {  # Group by officer email and badge number
                    "_id": {
                        "officer_email": "$upvotes.list.officer_email",
                        "officer_badge_number": "$upvotes.list.officer_badge_number"
                    },
                    "reports": {"$addToSet": "$dr_no"}  # Collect report numbers (DR numbers)
                }
            },
            {
                "$group": {  # Group again by email, collecting badge numbers
                    "_id": "$_id.officer_email",
                    "badge_numbers": {"$addToSet": "$_id.officer_badge_number"},
                    "reports": {"$push": "$reports"}  # Collect all related reports
                }
            },
            {
                "$match": {  # Ensure the email is linked to more than one badge number
                    "badge_numbers.1": {"$exists": True}
                }
            },
            {
                "$project": {  # Format output
                    "_id": 0,
                    "officer_email": "$_id",
                    "badge_numbers": 1,
                    "reports": {
                        "$reduce": {  # Flatten the list of reports
                            "input": "$reports",
                            "initialValue": [],
                            "in": {"$setUnion": ["$$value", "$$this"]}
                        }
                    }
                }
            }
        ]

        results = list(collection_reports.aggregate(pipeline))
        return {"status":"success","Report_with_same_email_different_badge":results}
    except Exception as e:
        return {"status": "error", "message": str(e)}

@router.get("/Query10/")
def query10(officer_name : str):
    try:
        pipeline = [
            {
                "$match": {  # Filter reports where officer has upvoted
                    "upvotes.list.officer_name": officer_name
                }
            },
            {
                "$group": {  # Collect distinct areas
                    "_id": None,
                    "areas": {"$addToSet": "$area_name"}
                }
            },
            {
                "$project": {  # Format output
                    "_id": 0,
                    "officer_name": officer_name,
                    "areas": 1
                }
            }
        ]
        results = list(collection_reports.aggregate(pipeline))
        return {"status":"success","Areas_voted":results}
    except Exception as e:
        return {"status": "error", "message": str(e)}

@router.post("/add/")
async def add_report(report : Report):
    report_data = report.model_dump()
    result = collection_reports.insert_one(report_data)
    if not result.inserted_id:
        return {"status": "error", "message": "Failed to insert report."}
    return {"status": "ok", "message": "Report inserted successfully.","Dr_no": report.dr_no}
