from pymongo import MongoClient, ASCENDING, DESCENDING

# Connect to MongoDB
client = MongoClient("mongodb://localhost:27017/")
db = client["lapd"]

# Collections
crime_reports = db["reports"]
upvotes = db["upvotes"]
police_officers = db["officer"]

# 📌 1. Indexes for `crime_reports` collection
crime_reports.create_index([("crm_codes.crime_codes", ASCENDING), ("date_occ", ASCENDING)])  # Crime code & date
crime_reports.create_index([("area", ASCENDING), ("date_occ", ASCENDING)])  # Area & date for location-based searches
crime_reports.create_index([("date_occ", ASCENDING)])  # Fast searches by occurrence date
crime_reports.create_index([("weapon.weapon_used_cd", ASCENDING), ("crm_codes.crime_codes", ASCENDING), ("area", ASCENDING)])  # Find weapon usage per crime & area

# 📌 2. Indexes for `upvotes` collection
upvotes.create_index([("report_id", ASCENDING), ("upvote_time", ASCENDING)])  # Find most upvoted reports on a specific day
upvotes.create_index([("officer_badge_number", ASCENDING)])  # Find most active officers
upvotes.create_index([("officer_badge_number", ASCENDING), ("report_id", ASCENDING)], unique=True)  # Prevent duplicate upvotes
upvotes.create_index([("officer_email", ASCENDING), ("officer_badge_number", ASCENDING)])  # Detect multiple badge numbers using same email
upvotes.create_index([("officer_badge_number", ASCENDING), ("report_id", ASCENDING)])  # Optimize officer upvotes per report

# 📌 3. Indexes for `police_officers` collection
police_officers.create_index([("badge_number", ASCENDING)], unique=True)  # Unique badge numbers
police_officers.create_index([("name", ASCENDING)])  # Search officers by name
police_officers.create_index([("department", ASCENDING)])  # Filter by department
police_officers.create_index([("date_joined", DESCENDING)])  # Find recently joined officers
police_officers.create_index([("active", ASCENDING)])  # Filter active/inactive officers

print("Indexes created successfully! 🚀")
