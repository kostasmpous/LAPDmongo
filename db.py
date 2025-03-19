from pymongo import MongoClient

#MongoDM Community localhost
client = MongoClient("mongodb://localhost:27017/")

# Database instance
db = client['lapd']

# Define JSON Schema for the collection
collection_reports_name = "reports"
collection_reports = db[collection_reports_name]

collection_upvotes_name = "upvotes"
collection_upvotes = db[collection_upvotes_name]

collection_officer_name = "officer"
collection_officers = db[collection_officer_name]
