from pymongo import MongoClient
import db_utils

# MongoDB connection to cloud MongoAtlas
#connection_string = "mongodb+srv://kostasmpous:C1bLHNuvmSsmZH0U@lapd.t2ysg.mongodb.net/?retryWrites=true&w=majority&appName=Lapd"
#client = MongoClient(connection_string,tlsCAFile=certifi.where())

#MongoDM Community localhost
client = MongoClient("mongodb://localhost:27017/")
db_utils.generate_officers()

# Database instance
db = client['lapd']

# Define JSON Schema for the collection
collection_name = "reports"
collection = db[collection_name]