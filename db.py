from pymongo import MongoClient

# MongoDB connection
connection_string = "mongodb+srv://kostasmpous:C1bLHNuvmSsmZH0U@lapd.t2ysg.mongodb.net/?retryWrites=true&w=majority&appName=Lapd"
client = MongoClient(connection_string)

# Database instance
db = client['lapd']
