from fastapi import FastAPI, Query
from routers import users, reports  # Import your routers
from pymongo import MongoClient
app = FastAPI()
app.include_router(reports.router)
# Include routers
app.include_router(users.router)
client = MongoClient("mongodb://localhost:27017/", directConnection=True)
db = client["lapd"]
collection = db["reports"]

@app.on_event("startup")
async def startup_event():
    print("Application startup: MongoDB connection initialized.")

@app.on_event("shutdown")
async def shutdown_event():
    print("Application shutdown: Closing MongoDB connection.")
