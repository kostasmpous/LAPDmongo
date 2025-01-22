from fastapi import FastAPI
from routers import users  # Import your routers
from db import db  # Import the database instance

app = FastAPI()

# Include routers
app.include_router(users.router)

@app.on_event("startup")
async def startup_event():
    print("Application startup: MongoDB connection initialized.")

@app.on_event("shutdown")
async def shutdown_event():
    print("Application shutdown: Closing MongoDB connection.")
