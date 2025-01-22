from fastapi import APIRouter
from db import db

router = APIRouter()

@router.get("/users")
async def get_users():
    users = db['users']
    return list(users.find())
