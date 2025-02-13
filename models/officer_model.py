from pydantic import BaseModel, Field
from typing import Optional
from datetime import datetime

class PoliceOfficer(BaseModel):
    badge_number: str = Field(..., title="Unique Officer ID (Badge Number)")
    name: str = Field(..., title="Officer Name")
    email: str = Field(..., title="Officer Email")
    rank: Optional[str] = Field(None, title="Officer Rank")
    department: Optional[str] = Field(None, title="Department Name")
    date_joined: Optional[datetime] = Field(default_factory=datetime.utcnow, title="Date Officer Joined")
    active: bool = Field(default=True, title="Is Officer Active?")
