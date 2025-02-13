from pydantic import BaseModel, Field
from datetime import datetime

class Upvote(BaseModel):
    report_id: str = Field(..., title="Report DR Number")  # Reference to report
    officer_name: str = Field(..., title="Officer Name")
    officer_email: str = Field(..., title="Officer Email")
    officer_badge_number: str = Field(..., title="Officer Badge Number")
    upvote_time: datetime = Field(default_factory=datetime.utcnow, title="Upvote Timestamp")
