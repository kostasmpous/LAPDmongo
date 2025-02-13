from typing import List, Optional
from pydantic import BaseModel, Field

#class for crime code validation
class CrimeCode(BaseModel):
    crime_codes: List[str]
    crm_cd_desc: str

#class for victim validation
class Victim(BaseModel):
    vict_age: Optional[int]
    vict_sex: str
    vict_descent: str

#class for premis validation
class Premis(BaseModel):
    premis_cd: str
    premis_desc: str

#class for weapon validation
class Weapon(BaseModel):
    weapon_used_cd: Optional[str] = ""
    weapon_desc: Optional[str] = ""

#class for Location validation
class LocationInfo(BaseModel):
    location: str
    lat: float
    lon: float

#class for upvote validation
class Upvote(BaseModel):
    officer_name: str
    officer_email: str
    officer_badge_number: str

#class for upvotes list validation
class Upvotes(BaseModel):
    count: int = 0
    list: List[Upvote] = []

#class for report validation
class Report(BaseModel):
    dr_no: str = Field(..., title="DR Number")
    date_rptd: str = Field(..., title="Date Reported", example="03/01/2020 12:00:00 AM")
    date_occ: str = Field(..., title="Date Occurred", example="03/01/2020 12:00:00 AM")
    time_occ: str = Field(..., title="Time of Occurrence", example="2130")
    area: int = Field(..., title="Area Code")
    area_name: str = Field(..., title="Area Name")
    rpt_dist_no: str = Field(..., title="Report District Number")
    crm_codes: CrimeCode
    mocodes: List[str] = []
    victim: Victim
    premis: Premis
    weapon: Weapon
    location_info: LocationInfo
    status: Optional[str] = ""
    status_desc: Optional[str] = ""
    upvotes: Upvotes = Upvotes()