# 🚔 NoSQL-LA-CRIME (LAPD Crime Data API)

NoSQL-LA-CRIME is a NoSQL database solution designed to store and manage crime data from the **Los Angeles Police Department (LAPD)**. This project utilizes **MongoDB** as the database and **FastAPI** as the RESTful API framework. 

---

## 📌 Project Overview
This project:
✔ **Stores LAPD Crime Reports** 📂  
✔ **Manages Police Officer Data & Upvotes** 👮  
✔ **Provides REST API for Crime Data Queries** 🌐  
✔ **Optimizes Queries with MongoDB Indexing** 🚀  

---

## 📌 Technologies Used
| Component     | Technology |
|--------------|------------|
| **Database** | MongoDB |
| **Backend API** | FastAPI (Python) |
| **Data Generation** | Faker |


---

## 📌 Features
✅ **Crime Data Storage**: Stores crime reports, including crime codes, time, location, and victim details.  
✅ **Police Officer Upvotes**: Officers can upvote reports to indicate priority.  
✅ **RESTful API**: Supports crime report retrieval, aggregation, and statistical queries.  
✅ **Indexing for Performance**: Optimized queries using MongoDB indexes.  

---

## 📌 Database Schema
The database consists of the following **collections**:

### 1️⃣ Crime Reports (`reports` Collection)
```json
{
  "_id": "ObjectId",
  "dr_no": "202304567",
  "date_rptd": "02/10/2024",
  "date_occ": "02/09/2024",
  "time_occ": "2230",
  "area": 7,
  "area_name": "Newton",
  "crm_codes": {
    "crime_codes": ["440", "561"],
    "crm_cd_desc": "Assault with Deadly Weapon"
  },
  "victim": {
    "vict_age": 34,
    "vict_sex": "M",
    "vict_descent": "H"
  },
  "premis": {
    "premis_cd": "501",
    "premis_desc": "Single Family Residence"
  },
  "weapon": {
    "weapon_used_cd": "101",
    "weapon_desc": "Handgun"
  },
  "location_info": {
    "location": "S Broadway & 77th St",
    "lat": 34.0056,
    "lon": -118.2791
  },
  "status": "IC",
  "status_desc": "Investigation Continues",
  "upvotes": {
    "count": 5,
    "list": [
      {
        "officer_name": "John Doe",
        "officer_email": "johndoe@example.com",
        "officer_badge_number": "12345"
      }
    ]
  }
}
```

### 2️⃣ Police Officers (`officers` Collection)
```json
{
  "_id": "ObjectId",
  "badge_number": "12345",
  "name": "John Doe",
  "email": "johndoe@example.com",
  "rank": "Sergeant",
  "department": "Homicide",
  "date_joined": "2015-06-12",
  "active": true
}
```

### 3️⃣ Upvotes (`upvotes` Collection)
```json
{
  "_id": "ObjectId",
  "officer_name": "John Doe",
  "officer_email": "johndoe@example.com",
  "officer_badge_number": "12345",
  "report_id": "202304567",
  "upvote_time": "2024-02-13T10:30:45.234Z"
}
```

---

## 📌 Installation & Setup
### 1️⃣ Clone Repository
```sh
git clone https://github.com/your-repo/noSQL-LA-CRIME.git
cd noSQL-LA-CRIME
```

### 2️⃣ Install Dependencies
```sh
pip install -r requirements.txt
```

### 3️⃣ Run MongoDB
```sh
mongod --dbpath=data
```

### 4️⃣ Start FastAPI Server
```sh
uvicorn main:app --reload
```
API is now running at: **`http://127.0.0.1:8000/docs`** 🎉

---

## 📌 Contributors
- **Konstantinos Bousinis** 🚀
- **University of Athens, Department of Informatics & Telecommunications** 🎓

---
