from fastapi import FastAPI, Depends, HTTPException
from fastapi.security import HTTPBasic, HTTPBasicCredentials
from pydantic import BaseModel
from typing import List
import pyodbc
import os
import secrets
from dotenv import load_dotenv

load_dotenv()

app = FastAPI()
security = HTTPBasic()

USERNAME = os.getenv("API_USER")
PASSWORD = os.getenv("API_PASS")

conn = pyodbc.connect(
    f"DRIVER={{ODBC Driver 17 for SQL Server}};"
    f"SERVER={os.getenv('SQL_SERVER')};"
    f"DATABASE={os.getenv('SQL_DB')};"
    f"UID={os.getenv('SQL_USER')};"
    f"PWD={os.getenv('SQL_PASS')}"
)

class Permit(BaseModel):
    RECORD_ID: str
    RECORD_TYPE: str
    RECORD_STATUS: str
    RECORD_OPEN_DATE: str
    RECORD_AGE: int
    ASSIGNED_USERID: str

def authenticate(credentials: HTTPBasicCredentials = Depends(security)):
    if not (secrets.compare_digest(credentials.username, USERNAME) and
            secrets.compare_digest(credentials.password, PASSWORD)):
        raise HTTPException(status_code=401, detail="Unauthorized")

@app.get("/permits", response_model=List[Permit])
def get_permits(credentials: HTTPBasicCredentials = Depends(authenticate)):
    cursor = conn.cursor()
    cursor.execute("""
        SELECT TOP 100 
            RECORD_ID, RECORD_TYPE, RECORD_STATUS, 
            RECORD_OPEN_DATE, RECORD_AGE, ASSIGNED_USERID
        FROM V_RECORD
        WHERE RECORD_STATUS = 'In Review'
        ORDER BY RECORD_OPEN_DATE DESC
    """)
    columns = [column[0] for column in cursor.description]
    return [dict(zip(columns, row)) for row in cursor.fetchall()]
