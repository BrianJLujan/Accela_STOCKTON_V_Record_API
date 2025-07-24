from fastapi import FastAPI, Depends, HTTPException, Query
from fastapi.security import HTTPBasic, HTTPBasicCredentials
from pydantic import BaseModel
from typing import List, Optional
from datetime import datetime
import pyodbc
import os
import secrets
from dotenv import load_dotenv

load_dotenv()

app = FastAPI()
security = HTTPBasic()

USERNAME = os.getenv("API_USER")
PASSWORD = os.getenv("API_PASS")

# Database connection
conn = pyodbc.connect(
    f"DRIVER={{ODBC Driver 17 for SQL Server}};"
    f"SERVER={os.getenv('SQL_SERVER')};"
    f"DATABASE={os.getenv('SQL_DB')};"
    f"UID={os.getenv('SQL_USER')};"
    f"PWD={os.getenv('SQL_PASS')}"
)

# Pydantic model
class Permit(BaseModel):
    AGENCY_ID: str
    RECORD_ID: Optional[str]
    RECORD_MODULE: str
    RECORD_NAME: Optional[str]
    RECORD_OPEN_DATE: Optional[datetime]
    RECORD_STATUS: Optional[str]
    RECORD_STATUS_DATE: Optional[datetime]
    RECORD_TYPE: Optional[str]
    UPDATED_BY: str
    ACA_INITIATED: Optional[str]
    ADDR_FULL_LINE_HASH: Optional[str]
    ADDR_FULL_LINE1_HASH: Optional[str]
    ASSIGNED_USERID: Optional[str]
    BALANCE_DUE: Optional[float]
    BUILDING_COUNT: Optional[int]
    CLOSED_USERID: Optional[str]
    COMPLETED_USERID: Optional[str]
    CONST_TYPE_CODE: Optional[str]
    DATE_ASSIGNED: Optional[datetime]
    DATE_CLOSED: Optional[datetime]
    DATE_COMPLETED: Optional[datetime]
    DATE_OPENED: Optional[datetime]
    DATE_OPENED_ORIGINAL: datetime
    DATE_STATUS: Optional[datetime]
    DATE_TRACK_START: Optional[datetime]
    DESCRIPTION: Optional[str]
    HOUSING_UNITS: Optional[int]
    IN_POSSESSION_HRS: Optional[float]
    INSPECTOR_USERID: Optional[str]
    JOB_VALUE: Optional[float]
    JOB_VALUE_CALCULATED: Optional[float]
    JOB_VALUE_CONTRACTOR: Optional[float]
    OFFICER_USERID: Optional[str]
    OPENED_USERID: Optional[str]
    PARENT_RECORD_ID_HASH: Optional[str]
    PERCENT_COMPLETE: Optional[float]
    PRIORITY: Optional[str]
    PUBLIC_OWNED: Optional[str]
    RECORD_AGE: Optional[int]
    RECORD_OPEN_HRS: Optional[float]
    RECORD_TYPE_4LEVEL_HASH: str
    RECORD_TYPE_CATEGORY: str
    RECORD_TYPE_GROUP: str
    RECORD_TYPE_SUBTYPE: str
    RECORD_TYPE_TYPE: str
    REPORTED_CHANNEL: Optional[str]
    SHORT_NOTES: Optional[str]
    STATUS: Optional[str]
    TOTAL_INVOICED: Optional[float]
    TOTAL_PAID: Optional[float]
    TRUST_ACCOUNT_BAL: Optional[float]
    TRUST_ACCOUNT_DESC: Optional[str]
    TRUST_ACCOUNT_ID_PRI: Optional[str]
    TRUST_ACCOUNT_STATUS: Optional[str]
    TEMPLATE_ID: str
    T_ID1: str
    T_ID2: str
    T_ID3: str
    STREET_NBR_ALPHA_HASH: Optional[str]

# Authentication
def authenticate(credentials: HTTPBasicCredentials = Depends(security)):
    if not (secrets.compare_digest(credentials.username, USERNAME) and
            secrets.compare_digest(credentials.password, PASSWORD)):
        raise HTTPException(status_code=401, detail="Unauthorized")

# GET /permits endpoint
@app.get("/permits", response_model=List[Permit])
def get_permits(
    credentials: HTTPBasicCredentials = Depends(authenticate),
    limit: int = Query(100, le=1000)
):
    cursor = conn.cursor()
    cursor.execute(f"""
        SELECT TOP {limit}
            AGENCY_ID, RECORD_ID, RECORD_MODULE, RECORD_NAME, RECORD_OPEN_DATE, RECORD_STATUS,
            RECORD_STATUS_DATE, RECORD_TYPE, UPDATED_BY, ACA_INITIATED,
            [ADDR_FULL_LINE#] AS ADDR_FULL_LINE_HASH,
            [ADDR_FULL_LINE1#] AS ADDR_FULL_LINE1_HASH,
            ASSIGNED_USERID, BALANCE_DUE, BUILDING_COUNT, CLOSED_USERID,
            COMPLETED_USERID, CONST_TYPE_CODE, DATE_ASSIGNED, DATE_CLOSED, DATE_COMPLETED,
            DATE_OPENED, DATE_OPENED_ORIGINAL, DATE_STATUS, DATE_TRACK_START, DESCRIPTION,
            HOUSING_UNITS, IN_POSSESSION_HRS, INSPECTOR_USERID, JOB_VALUE, JOB_VALUE_CALCULATED,
            JOB_VALUE_CONTRACTOR, OFFICER_USERID, OPENED_USERID,
            [PARENT_RECORD_ID#] AS PARENT_RECORD_ID_HASH,
            PERCENT_COMPLETE, PRIORITY, PUBLIC_OWNED, RECORD_AGE, RECORD_OPEN_HRS,
            [RECORD_TYPE_4LEVEL#] AS RECORD_TYPE_4LEVEL_HASH,
            RECORD_TYPE_CATEGORY, RECORD_TYPE_GROUP, RECORD_TYPE_SUBTYPE,
            RECORD_TYPE_TYPE, REPORTED_CHANNEL, SHORT_NOTES, STATUS, TOTAL_INVOICED, TOTAL_PAID,
            TRUST_ACCOUNT_BAL, TRUST_ACCOUNT_DESC, TRUST_ACCOUNT_ID_PRI, TRUST_ACCOUNT_STATUS,
            TEMPLATE_ID, T_ID1, T_ID2, T_ID3,
            [STREET_NBR_ALPHA#] AS STREET_NBR_ALPHA_HASH
        FROM V_RECORD
        ORDER BY RECORD_OPEN_DATE DESC
    """)
    columns = [col[0] for col in cursor.description]
    rows = cursor.fetchall()
    return [dict(zip(columns, row)) for row in rows]