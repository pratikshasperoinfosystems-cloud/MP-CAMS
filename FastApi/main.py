# python -m uvicorn main:app --host 0.0.0.0 --port 8000 --reload '''



from fastapi import FastAPI, WebSocket, WebSocketDisconnect, Query, HTTPException
from database import database
from database2 import database2

from databases import Database
import asyncio
from datetime import timedelta
from datetime import datetime
import json
from fastapi import APIRouter
from typing import List, Optional
from aiokafka import AIOKafkaProducer, AIOKafkaConsumer
import hashlib
from pydantic import BaseModel
import decimal
from decimal import Decimal
import json
# import aioredis
from redis import asyncio as redis
from fastapi.encoders import jsonable_encoder
from fastapi.responses import StreamingResponse
import pandas as pd
import io
from datetime import datetime
from openpyxl.styles import Font, PatternFill, Alignment
import time, asyncio

from fastapi.middleware.cors import CORSMiddleware
from contextlib import asynccontextmanager

import pytz
from fastapi import FastAPI, Depends, HTTPException, WebSocket
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from jose import jwt, JWTError
from datetime import datetime, timedelta


import os
from dotenv import load_dotenv
load_dotenv() 
SECRET_KEY = os.getenv("SECRET_KEY")
ALGORITHM = "HS256"

security = HTTPBearer()



ist = pytz.timezone("Asia/Kolkata")
now = datetime.now(ist)
formatted_time = now.strftime("%Y-%m-%d %H:%M:%S")
print(formatted_time)



# ---------- Lifespan (pehle define karo) ----------
@asynccontextmanager
async def lifespan(app: FastAPI):           # ✅ type hint sahi
    # ---------- STARTUP ----------
    await database.connect()
    await database2.connect()
    await init_redis()

    global alert_worker_task, notifier_task
    alert_worker_task = asyncio.create_task(rtm_alert_insert_worker())
    notifier_task = asyncio.create_task(alert_ws_notifier())
    logger.info("✅ App STARTED — workers running")

    yield

    # ---------- SHUTDOWN ----------
    for t in [alert_worker_task, notifier_task]:
        if t:
            t.cancel()
            try:
                await t
            except asyncio.CancelledError:
                pass

    if redis_client:
        await redis_client.aclose()
    if database.is_connected:
        await database.disconnect()
    if database2.is_connected:
        await database2.disconnect()
    logger.info("🛑 App STOPPED")

app = FastAPI(lifespan=lifespan)


router = APIRouter()

# app = FastAPI()

class LoginRequest(BaseModel):
    username: str
    password: str


class LogoutRequest(BaseModel):
    username: str

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(router)


        
        
_cache = {}
_cache_expiry = {}

# Global Redis cache (shared)
# redis = None
redis_client = None

async def init_redis():
    global redis_client

    if redis_client is None:
        redis_client = redis.from_url(
            "redis://localhost",
            encoding="utf-8",
            decode_responses=True,
        )

# async def cached_query(sql, params=None, ttl=15, fetch="all", db=database):
#     """
#     Run cached query with support for multiple databases.
    
#     Args:
#         sql (str): SQL query
#         params (tuple/dict): query params
#         ttl (int): cache expiry in seconds
#         fetch (str): "all" or "one"
#         db (Database): which database object to use (default = database)
#     """
#     key = (id(db), sql, str(params), fetch)  # ✅ include db id in key
#     now = time.time()

#     if key in _cache and now < _cache_expiry[key]:
#         return _cache[key]

#     if fetch == "one":
#         result = await db.fetch_one(sql, params)
#     else:
#         result = await db.fetch_all(sql, params)

#     _cache[key] = result
#     _cache_expiry[key] = now + ttl
#     return result
MAX_CACHE_ENTRIES = 2000

async def cached_query(sql, params=None, ttl=15, fetch="all", db=database):
    """
    Redis + Memory hybrid cache
    WebSocket code unchanged
    """
    await init_redis()

    # 🔑 Compact & stable cache key
    key_data = {
        "db": id(db),
        "sql": sql,
        "params": params,
        "fetch": fetch,
    }
    raw_key = json.dumps(key_data, sort_keys=True)
    cache_key = "cache_query:" + hashlib.md5(raw_key.encode()).hexdigest()

    now = time.time()

    # ------------------------------
    # 1️⃣ Local memory cache
    # ------------------------------
    if cache_key in _cache and now < _cache_expiry.get(cache_key, 0):
        return _cache[cache_key]

    # ------------------------------
    # 2️⃣ Redis cache
    # ------------------------------
    try:
        # print("REDIS BEFORE GET")
        redis_data = await redis_client.get(cache_key)
        # print("REDIS AFTER GET")

        if redis_data:
            result = json.loads(redis_data)
            _cache[cache_key] = result
            _cache_expiry[cache_key] = now + ttl
            return result
    except Exception as e:
        print("Redis error, fallback to DB:", e)

    # ------------------------------
    # 3️⃣ DB query (ONLY ONCE)
    # ------------------------------
    if fetch == "one":
        row = await db.fetch_one(sql, params)
        result = dict(row) if row else None
    else:
        rows = await db.fetch_all(sql, params)
        result = [dict(r) for r in rows]

    # ------------------------------
    # 4️⃣ Save cache (best effort)
    # ------------------------------
    _cache[cache_key] = result
    _cache_expiry[cache_key] = now + ttl

    try:
        await redis_client.set(
            cache_key,
            json.dumps(result, default=str),
            ex=ttl,
        )
    except Exception:
        pass

    return result

# async def cached_query(sql, params=None, ttl=15):
#     key = (sql, str(params))
#     now = time.time()
#     if key in _cache and now < _cache_expiry[key]:
#         return _cache[key]
#     result = await database.fetch_all(sql, params)
#     _cache[key] = result
#     _cache_expiry[key] = now + ttl
#     return result


# async def cached_query(sql, params=None, ttl=15, fetch="all"):
#     key = (sql, str(params), fetch)
#     now = time.time()
#     if key in _cache and now < _cache_expiry[key]:
#         return _cache[key]

#     if fetch == "one":
#         result = await database.fetch_one(sql, params)
#     else:
#         result = await database.fetch_all(sql, params)

#     _cache[key] = result
#     _cache_expiry[key] = now + ttl
#     return result
        
# connected_clients: dict[str, List[WebSocket]] = {}
        
connected_clients: dict[str, List[WebSocket]] = {}





#======================== Dashbords Websoket ==============================================

def generate_token(user_id: str):
    payload = {
        "sub": user_id,
        "exp": datetime.utcnow() + timedelta(hours=24)
    }
    return jwt.encode(payload, SECRET_KEY, algorithm=ALGORITHM)

def verify_token(
    creds: HTTPAuthorizationCredentials = Depends(security)
):
    try:
        payload = jwt.decode(
            creds.credentials,
            SECRET_KEY,
            algorithms=[ALGORITHM]
        )
        return payload["sub"]
    except JWTError:
        raise HTTPException(status_code=401, detail="Invalid token")
    
async def verify_jwt_token(token: str):
    if not token:
        return None

    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
        user_id = payload.get("sub")

        row = await database.fetch_one(
            "SELECT clg_is_login FROM ems_colleague WHERE clg_ref_id = :u",
            {"u": user_id}
        )

        if not row or row["clg_is_login"] != "yes":
            return None

        return user_id
    except JWTError:
        return None


# ✅ Login API
@app.post("/login")
async def login_user(data: LoginRequest):
    username = data.username
    password = data.password
    password_md5 = hashlib.md5(password.encode()).hexdigest()

    query = """
        SELECT clg_group, clg_is_login
        FROM ems_colleague
        WHERE clg_ref_id = :username AND clg_password = :password
    """
    result = await database.fetch_one(query, {"username": username, "password": password_md5})

    if not result:
        raise HTTPException(status_code=401, detail="Invalid username or password")

    # if result["clg_group"] != "UG-NEW-DASHBOARD":
    #     raise HTTPException(status_code=403, detail="You're not a Dashboard user. This is a Dashboard login. Please contact Super Admin.")

    update_query = """
        UPDATE ems_colleague SET clg_is_login = 'yes' WHERE clg_ref_id = :username
    """
    await database.execute(update_query, {"username": username})
    token = generate_token(username)

    return {"message": "Login successful", "status": "success", "username": username, "token": token}


# ✅ Logout API
# @app.post("/logout")
# async def logout_user(data: LogoutRequest):
#     username = data.username

#     query = """
#         SELECT clg_ref_id FROM ems_colleague WHERE clg_ref_id = :username
#     """
#     result = await database.fetch_one(query, {"username": username})

#     if not result:
#         raise HTTPException(status_code=404, detail="User not found")

#     update_query = """
#         UPDATE ems_colleague SET clg_is_login = 'no' WHERE clg_ref_id = :username
#     """
#     await database.execute(update_query, {"username": username})

#     return {"message": "Logout successful", "status": "success"}

@app.post("/logout")
async def logout_user(user_id: str = Depends(verify_token)):

    update_query = """
        UPDATE ems_colleague
        SET clg_is_login = 'no'
        WHERE clg_ref_id = :username
    """
    await database.execute(update_query, {"username": user_id})

    return {
        "message": "Logout successful",
        "status": "success"
    }


def format_seconds_to_mmss(total_seconds):
    minutes = int(total_seconds // 60)
    seconds = int(total_seconds % 60)
    return f"{minutes:02}:{seconds:02}"

        
        
def format_seconds_to_hhmmss(total_seconds):
    t = timedelta(seconds=int(total_seconds))
    return str(t)


        
        
        
        
from typing import Dict        
import zlib
import json


        

from pydantic import BaseModel
from typing import List




class DistrictOut(BaseModel):
    district_id: int
    district_name: str

@app.get("/api/districts", response_model=List[DistrictOut])
async def get_districts(division_id: Optional[int] = Query(None)):
    if division_id is not None:
        query = """
            SELECT dst_code, dst_name
            FROM ems_mas_districts
            WHERE div_id = :division_id
            AND dst_state='MH'
            AND dstis_deleted = '0'
            ORDER BY dst_name
        """
        # rows = await database.fetch_all(query, values={"division_id": division_id})
        rows = await cached_query(query, {"division_id": division_id}, ttl=10)
    else:
        query = """
            SELECT dst_code, dst_name
            FROM ems_mas_districts
            WHERE dst_state='MH'
            AND dstis_deleted = '0'
            ORDER BY dst_name
        """
        # rows = await database.fetch_all(query)
        rows = await cached_query(query, ttl=10)

    districts = [
        DistrictOut(
            district_id=row["dst_code"],
            district_name=row["dst_name"]
        )
        for row in rows
    ]
    return districts



class DivisionOut(BaseModel):
    division_id: int
    division_name: str

@app.get("/api/division", response_model=List[DivisionOut])
async def get_districts():
    query = """
        SELECT div_code, div_name
        FROM ems_mas_division
        ORDER BY div_name
    """
    rows = await database.fetch_all(query)

    division = [
        DivisionOut(
            division_id=row["div_code"],
            division_name=row["div_name"]
        )
        for row in rows
    ]
    return division


from datetime import datetime, date

def normalize_row(row):
    """
    Convert DB row (Record, tuple, or dict) into plain dictionary.
    Handles datetime/date conversion for JSON serialization.
    """
    if not row:
        return {}

    # If it's a databases Record (like from databases library)
    try:
        data = dict(row._mapping)
    except AttributeError:
        data = row if isinstance(row, dict) else {}

    # Convert all values safely (including datetime/date)
    normalized = {}
    for k, v in data.items():
        if v is None:
            normalized[k] = None
        elif isinstance(v, (datetime, date)):
            normalized[k] = v.strftime("%Y-%m-%d %H:%M:%S")
        else:
            normalized[k] = str(v)
    return normalized



@app.get("/api/ambulance-list")
async def get_ambulance_list():
    query = """
        SELECT
            a.amb_rto_register_no AS ambulance_number,
            d.dst_name AS district_name,
            a.amb_district AS dst_code
        FROM ems_ambulance a
        LEFT JOIN ems_mas_districts d
            ON a.amb_district = d.dst_code
        WHERE a.amb_rto_register_no IS NOT NULL
        ORDER BY d.dst_name, a.amb_rto_register_no;
    """

    rows = await cached_query(query, ttl=30, fetch="all")
    if not rows:
        return {"status": "error", "message": "No ambulances found"}

    result = [
        {
            "ambulance_number": row["ambulance_number"],
            "district_name": row["district_name"],
            "dst_code": row["dst_code"]
        }
        for row in rows
    ]

    return {"status": "success", "count": len(result), "data": result}



# @app.websocket("/ws/rtm_dashboard")
# async def rtm_dashboard_ws(websocket: WebSocket):
#     await websocket.accept()
#     prev_data = None
#     last_filter = {}  # 👈 store last filter

#     try:
#         while True:
#             # ✅ Try receiving new filter (optional)
#             try:
#                 msg = await asyncio.wait_for(websocket.receive_json(), timeout=2)
#                 if msg:  # only update if frontend sends something
#                     last_filter = msg
#             except asyncio.TimeoutError:
#                 msg = {}  # no new message in this cycle

#             # ✅ Use last_filter if available
#             filters = last_filter or {}

#             inc_start_date = filters.get("start_date")
#             inc_end_date = filters.get("end_date")
#             district = filters.get("dst_code")
#             ambulance_no = filters.get("ambulance_no")
#             inc_ref_id = filters.get("inc_ref_id")

#             # ✅ Build dynamic WHERE clause
#             where_clauses = []
#             params = {}

#             if inc_start_date and inc_end_date:
#                 where_clauses.append(
#                     "STR_TO_DATE(inc_datetime, '%Y-%m-%d %H:%i:%s') BETWEEN STR_TO_DATE(:start_date, '%Y-%m-%d') AND STR_TO_DATE(:end_date, '%Y-%m-%d')"
#                 )
#                 params["start_date"] = inc_start_date
#                 params["end_date"] = inc_end_date

#             if district:
#                 where_clauses.append("dst_code = :district")
#                 params["district"] = district

#             if ambulance_no:
#                 where_clauses.append("ambulance_no = :ambulance_no")
#                 params["ambulance_no"] = ambulance_no

#             if inc_ref_id:
#                 where_clauses.append("inc_ref_id = :inc_ref_id")
#                 params["inc_ref_id"] = inc_ref_id

#             where_sql = f"WHERE {' AND '.join(where_clauses)}" if where_clauses else ""

#             # ✅ Query
#             query = f"""
#                 SELECT 
#                     inc_ref_id,
#                     ambulance_no,
#                     dst_code,
#                     district_name,
#                     base_location_name,
#                     call_type,
#                     caller_mobile,
#                     pilot_name,
#                     pilot_mobile,
#                     paramedic_name,
#                     paramedic_mobile,
#                     assigned_time,
#                     parameter_count,
#                     inc_dispatch_time,
#                     inc_recive_time,
#                     inc_datetime,
#                     acknowledge,
#                     start_from_base_loc,
#                     acknowledge_duration,
#                     start_from_base_duration,
#                     at_scene,
#                     at_scene_duration,
#                     wait_time_at_scene_duration,
#                     from_scene,
#                     start_from_scene_duration,
#                     enroute_to_hospital_duration,
#                     at_hospital,
#                     at_hospital_duration,
#                     patient_handover,
#                     handover_duration,
#                     back_to_base_loc,
#                     back_to_base_duration,
#                     inc_pcr_status,
#                     clg_is_login,
#                     destination_hospital_id,
#                     rec_hospital_name,
#                     hospital_id,
#                     amb_working_area,
#                     pcr_missing_count,
#                     pilot_parameters,
#                     is_validate
#                 FROM RTM_dashboard
#                 {where_sql}
#                 ORDER BY STR_TO_DATE(inc_datetime, '%Y-%m-%d %H:%i:%s') DESC
#                 LIMIT 10
#             """

#             rows = await cached_query(query, params=params, fetch="all", ttl=5, db=database2)
#             current_data = [normalize_row(row) for row in rows] if rows else []

#             # ✅ Send data only if changed
#             if current_data != prev_data:
#                 await websocket.send_json({"latest_records": current_data})
#                 prev_data = current_data

#             await asyncio.sleep(15)

#     except WebSocketDisconnect:
#         print("RTM Dashboard WebSocket client disconnected.")
        

# @app.websocket("/ws/rtm_dashboard")
# async def rtm_dashboard_ws(websocket: WebSocket):
#     await websocket.accept()
#     prev_data = None
#     last_filter = {}

#     try:
#         while True:
#             try:
#                 msg = await asyncio.wait_for(websocket.receive_json(), timeout=2)
#                 if msg:
#                     last_filter = msg
#             except asyncio.TimeoutError:
#                 msg = {}

#             filters = last_filter or {}

#             inc_start_date = filters.get("start_date")
#             inc_end_date = filters.get("end_date")
#             district = filters.get("dst_code")
#             ambulance_no = filters.get("ambulance_no")
#             inc_ref_id = filters.get("inc_ref_id")

#             where_clauses = []
#             params = {}

#             # ✅ PostgreSQL compatible date filter
#             if inc_start_date and inc_end_date:
#                 where_clauses.append(
#                     "(inc_datetime::timestamp BETWEEN TO_TIMESTAMP(:start_date, 'YYYY-MM-DD') AND TO_TIMESTAMP(:end_date, 'YYYY-MM-DD'))"
#                 )
#                 params["start_date"] = inc_start_date
#                 params["end_date"] = inc_end_date

#             if district:
#                 where_clauses.append("dst_code = :district")
#                 params["district"] = district

#             if ambulance_no:
#                 where_clauses.append("ambulance_no = :ambulance_no")
#                 params["ambulance_no"] = ambulance_no

#             if inc_ref_id:
#                 where_clauses.append("inc_ref_id = :inc_ref_id")
#                 params["inc_ref_id"] = inc_ref_id

#             where_sql = f"WHERE {' AND '.join(where_clauses)}" if where_clauses else ""

#             # ✅ PostgreSQL-safe query (no STR_TO_DATE)
#             query = f"""
#                 SELECT 
#                     inc_ref_id,
#                     ambulance_no,
#                     dst_code,
#                     district_name,
#                     base_location_name,
#                     call_type,
#                     caller_mobile,
#                     pilot_name,
#                     pilot_mobile,
#                     paramedic_name,
#                     paramedic_mobile,
#                     assigned_time,
#                     parameter_count,
#                     inc_dispatch_time,
#                     inc_recive_time,
#                     inc_datetime,
#                     acknowledge,
#                     start_from_base_loc,
#                     acknowledge_duration,
#                     start_from_base_duration,
#                     at_scene,
#                     at_scene_duration,
#                     wait_time_at_scene_duration,
#                     from_scene,
#                     start_from_scene_duration,
#                     enroute_to_hospital_duration,
#                     at_hospital,
#                     at_hospital_duration,
#                     patient_handover,
#                     handover_duration,
#                     back_to_base_loc,
#                     back_to_base_duration,
#                     inc_pcr_status,
#                     clg_is_login,
#                     destination_hospital_id,
#                     rec_hospital_name,
#                     hospital_id,
#                     amb_working_area,
#                     pilot_parameters,
#                     is_validate,
#                     trip,
#                     remark
#                 FROM rtm_dashboard
#                 {where_sql}
#                 ORDER BY inc_datetime::timestamp DESC
#                 LIMIT 10
#             """

#             rows = await cached_query(query, params=params, fetch="all", ttl=5, db=database2)
#             current_data = [normalize_row(row) for row in rows] if rows else []

#             if current_data != prev_data:
#                 await websocket.send_json({"latest_records": current_data})
#                 prev_data = current_data

#             await asyncio.sleep(3)

#     except WebSocketDisconnect:
#         print("RTM Dashboard WebSocket client disconnected.")

def hash_filter(data: dict):
    return json.dumps(data, sort_keys=True)

@app.websocket("/ws/rtm_dashboard")
async def rtm_dashboard_ws(websocket: WebSocket):
    user_id = await verify_jwt_token(websocket.query_params.get("token"))
    if not user_id:
        print("❌ WS rejected: invalid or missing token")
        await websocket.close(code=1008)
        return
    await websocket.accept()

    prev_data = None
    last_filter = {}
    last_filter_hash = None

    try:
        while True:
            try:
                msg = await asyncio.wait_for(
                    websocket.receive_json(), timeout=0.5
                )
                if msg:
                    last_filter = msg
            except asyncio.TimeoutError:
                pass

            if not last_filter:
                await asyncio.sleep(0.2)
                continue

            inc_ref_id = last_filter.get("inc_ref_id")
            if not inc_ref_id:
                continue

            current_filter_hash = hash_filter(last_filter)
            if current_filter_hash == last_filter_hash:
                await asyncio.sleep(0.2)
                continue

            last_filter_hash = current_filter_hash

            # ---------- SQL SAFE BUILD ----------
            where_sql = "WHERE 1=1"
            params = {}

            where_sql += " AND inc_ref_id = :inc_ref_id"
            params["inc_ref_id"] = inc_ref_id

            if last_filter.get("dst_code"):
                where_sql += " AND dst_code = :dst_code"
                params["dst_code"] = last_filter["dst_code"]

            if last_filter.get("ambulance_no"):
                where_sql += " AND ambulance_no = :ambulance_no"
                params["ambulance_no"] = last_filter["ambulance_no"]

            query = f"""
                SELECT
                    inc_ref_id,
                     ambulance_no,
                     dst_code,
                     district_name,
                     base_location_name,
                     call_type,
                     caller_mobile,
                     pilot_name,
                     pilot_mobile,
                     paramedic_name,
                     paramedic_mobile,
                     assigned_time,
                     parameter_count,
                     inc_dispatch_time,
                     inc_recive_time,
                     inc_datetime,
                     acknowledge,
                     start_from_base_loc,
                     acknowledge_duration,
                     start_from_base_duration,
                     at_scene,
                     at_scene_duration,
                     wait_time_at_scene_duration,
                     from_scene,
                     start_from_scene_duration,
                     enroute_to_hospital_duration,
                     at_hospital,
                     at_hospital_duration,
                     patient_handover,
                     handover_duration,
                     back_to_base_loc,
                     back_to_base_duration,
                     inc_pcr_status,
                     clg_is_login,
                     destination_hospital_id,
                     rec_hospital_name,
                     hospital_id,
                     amb_working_area,
                     pilot_parameters,
                     is_validate,
                     trip,
                     remark,
                     pilot_login_out,
                     emso_login_out,
                     amb_type
                 FROM rtm_dashboard
                 {where_sql}
                 ORDER BY inc_datetime::timestamp DESC
                 LIMIT 1
             """

            # 🔥 DEBUG (KEEP TEMPORARILY)
            print("SQL =>", query)
            print("PARAMS =>", params)

            rows = await cached_query(
                query,
                params=params,
                fetch="all",
                ttl=2,
                db=database2
            )

            data = [normalize_row(r) for r in rows] if rows else []

            if data != prev_data:
                await websocket.send_json({"latest_records": data})
                prev_data = data

    except WebSocketDisconnect:
        print("RTM Dashboard WebSocket disconnected")
        

def hhmmss_to_seconds(value: str) -> int:
    """
    Converts HH:MM:SS text to seconds safely.
    Returns 0 for NULL / empty / invalid values.
    """
    try:
        if not value:
            return 0
        h, m, s = value.split(":")
        return int(h) * 3600 + int(m) * 60 + int(s)
    except Exception:
        return 0

@app.websocket("/ws/rtm_alerts")
async def rtm_alerts_ws(websocket: WebSocket):
    await websocket.accept()
    prev_alerts = None

    try:
        while True:
            query = """
                SELECT *
                FROM rtm_dashboard
                WHERE EXTRACT(YEAR FROM inc_datetime) = 2026
                ORDER BY inc_datetime DESC
                LIMIT 200
            """

            rows = await cached_query(
                query,
                fetch="all",
                ttl=5,
                db=database2
            )

            alerts = []

            for row in rows:
                row = normalize_row(row)

                inc_dispatch_sec = hhmmss_to_seconds(row.get("inc_dispatch_time"))
                acknowledge_sec = hhmmss_to_seconds(row.get("acknowledge_duration"))
                start_base_sec = hhmmss_to_seconds(row.get("start_from_base_duration"))
                at_scene_sec = hhmmss_to_seconds(row.get("at_scene_duration"))

                amb_area = row.get("amb_working_area")

                is_alert = False

                # 1️⃣ Dispatch > 2 min 30 sec
                if inc_dispatch_sec > 150:
                    is_alert = True

                # 2️⃣ Acknowledge > 30 sec
                elif acknowledge_sec > 30:
                    is_alert = True

                # 3️⃣ Start from base > 2 min
                elif start_base_sec > 120:
                    is_alert = True

                # 4️⃣ At scene duration logic
                elif amb_area == "1" and at_scene_sec > 1500:
                    is_alert = True
                elif amb_area == "2" and at_scene_sec > 1080:
                    is_alert = True

                if is_alert:
                    alerts.append(row)

            if alerts != prev_alerts:
                await websocket.send_json({
                    "year": 2026,
                    "alert_count": len(alerts),
                    "alerts": alerts
                })
                prev_alerts = alerts

            await asyncio.sleep(3)

    except WebSocketDisconnect:
        print("RTM Alert WebSocket disconnected")
        

def to_float(val):
    try:
        return float(val) if val not in ("", None) else None
    except Exception:
        return None      
    
from datetime import datetime

def to_datetime(val):
    try:
        if not val:
            return None
        if isinstance(val, datetime):
            return val
        return datetime.strptime(val, "%Y-%m-%d %H:%M:%S")
    except Exception:
        return None 
    
def to_int(val):
    try:
        return int(val) if val not in ("", None) else None
    except Exception:
        return None
        
# @app.websocket("/ws/rtm_alerts_insert")
# async def rtm_alerts_insert_ws(websocket: WebSocket):
#     await websocket.accept()

#     try:
#         while True:
#             query = """
#                 SELECT *
#                 FROM rtm_dashboard
#                 WHERE EXTRACT(YEAR FROM inc_datetime) = 2026
#                 ORDER BY inc_datetime DESC
#                 LIMIT 200
#             """

#             rows = await cached_query(
#                 query,
#                 fetch="all",
#                 ttl=5,
#                 db=database2
#             )

#             for row in rows:
#                 row = normalize_row(row)

#                 # ===============================
#                 # Convert durations to seconds
#                 # ===============================
#                 inc_dispatch_sec = hhmmss_to_seconds(row.get("inc_dispatch_time"))
#                 acknowledge_sec = hhmmss_to_seconds(row.get("acknowledge_duration"))
#                 start_base_sec = hhmmss_to_seconds(row.get("start_from_base_duration"))
#                 at_scene_sec = hhmmss_to_seconds(row.get("at_scene_duration"))

#                 amb_area = row.get("amb_working_area")

#                 alert_type = None
#                 severity = None

#                 # ===============================
#                 # ALERT LOGIC
#                 # ===============================
#                 if inc_dispatch_sec > 150:
#                     alert_type = "DISPATCH_DELAY"
#                     severity = "HIGH"

#                 elif acknowledge_sec > 30:
#                     alert_type = "ACK_DELAY"
#                     severity = "MEDIUM"

#                 elif start_base_sec > 120:
#                     alert_type = "START_DELAY"
#                     severity = "MEDIUM"

#                 elif amb_area == "1" and at_scene_sec > 1500:
#                     alert_type = "AT_SCENE_DELAY"
#                     severity = "LOW"

#                 elif amb_area == "2" and at_scene_sec > 1080:
#                     alert_type = "AT_SCENE_DELAY"
#                     severity = "LOW"

#                 # ===============================
#                 # No alert → skip
#                 # ===============================
#                 if not alert_type:
#                     continue

#                 # ===============================
#                 # Duplicate check
#                 # ===============================
#                 exists = await database2.fetch_one(
#                     """
#                     SELECT 1
#                     FROM central_alerts
#                     WHERE incident_id = :incident_id
#                       AND alert_type = :alert_type
#                       AND is_deleted = false
#                     LIMIT 1
#                     """,
#                     {
#                         "incident_id": row.get("inc_ref_id"),
#                         "alert_type": alert_type
#                     }
#                 )

#                 if exists:
#                     continue

#                 # ===============================
#                 # INSERT ALERT
#                 # ===============================
#                 await database2.execute(
#                     """
#                     INSERT INTO central_alerts (
#                         alert_type,
#                         incident_id,
#                         system_type,
#                         severity,
#                         ambulance_no,
#                         remark,
#                         district,
#                         inc_latitude,
#                         inc_longitude,
#                         amb_lat,
#                         amb_long,
#                         inc_datetime,
#                         pilot_name,
#                         pilot_mobile,
#                         paramedic_name,
#                         paramedic_mobile
#                     )
#                     VALUES (
#                         :alert_type,
#                         :incident_id,
#                         :system_type,
#                         :severity,
#                         :ambulance_no,
#                         :remark,
#                         :district,
#                         :inc_latitude,
#                         :inc_longitude,
#                         :amb_lat,
#                         :amb_long,
#                         :inc_datetime,
#                         :pilot_name,
#                         :pilot_mobile,
#                         :paramedic_name,
#                         :paramedic_mobile
#                     )
#                     """,
#                     {
#                         "alert_type": alert_type,
#                         "incident_id": row.get("inc_ref_id"),
#                         "system_type": row.get("inc_system_type"),
#                         "severity": severity,
#                         "ambulance_no": row.get("ambulance_no"),
#                         "remark": f"{alert_type} threshold breached",

#                         "district": row.get("district_name"),

#                         "inc_latitude": to_float(row.get("inc_lat")),
#                         "inc_longitude": to_float(row.get("inc_long")),
#                         "amb_lat": to_float(row.get("gps_amb_lat")),
#                         "amb_long": to_float(row.get("gps_amb_log")),

#                         "inc_datetime": to_datetime(row.get("inc_datetime")),

#                         "pilot_name": row.get("pilot_name"),
#                         "pilot_mobile": to_int(row.get("pilot_mobile")),
#                         "paramedic_name": row.get("paramedic_name"),
#                         "paramedic_mobile": to_int(row.get("paramedic_mobile")),
#                     }
#                 )

#             # ===============================
#             # Sleep before next cycle
#             # ===============================
#             await asyncio.sleep(5)

#     except WebSocketDisconnect:
#         print("RTM Alert Insert WebSocket disconnected")

#     except Exception as e:
#         print("RTM Alert Insert Error:", str(e))
#################################################################################################

# alert_worker_task = None

# async def get_active_thresholds():
#     """Fetch active thresholds, cached for 30s."""
#     query = """
#         SELECT alert_type, amb_area, threshold_seconds, severity, priority
#         FROM alert_thresholds
#         WHERE is_active = TRUE
#         ORDER BY priority ASC;
#     """
#     rows = await cached_query(query, fetch="all", ttl=30, db=database2)
#     return [normalize_row(r) for r in rows]


# def resolve_alert(row, thresholds):
#     inc_dispatch_sec = hhmmss_to_seconds(row.get("inc_dispatch_time"))
#     acknowledge_sec  = hhmmss_to_seconds(row.get("acknowledge_duration"))
#     start_base_sec   = hhmmss_to_seconds(row.get("start_from_base_duration"))
#     at_scene_sec     = hhmmss_to_seconds(row.get("at_scene_duration"))
#     amb_area         = row.get("amb_working_area")

#     # MDT_NOT_LOGGED_IN check — pilot_login_out column use kiya
#     pilot_login_out_val = row.get("pilot_login_out")
#     mdt_not_found = pilot_login_out_val is None or pilot_login_out_val == "No"

#     metric_map = {
#         "DISPATCH_DELAY": inc_dispatch_sec,
#         "ACK_DELAY": acknowledge_sec,
#         "START_DELAY": start_base_sec,
#         "AT_SCENE_DELAY": at_scene_sec,
#     }

#     for t in thresholds:
#         if t["amb_area"] is not None and t["amb_area"] != amb_area:
#             continue

#         # Boolean-type alert - MDT
#         if t["alert_type"] == "MDT_NOT_LOGGED_IN":
#             if mdt_not_found:
#                 return t["alert_type"], t["severity"]
#             continue

#         # Numeric-type alerts
#         metric_value = metric_map.get(t["alert_type"])
#         threshold_val = int(t["threshold_seconds"])   # fix: string se int conversion

#         if metric_value is not None and metric_value > threshold_val:
#             return t["alert_type"], t["severity"]

#     return None, None


# async def rtm_alert_insert_worker():
#     print("🚀 RTM Alert Insert Worker STARTED")

#     while True:
#         try:
#             query = """
#                 SELECT *
#                 FROM rtm_dashboard
#                 WHERE inc_datetime >= CURRENT_DATE
#                   AND inc_datetime < CURRENT_DATE + INTERVAL '1 day'
#                 ORDER BY inc_datetime DESC
#                 LIMIT 200;
#             """

#             rows = await cached_query(query, fetch="all", ttl=5, db=database2)
#             thresholds = await get_active_thresholds()

#             for row in rows:
#                 try:
#                     row = normalize_row(row)

#                     alert_type, severity = resolve_alert(row, thresholds)
#                     if not alert_type:
#                         continue

#                     params = {
#                         "alert_type": alert_type,
#                         "incident_id": row.get("inc_ref_id"),
#                         "system_type": row.get("inc_system_type"),
#                         "severity": severity,
#                         "ambulance_no": row.get("ambulance_no"),
#                         "remark": f"{alert_type} threshold breached",
#                         "division": row.get("division_name"),
#                         "district": row.get("district_name"),
#                         "inc_latitude": to_float(row.get("inc_lat")),
#                         "inc_longitude": to_float(row.get("inc_long")),
#                         "amb_lat": to_float(row.get("gps_amb_lat")),
#                         "amb_long": to_float(row.get("gps_amb_log")),
#                         "inc_datetime": to_datetime(row.get("inc_datetime")),
#                         "pilot_name": row.get("pilot_name"),
#                         "pilot_mobile": to_int(row.get("pilot_mobile")),
#                         "paramedic_name": row.get("paramedic_name"),
#                         "paramedic_mobile": to_int(row.get("paramedic_mobile")),
#                     }

#                     await database2.execute(
#                         """
#                         INSERT INTO central_alerts (
#                             alert_type, incident_id, system_type, severity,
#                             ambulance_no, remark, division, district,
#                             inc_latitude, inc_longitude, amb_lat, amb_long,
#                             inc_datetime, pilot_name, pilot_mobile,
#                             paramedic_name, paramedic_mobile
#                         )
#                         VALUES (
#                             :alert_type, :incident_id, :system_type, :severity,
#                             :ambulance_no, :remark, :division, :district,
#                             :inc_latitude, :inc_longitude, :amb_lat, :amb_long,
#                             :inc_datetime, :pilot_name, :pilot_mobile,
#                             :paramedic_name, :paramedic_mobile
#                         )
#                         ON CONFLICT (incident_id, alert_type) DO NOTHING
#                         """,
#                         params
#                     )

#                 except Exception as row_err:
#                     print(f"⚠️ Row error (incident_id={row.get('inc_ref_id')}):", str(row_err))
#                     continue

#         except Exception as e:
#             print("❌ RTM Alert Worker Error:", str(e))

#         await asyncio.sleep(5)

        


        
      
alert_worker_task = None

async def get_active_thresholds():
    """Fetch active thresholds, cached for 30s."""
    query = """
        SELECT alert_type, amb_area, threshold_seconds, severity, priority
        FROM alert_thresholds
        WHERE is_active = TRUE
        ORDER BY priority ASC;
    """
    rows = await cached_query(query, fetch="all", ttl=30, db=database2)
    return [normalize_row(r) for r in rows]


def resolve_alerts(row, thresholds):
    acknowledge_sec  = hhmmss_to_seconds(row.get("acknowledge_duration"))
    start_base_sec   = hhmmss_to_seconds(row.get("start_from_base_duration"))
    at_scene_sec     = hhmmss_to_seconds(row.get("at_scene_duration"))
    amb_area         = row.get("amb_working_area")

    ack_raw = row.get("acknowledge_duration")
    ack_done = ack_raw is not None and str(ack_raw).strip() != ""

    patient_handover_dt = to_datetime(row.get("patient_handover"))
    back_to_base_dt     = to_datetime(row.get("back_to_base_loc"))

    back_to_base_sec = None
    if patient_handover_dt and back_to_base_dt:
        diff = (back_to_base_dt - patient_handover_dt).total_seconds()
        if diff >= 0:
            back_to_base_sec = diff

    pilot_login_out_val = row.get("pilot_login_out")
    mdt_not_found = pilot_login_out_val is None or pilot_login_out_val == "No"

    metric_map = {
        "ACK_DELAY": acknowledge_sec,
        "START_DELAY": start_base_sec,
        "AT_SCENE_DELAY": at_scene_sec,
        "BACK_TO_BASE_DELAY": back_to_base_sec,
    }

    matched_alerts = []
    seen_alert_types = set()

    for t in thresholds:
        if t["amb_area"] is not None and t["amb_area"] != amb_area:
            continue

        alert_type = t["alert_type"]

        if alert_type in seen_alert_types:
            continue

        if alert_type == "MDT_NOT_LOGGED_IN":
            if ack_done and mdt_not_found:
                matched_alerts.append((alert_type, t["severity"]))
                seen_alert_types.add(alert_type)
            continue

        metric_value = metric_map.get(alert_type)
        threshold_val = int(t["threshold_seconds"])

        if metric_value is not None and metric_value > threshold_val:
            matched_alerts.append((alert_type, t["severity"]))
            seen_alert_types.add(alert_type)

    return matched_alerts



async def rtm_alert_insert_worker():
    print("🚀 RTM Alert Insert Worker STARTED")

    while True:
        try:
            query = """
                SELECT *
                FROM rtm_dashboard
                WHERE inc_datetime >= CURRENT_DATE
                  AND inc_datetime < CURRENT_DATE + INTERVAL '1 day'
                ORDER BY inc_datetime DESC
                LIMIT 1000;
            """

            rows = await cached_query(query, fetch="all", ttl=5, db=database2)
            thresholds = await get_active_thresholds()

            for row in rows:
                try:
                    row = normalize_row(row)

                    alerts = resolve_alerts(row, thresholds)
                    if not alerts:
                        continue

                    for alert_type, severity in alerts:
                        params = {
                            "alert_type": alert_type,
                            "incident_id": row.get("inc_ref_id"),
                            "system_type": row.get("inc_system_type"),
                            "severity": severity,
                            "ambulance_no": row.get("ambulance_no"),
                            "remark": f"{alert_type} threshold breached",
                            "division": row.get("division_name"),
                            "district": row.get("district_name"),
                            "inc_latitude": to_float(row.get("inc_lat")),
                            "inc_longitude": to_float(row.get("inc_long")),
                            "amb_lat": to_float(row.get("gps_amb_lat")),
                            "amb_long": to_float(row.get("gps_amb_log")),
                            "inc_datetime": to_datetime(row.get("inc_datetime")),
                            "pilot_name": row.get("pilot_name"),
                            "pilot_mobile": to_int(row.get("pilot_mobile")),
                            "paramedic_name": row.get("paramedic_name"),
                            "paramedic_mobile": to_int(row.get("paramedic_mobile")),
                        }

                        await database2.execute(
                            """
                            INSERT INTO central_alerts (
                                alert_type, incident_id, system_type, severity,
                                ambulance_no, remark, division, district,
                                inc_latitude, inc_longitude, amb_lat, amb_long,
                                inc_datetime, pilot_name, pilot_mobile,
                                paramedic_name, paramedic_mobile
                            )
                            VALUES (
                                :alert_type, :incident_id, :system_type, :severity,
                                :ambulance_no, :remark, :division, :district,
                                :inc_latitude, :inc_longitude, :amb_lat, :amb_long,
                                :inc_datetime, :pilot_name, :pilot_mobile,
                                :paramedic_name, :paramedic_mobile
                            )
                            ON CONFLICT (incident_id, alert_type) DO NOTHING
                            """,
                            params
                        )

                except Exception as row_err:
                    print(f"⚠️ Row error (incident_id={row.get('inc_ref_id')}):", str(row_err))
                    continue

        except Exception as e:
            print("❌ RTM Alert Worker Error:", str(e))

        await asyncio.sleep(5)
        ##################################################################################
        
@app.on_event("startup")
async def start_alert_worker():
    global alert_worker_task
    print("✅ Application STARTED")
    alert_worker_task = asyncio.create_task(rtm_alert_insert_worker())
    asyncio.create_task(alert_ws_notifier())

@app.on_event("shutdown")
async def stop_alert_worker():
    global alert_worker_task
    if alert_worker_task:
        alert_worker_task.cancel()
        print("🛑 RTM Alert Insert Worker STOPPED")
        
        
        
        
from datetime import datetime, date
from decimal import Decimal

def serialize_row(row):
    """Convert a database row to a JSON-serializable dict"""
    if hasattr(row, '_mapping'):
        data = dict(row._mapping)
    elif hasattr(row, 'keys'):
        data = {key: row[key] for key in row.keys()}
    else:
        data = dict(row)

    for k, v in list(data.items()):
        if isinstance(v, (datetime, date)):
            data[k] = v.isoformat()
        elif isinstance(v, Decimal):
            data[k] = float(v)

    return data



import logging
logger = logging.getLogger("central_alerts")
logging.basicConfig(level=logging.INFO)


class ConnectionManager:
    """
    Each connection gets its own asyncio.Queue.
    Broadcaster pushes to all queues (non-blocking).
    Each WS worker drains its own queue → slow client doesn't block others.
    """
    def __init__(self):
        self.active_connections: dict[WebSocket, dict] = {}

    async def connect(self, websocket: WebSocket, user_id: str):
        await websocket.accept()
        self.active_connections[websocket] = {
            "user_id": user_id,
            "queue": asyncio.Queue(maxsize=1000),
            "last_seen": time.time(),
        }
        return self.active_connections[websocket]

    def disconnect(self, websocket: WebSocket):
        self.active_connections.pop(websocket, None)

    async def safe_send(self, websocket: WebSocket, payload: dict):
        try:
            await websocket.send_json(payload)
            return True
        except Exception as e:
            logger.warning(f"Send failed, disconnecting: {e}")
            self.disconnect(websocket)
            return False

    def broadcast(self, payload: dict):
        """Non-blocking broadcast — pushes to every queue."""
        dead = []
        for ws, info in self.active_connections.items():
            try:
                info["queue"].put_nowait(payload)
            except asyncio.QueueFull:
                logger.warning("Queue full, dropping client")
                dead.append(ws)
        for ws in dead:
            self.disconnect(ws)


manager = ConnectionManager()


# @app.websocket("/ws/central_alerts")
# async def central_alerts_ws(websocket: WebSocket):
#     await manager.connect(websocket)
#     print("🔌 WebSocket client connected")

#     # 🔹 SEND ALL EXISTING ALERTS
#     rows = await database2.fetch_all(
#         "SELECT * FROM central_alerts ORDER BY alert_id ASC"
#     )

#     await websocket.send_json({
#         "type": "ALL_ALERTS",
#         "data": [serialize_row(r) for r in rows]
#     })

#     try:
#         while True:
#             await asyncio.sleep(1000)
#     except:
#         manager.disconnect(websocket)
#         print("❌ WebSocket client disconnected")

# from datetime import datetime, date, timedelta

# today_start = datetime.combine(date.today(), datetime.min.time())
# today_end = today_start + timedelta(days=1)

# def group_by_severity(rows):
#     """Group alerts by severity"""
#     severity_order = ["CRITICAL", "HIGH", "MEDIUM", "LOW"]
#     grouped = {sev: [] for sev in severity_order}

#     for r in rows:
#         serialized = serialize_row(r)
#         sev = (serialized.get("severity") or "").upper()

#         if sev in grouped:
#             grouped[sev].append(serialized)
#         else:
#             grouped["LOW"].append(serialized)

#     return grouped

###mainnnnn
# class CustomJSONEncoder(json.JSONEncoder):
#     def default(self, obj):
#         if isinstance(obj, (datetime, date)):
#             return obj.isoformat()
#         elif isinstance(obj, Decimal):
#             return float(obj)
#         return super().default(obj)


# # ===============================
# # MAIN WEBSOCKET
# # ===============================

# @app.websocket("/ws/central_alerts")
# async def central_alerts_ws(websocket: WebSocket):

#     user_id = await verify_jwt_token(websocket.query_params.get("token"))
#     if not user_id:
#         await websocket.accept()
#         await websocket.send_json({
#             "type": "ERROR",
#             "status": 401,
#             "message": "Invalid or expired token. Please login again."
#         })
#         await websocket.close(code=1008)
#         return

#     await manager.connect(websocket)
#     print("🔌 WebSocket client connected")

#     try:
#         # =====================================================
#         # 1️⃣ SEND DATA ON CONNECT (TODAY DEFAULT)
#         # =====================================================
#         rows = await cached_query(
#             """
#             SELECT *
#             FROM central_alerts
#             WHERE inc_datetime >= CURRENT_DATE
#               AND inc_datetime < CURRENT_DATE + INTERVAL '1 day'
#             ORDER BY alert_id DESC
#             """,
#             ttl=3,
#             fetch="all",
#             db=database2
#         )

#         today_all = [serialize_row(r) for r in rows]
#         by_severity = group_by_severity(rows)

#         # ===============================
#         # COUNTS (108 / 102)
#         # ===============================
#         count_rows = await cached_query(
#             """
#             SELECT
#                 system_type,
#                 severity,
#                 COUNT(*) AS total
#             FROM central_alerts
#             WHERE inc_datetime >= CURRENT_DATE
#               AND inc_datetime < CURRENT_DATE + INTERVAL '1 day'
#               AND escalate_status = '1'
#               AND system_type IN ('108', '102')
#             GROUP BY system_type, severity
#             """,
#             ttl=3,
#             fetch="all",
#             db=database2
#         )

#         counts = {
#             "total": {"108": 0, "102": 0},
#             "severity": {"108": {}, "102": {}}
#         }

#         for r in count_rows:
#             system = r["system_type"]
#             severity = r["severity"]
#             total = r["total"]

#             counts["total"][system] += total
#             counts["severity"][system][severity] = total

#         await websocket.send_json({
#             "type": "ALL_ALERTS",
#             "data": {
#                 "today_all": today_all,
#                 "by_severity": by_severity,
#                 "counts": counts
#             }
#         })

#         print("📤 Sent ALL_ALERTS on connect")

#         # =====================================================
#         # 2️⃣ LISTEN FOR FILTER REQUESTS
#         # =====================================================
#         while True:
#             msg = await websocket.receive_json()

#             incident_id = msg.get("incident_id")
#             filter_date = msg.get("date")  # YYYY-MM-DD

#             conditions = []
#             params = {}

#             # incident filter
#             if incident_id:
#                 conditions.append("incident_id = :incident_id")
#                 params["incident_id"] = str(incident_id)

#             # created_date filter
#             if filter_date:
#                 start_dt = datetime.strptime(filter_date, "%Y-%m-%d")
#                 end_dt = start_dt + timedelta(days=1)

#                 conditions.append("""
#                     created_date >= :start_date
#                     AND created_date < :end_date
#                 """)

#                 params["start_date"] = start_dt
#                 params["end_date"] = end_dt
#             else:
#                 # default today logic
#                 conditions.append("""
#                     inc_datetime >= CURRENT_DATE
#                     AND inc_datetime < CURRENT_DATE + INTERVAL '1 day'
#                 """)

#             where_clause = " AND ".join(conditions)

#             query = f"""
#                 SELECT *
#                 FROM central_alerts
#                 WHERE {where_clause}
#                 ORDER BY alert_id DESC
#             """

#             if filter_date:
#                 rows = await database2.fetch_all(query, params)
#             else:
#                 rows = await cached_query(
#                     query,
#                     params=params,
#                     ttl=3,
#                     fetch="all",
#                     db=database2
#                 )

#             today_all = [serialize_row(r) for r in rows]
#             by_severity = group_by_severity(rows)

#             await websocket.send_json({
#                 "type": "ALL_ALERTS",
#                 "data": {
#                     "today_all": today_all,
#                     "by_severity": by_severity,
#                     "counts": counts
#                 }
#             })

#             print(f"📤 Sent alerts (incident_id={incident_id}, date={filter_date})")

#     except Exception as e:
#         manager.disconnect(websocket)
#         print("❌ WebSocket client disconnected:", e)

from datetime import datetime, date, timedelta

today_start = datetime.combine(date.today(), datetime.min.time())
today_end = today_start + timedelta(days=1)

def group_by_severity(rows):
    """Group alerts by severity"""
    severity_order = ["CRITICAL", "HIGH", "MEDIUM", "LOW"]
    grouped = {sev: [] for sev in severity_order}

    for r in rows:
        serialized = serialize_row(r)
        sev = (serialized.get("severity") or "").upper()

        if sev in grouped:
            grouped[sev].append(serialized)
        else:
            grouped["LOW"].append(serialized)

    return grouped


class CustomJSONEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, (datetime, date)):
            return obj.isoformat()
        elif isinstance(obj, Decimal):
            return float(obj)
        return super().default(obj)


# # ===============================
# # MAIN WEBSOCKET
# # ===============================

# @app.websocket("/ws/central_alerts")
# async def central_alerts_ws(websocket: WebSocket):

#     user_id = await verify_jwt_token(websocket.query_params.get("token"))
#     if not user_id:
#         await websocket.accept()
#         await websocket.send_json({
#             "type": "ERROR",
#             "status": 401,
#             "message": "Invalid or expired token. Please login again."
#         })
#         await websocket.close(code=1008)
#         return

#     await manager.connect(websocket)
#     print("🔌 WebSocket client connected")

#     try:
#         # =====================================================
#         # 1️⃣ SEND DATA ON CONNECT (TODAY DEFAULT)
#         # =====================================================
#         rows = await cached_query(
#             """
#             SELECT *
#             FROM central_alerts
#             WHERE inc_datetime >= CURRENT_DATE
#               AND inc_datetime < CURRENT_DATE + INTERVAL '1 day'
#             ORDER BY inc_datetime DESC
#             """,
#             ttl=3,
#             fetch="all",
#             db=database2
#         )

#         today_all = [serialize_row(r) for r in rows]
#         by_severity = group_by_severity(rows)

#         # ===============================
#         # COUNTS (108 / 102)
#         # ===============================
#         count_rows = await cached_query(
#             """
#             SELECT
#                 system_type,
#                 severity,
#                 COUNT(*) AS total
#             FROM central_alerts
#             WHERE inc_datetime >= CURRENT_DATE
#               AND inc_datetime < CURRENT_DATE + INTERVAL '1 day'
#               AND escalate_status = '1'
#               AND system_type IN ('108', '102')
#             GROUP BY system_type, severity
#             """,
#             ttl=3,
#             fetch="all",
#             db=database2
#         )

#         counts = {
#             "total": {"108": 0, "102": 0},
#             "severity": {"108": {}, "102": {}}
#         }

#         for r in count_rows:
#             system = r["system_type"]
#             severity = r["severity"]
#             total = r["total"]

#             counts["total"][system] += total
#             counts["severity"][system][severity] = total

#         await websocket.send_json({
#             "type": "ALL_ALERTS",
#             "data": {
#                 "today_all": today_all,
#                 "by_severity": by_severity,
#                 "counts": counts
#             }
#         })

#         print("📤 Sent ALL_ALERTS on connect")

#         # =====================================================
#         # 2️⃣ LISTEN FOR FILTER REQUESTS
#         # =====================================================
#         while True:
#             msg = await websocket.receive_json()

#             incident_id = msg.get("incident_id")
#             filter_date = msg.get("date")  # YYYY-MM-DD

#             conditions = []
#             params = {}

#             # incident filter
#             if incident_id:
#                 conditions.append("incident_id = :incident_id")
#                 params["incident_id"] = str(incident_id)

#             # created_date filter
#             if filter_date:
#                 start_dt = datetime.strptime(filter_date, "%Y-%m-%d")
#                 end_dt = start_dt + timedelta(days=1)

#                 conditions.append("""
#                     created_date >= :start_date
#                     AND created_date < :end_date
#                 """)

#                 params["start_date"] = start_dt
#                 params["end_date"] = end_dt
#             else:
#                 # default today logic
#                 conditions.append("""
#                     inc_datetime >= CURRENT_DATE
#                     AND inc_datetime < CURRENT_DATE + INTERVAL '1 day'
#                 """)

#             where_clause = " AND ".join(conditions)

#             query = f"""
#                 SELECT *
#                 FROM central_alerts
#                 WHERE {where_clause}
#                 ORDER BY inc_datetime DESC
#             """

#             if filter_date:
#                 rows = await database2.fetch_all(query, params)
#             else:
#                 rows = await cached_query(
#                     query,
#                     params=params,
#                     ttl=3,
#                     fetch="all",
#                     db=database2
#                 )

#             today_all = [serialize_row(r) for r in rows]
#             by_severity = group_by_severity(rows)

#             await websocket.send_json({
#                 "type": "ALL_ALERTS",
#                 "data": {
#                     "today_all": today_all,
#                     "by_severity": by_severity,
#                     "counts": counts
#                 }
#             })

#             print(f"📤 Sent alerts (incident_id={incident_id}, date={filter_date})")

#     except Exception as e:
#         manager.disconnect(websocket)
#         print("❌ WebSocket client disconnected:", e)


HEARTBEAT_INTERVAL = 25        # server pings every 25s
HEARTBEAT_TIMEOUT = 60         # if no pong in 60s, kill
FULL_SYNC_INTERVAL = 15        # safety-net full refresh every 15s
TOKEN_RECHECK_INTERVAL = 300   # recheck token every 5 min
KEEPALIVE_INTERVAL = 25


async def _fetch_alerts_payload(incident_id=None, filter_date=None):
    """Fetch alerts + counts as a single payload. Used both for initial + filter + full-sync."""
    conditions = []
    params = {}

    if incident_id:
        conditions.append("incident_id = :incident_id")
        params["incident_id"] = str(incident_id)
    elif filter_date:
        start_dt = datetime.strptime(filter_date, "%Y-%m-%d")
        end_dt = start_dt + timedelta(days=1)
        conditions.append("created_date >= :start_date AND created_date < :end_date")
        params["start_date"] = start_dt
        params["end_date"] = end_dt
    else:
        conditions.append("inc_datetime >= CURRENT_DATE AND inc_datetime < CURRENT_DATE + INTERVAL '1 day'")

    where_clause = " AND ".join(conditions)

    query = f"""
        SELECT * FROM central_alerts
        WHERE {where_clause}
        ORDER BY inc_datetime DESC
    """

    if filter_date:
        rows = await database2.fetch_all(query, params)
    else:
        rows = await cached_query(query, params=params, ttl=3, fetch="all", db=database2)

    today_all = [serialize_row(r) for r in rows]
    by_severity = group_by_severity(rows)

    # ✅ Counts recalculated for EVERY filter (bug fixed)
    count_rows = await cached_query(
        f"""
        SELECT system_type, severity, COUNT(*) AS total
        FROM central_alerts
        WHERE {where_clause}
          AND escalate_status = '1'
          AND system_type IN ('108', '102')
        GROUP BY system_type, severity
        """,
        params=params,
        ttl=3,
        fetch="all",
        db=database2,
    ) or []

    counts = {"total": {"108": 0, "102": 0}, "severity": {"108": {}, "102": {}}}
    for r in count_rows:
        sys_t = r["system_type"]
        sev = r["severity"]
        counts["total"][sys_t] += r["total"]
        counts["severity"][sys_t][sev] = r["total"]

    return {
        "type": "ALL_ALERTS",
        "data": {
            "today_all": today_all,
            "by_severity": by_severity,
            "counts": counts,
        },
    }


@app.websocket("/ws/central_alerts")
async def central_alerts_ws(websocket: WebSocket):
    user_id = await verify_jwt_token(websocket.query_params.get("token"))
    if not user_id:
        await websocket.accept()
        await websocket.send_json({
            "type": "ERROR", "status": 401,
            "message": "Invalid or expired token. Please login again."
        })
        await websocket.close(code=1008)
        return

    info = await manager.connect(websocket, user_id)
    queue: asyncio.Queue = info["queue"]
    should_stop = asyncio.Event()

    try:
        # ✅ Initial full state (frontend knows ALL_ALERTS)
        payload = await _fetch_alerts_payload()
        if not await manager.safe_send(websocket, payload):
            return
        logger.info(f"📤 Sent ALL_ALERTS on connect (user={user_id})")

        # ============ RECEIVE LOOP — only filter requests ============
        async def receive_loop():
            while not should_stop.is_set():
                try:
                    msg = await websocket.receive_json()
                except WebSocketDisconnect:
                    logger.info(f"👋 Client disconnected (user={user_id})")
                    should_stop.set()
                    return
                except Exception as e:
                    logger.warning(f"receive_loop error: {e}")
                    should_stop.set()
                    return

                # Handle filter request from frontend
                incident_id = msg.get("incident_id")
                filter_date = msg.get("date")
                try:
                    payload = await _fetch_alerts_payload(incident_id, filter_date)
                    if not await manager.safe_send(websocket, payload):
                        should_stop.set()
                        return
                except Exception as e:
                    logger.error(f"Filter fetch error: {e}")

        # ============ DRAIN LOOP — only ALERT_CHANGED broadcasts ============
        async def drain_loop():
            while not should_stop.is_set():
                try:
                    payload = await asyncio.wait_for(queue.get(), timeout=2)
                except asyncio.TimeoutError:
                    continue
                if not await manager.safe_send(websocket, payload):
                    should_stop.set()
                    return

        # ❌ NO full_sync_loop — removed (was causing flicker)
        # ❌ NO keepalive_loop — removed (was causing flicker)
        # ❌ NO token_recheck_loop — removed (causing unnecessary disconnects)

        # ============ RUN TASKS ============
        tasks = [
            asyncio.create_task(receive_loop(), name="receive"),
            asyncio.create_task(drain_loop(),   name="drain"),
        ]

        done, pending = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
        should_stop.set()

        for t in pending:
            t.cancel()
        for t in tasks:
            try:
                await t
            except (asyncio.CancelledError, WebSocketDisconnect):
                pass
            except Exception as e:
                logger.debug(f"Task {t.get_name()} ended: {e}")

    except WebSocketDisconnect:
        logger.info(f"❌ WebSocket disconnected (user={user_id})")
    except Exception as e:
        logger.exception(f"❌ WebSocket error: {e}")
    finally:
        manager.disconnect(websocket)
        try:
            await websocket.close()
        except Exception:
            pass
        logger.info(f"🔌 Cleaned up connection (user={user_id})")

###################################################################################################
# # ===============================
# # ALERT CHANGE NOTIFIER
# # ===============================

last_sent_updated = None
last_sent_alert_id = None


async def alert_ws_notifier():
    """
    Poll central_alerts every 1s for ANY change (insert/update/delete).
    On first run: send nothing (clients get full state on connect).
    On restart: pick latest as baseline (no history flood).
    """
    global last_sent_updated, last_sent_alert_id
    logger.info("🚀 Alert WS Notifier STARTED")

    # Bootstrap: set baseline to latest so we don't flood on restart
    if last_sent_updated is None:
        row = await database2.fetch_one(
            """
            SELECT alert_id, updated_date
            FROM central_alerts
            ORDER BY updated_date DESC NULLS LAST, alert_id DESC
            LIMIT 1
            """
        )
        if row:
            last_sent_updated = row["updated_date"]
            last_sent_alert_id = row["alert_id"]

    while True:
        try:
            rows = await database2.fetch_all(
                """
                SELECT *
                FROM central_alerts
                WHERE (updated_date > :last_time)
                   OR (updated_date = :last_time AND alert_id > :last_id)
                ORDER BY updated_date ASC, alert_id ASC
                """,
                {"last_time": last_sent_updated, "last_id": last_sent_alert_id}
            )

            if rows:
                last_sent_updated = rows[-1]["updated_date"]
                last_sent_alert_id = rows[-1]["alert_id"]

                payload = {
                    "type": "ALERT_CHANGED",
                    "data": [serialize_row(r) for r in rows]
                }
                manager.broadcast(payload)   # ✅ non-blocking queue push
                logger.info(f"📡 Broadcast {len(rows)} changed alerts")

        except Exception as e:
            logger.exception(f"❌ Alert WS Notifier error: {e}")

        await asyncio.sleep(1)

# last_sent_updated = None
# last_sent_alert_id = None


# async def alert_ws_notifier():
#     global last_sent_updated, last_sent_alert_id
#     print("🚀 Alert WebSocket Notifier STARTED")

#     while True:
#         try:
#             if last_sent_updated is None:
#                 row = await database2.fetch_one(
#                     """
#                     SELECT alert_id, updated_date
#                     FROM central_alerts
#                     ORDER BY updated_date DESC, alert_id DESC
#                     LIMIT 1
#                     """
#                 )
#                 if row:
#                     last_sent_updated = row["updated_date"]
#                     last_sent_alert_id = row["alert_id"]

#             else:
#                 rows = await database2.fetch_all(
#                     """
#                     SELECT *
#                     FROM central_alerts
#                     WHERE (updated_date > :last_time)
#                        OR (updated_date = :last_time AND alert_id > :last_id)
#                     ORDER BY updated_date ASC, alert_id ASC
#                     """,
#                     {
#                         "last_time": last_sent_updated,
#                         "last_id": last_sent_alert_id
#                     }
#                 )

#                 if rows:
#                     last_sent_updated = rows[-1]["updated_date"]
#                     last_sent_alert_id = rows[-1]["alert_id"]

#                     payload = {
#                         "type": "ALERT_CHANGED",
#                         "data": [serialize_row(r) for r in rows]
#                     }

#                     await manager.broadcast_text(payload)
#                     print(f"📡 WS sent {len(rows)} changed alerts")

#         except Exception as e:
#             print("❌ Alert WS Notifier Error:", e)

#         await asyncio.sleep(1)

    #################################################################
class EscalateRequest(BaseModel):
    remark: Optional[str] = None
    escalated_by: Optional[str] = None
           
@app.put("/api/escalate/{alert_id}")
async def escalate_alert(alert_id: int, payload: EscalateRequest):
    """
    Escalate alert:
    escalate_status: 1 -> 2
    remark: from frontend (optional)
    updated_date: current timestamp
    """

    # 🔍 Check if alert exists
    alert = await database2.fetch_one(
        """
        SELECT alert_id, escalate_status
        FROM central_alerts
        WHERE alert_id = :alert_id
        """,
        {"alert_id": alert_id}
    )

    if not alert:
        raise HTTPException(status_code=404, detail="Alert not found")

    await database2.execute(
        """
        UPDATE central_alerts
        SET escalate_status = 2,
            escalated_deny_remark = :remark,
            updated_date = NOW(),
            escalated_date = NOW(),
            escalated_by = :escalated_by
        WHERE alert_id = :alert_id
        """,
        {
            "alert_id": alert_id,
            "remark": payload.remark,
            "escalated_by": payload.escalated_by
        }
    )

    return {
        "status": "success",
        "message": "Alert escalated successfully",
        "alert_id": alert_id,
        "escalate_status": 2,
        "remark": payload.remark,
        "updated_date": datetime.utcnow().isoformat()
    }




def get_date_filter(range_type: str):
    if range_type == "today":
        return "DATE(created_date) = CURRENT_DATE"

    elif range_type == "month":
        return "DATE_TRUNC('month', created_date) = DATE_TRUNC('month', CURRENT_DATE)"

    else:  # all
        return "1=1"
    


@app.get("/api/dashboard")
async def dashboard_alerts_overview(
    range: Optional[str] = Query("today", enum=["today", "month", "all"])
):
    date_filter = get_date_filter(range)

    # ===============================
    # TOTAL ALERTS
    # ===============================
    total_alerts_sql = f"""
    SELECT
        COUNT(*) as total,
        COUNT(*) FILTER (WHERE system_type='108') as system_108,
        COUNT(*) FILTER (WHERE system_type='102') as system_102
    FROM public.central_alerts
    WHERE is_deleted = false
    AND {date_filter}
    """

    total_alerts = await database2.fetch_one(total_alerts_sql)

    # ===============================
    # ESCALATED ALERTS
    # ===============================
    escalated_sql = f"""
    SELECT
        COUNT(*) as total,
        COUNT(*) FILTER (WHERE system_type='108') as system_108,
        COUNT(*) FILTER (WHERE system_type='102') as system_102
    FROM public.central_alerts
    WHERE is_deleted = false
    AND escalate_status = '2'
    AND {date_filter}
    """

    escalated_alerts = await database2.fetch_one(escalated_sql)

    # ===============================
    # SEVERITY TIMELINE
    # ===============================
    severity_sql = f"""
    SELECT
        severity,
        COUNT(*) FILTER (WHERE system_type='108') as system_108,
        COUNT(*) FILTER (WHERE system_type='102') as system_102
    FROM public.central_alerts
    WHERE is_deleted = false
    AND {date_filter}
    GROUP BY severity
    ORDER BY severity
    """

    severity_rows = await database2.fetch_all(severity_sql)

    severity_data = [
        {
            "severity": row["severity"],
            "system_108": row["system_108"],
            "system_102": row["system_102"]
        }
        for row in severity_rows
    ]

    return {
        "total_alerts": dict(total_alerts),
        "escalated_alerts": dict(escalated_alerts),
        "severity_timeline": severity_data
    }






def format_worksheet(ws):

    header_fill = PatternFill(start_color="1F4E78", end_color="1F4E78", fill_type="solid")
    header_font = Font(color="FFFFFF", bold=True)

    for cell in ws[1]:
        cell.fill = header_fill
        cell.font = header_font
        cell.alignment = Alignment(horizontal="center", vertical="center")

    ws.freeze_panes = "A2"
    ws.auto_filter.ref = ws.dimensions

    # Auto Column Width
    for col in ws.columns:
        max_length = 0
        col_letter = col[0].column_letter

        for cell in col:
            try:
                if cell.value:
                    max_length = max(max_length, len(str(cell.value)))
            except:
                pass

        ws.column_dimensions[col_letter].width = max_length + 3

@app.get("/api/dashboard/download-client-report")
async def download_client_report(
    range_type: str = Query("today", enum=["today", "month", "all"])
):

    date_filter = get_date_filter(range_type)

    # ===============================
    # FETCH FULL DATA
    # ===============================
    full_sql = f"""
    SELECT *
    FROM public.central_alerts
    WHERE is_deleted = false
    AND {date_filter}
    ORDER BY created_date DESC
    """

    rows = await database2.fetch_all(full_sql)

    df = pd.DataFrame([dict(r) for r in rows]) if rows else pd.DataFrame()

    if not df.empty:

        # ===============================
        # SERIAL NUMBER
        # ===============================
        df.insert(0, "Sr No", list(range(1, len(df) + 1)))

        # ===============================
        # ESCALATION STATUS TEXT
        # ===============================
        escalation_map = {
            "0": "Open",
            "1": "In Progress",
            "2": "Escalated",
            "3": "Closed"
        }

        if "escalate_status" in df.columns:
            df["Escalation Status"] = df["escalate_status"].astype(str).map(escalation_map)
            df.drop(columns=["escalate_status"], inplace=True)

        # ===============================
        # DROP UNWANTED COLUMNS
        # ===============================
        if "is_deleted" in df.columns:
            df.drop(columns=["is_deleted"], inplace=True)

        # ===============================
        # INDIA DATE FORMAT
        # ===============================
        date_cols = ["created_date", "updated_date", "cancel_date", "escalated_date"]

        for col in date_cols:
            if col in df.columns:
                df[col] = pd.to_datetime(df[col], errors="coerce") \
                    .dt.strftime("%d-%m-%Y %H:%M")

        # ===============================
        # CLIENT FRIENDLY COLUMN RENAME
        # ===============================
        rename_map = {
            "severity": "Severity",
            "created_date": "Created Date & Time",
            "updated_date": "Updated Date & Time",
            "division": "Division",
            "district": "District",
            "inc_latitude":"Incidence Latitude",
            "inc_longitude":"Incidence Longitude",
            "amb_lat":"Ambulance Lattitude",
            "amb_long":"Ambulance Longitude",
            "paramedic_name":"EMT Name",
            "paramedic_mobile":"EMT Mobile",
            "inc_datetime":"Incidence Datetime",
            "alert_type":"Alert Type",
            "incident_id":"Incident Id",
            "ambulance_no":"Ambulance Number",
            "remark":"Remark",
            "escalated_deny_remark":"Escalated/Deny Remark",
            "pilot_name":"Pilot Name",
            "pilot_mobile":"Pilot Mobile",
            "escalated_date":"Escalated Date",
            "cancel_date":"Cancel Date",
            "escalated_by": "Escalated By",
            "cancel_by":"Cancel By",
            "Escalation Status":"Escalation Status",
            "system_type":"System Type",
            "alert_id":"Alert ID",
            "Sr No":"Sr No",
            "alert_type":"Alert Type"
            
        }

        df.rename(columns=rename_map, inplace=True)

        # ===============================
        # COLUMN ORDER (as per report requirement)
        # ===============================
        column_order = [
            "Sr No",
            "Alert ID",
            "Alert Type",
            "System Type",
            "Severity",
            "Incident Id",
            "Incidence Datetime",
            "Division",
            "District",
            "Incidence Latitude",
            "Incidence Longitude",
            "Ambulance Number",
            "Ambulance Lattitude",
            "Ambulance Longitude",
            "Pilot Name",
            "Pilot Mobile",
            "EMT Name",
            "EMT Mobile",
            "Created Date & Time",
            "Updated Date & Time",
            "Escalation Status",
            "Escalated Date",
            "Escalated By",
            "Escalated/Deny Remark",
            "Cancel Date",
            "Cancel By",
            "Remark",
        ]

       
        existing_ordered_cols = [c for c in column_order if c in df.columns]
       
        remaining_cols = [c for c in df.columns if c not in existing_ordered_cols]
        df = df[existing_ordered_cols + remaining_cols]

    else:
        df = pd.DataFrame([{"Message": "No Data Found"}])

    # ===============================
    # SUMMARY SHEET
    # ===============================
    summary_sql = f"""
    SELECT
        COUNT(*) as total_alerts,
        COUNT(*) FILTER (WHERE escalate_status='2') as escalated_alerts,
        COUNT(*) FILTER (WHERE system_type='108') as system_108
    FROM public.central_alerts
    WHERE is_deleted = false
    AND {date_filter}
    """

    summary_row = await database2.fetch_one(summary_sql)
    df_summary = pd.DataFrame([dict(summary_row)]) if summary_row else pd.DataFrame()

    # ===============================
    # SUMMARY SHEET COLUMN RENAME
    # ===============================
    summary_rename_map = {
        "total_alerts": "Total Alerts",
        "escalated_alerts": "Escalated Alerts",
        "system_108": "System 108"
    }

    if not df_summary.empty:
        df_summary.rename(columns=summary_rename_map, inplace=True)

    # ===============================
    # WRITE EXCEL
    # ===============================
    output = io.BytesIO()

    with pd.ExcelWriter(output, engine="openpyxl") as writer:

        df.to_excel(writer, sheet_name="All Alert Records", index=False)
        df_summary.to_excel(writer, sheet_name="Summary", index=False)

        workbook = writer.book

        format_worksheet(workbook["All Alert Records"])
        format_worksheet(workbook["Summary"])

    output.seek(0)

    file_name = f"Central_Alerts_Client_Report_{datetime.now().strftime('%Y%m%d_%H%M')}.xlsx"

    return StreamingResponse(
        output,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={
            "Content-Disposition": f"attachment; filename={file_name}"
        }
    )
#################################################################

class SeverityUpdate(BaseModel):
    alert_id: int
    severity: str
 
@app.put("/api/severity")
async def update_severity(data: SeverityUpdate):
    await database2.execute(
        """
        UPDATE central_alerts
        SET severity = :severity,
            updated_date = NOW()
        WHERE alert_id = :alert_id
        """,
        {
            "severity": data.severity,
            "alert_id": data.alert_id
        }
    )
    return {"message": "updated"}


class CancelUpdate(BaseModel):
    alert_id: int
    remark: str
    cancel_by: Optional[str] = None
 
@app.put("/api/cancel")
async def cancel_alert(data: CancelUpdate):
    await database2.execute(
        """
        UPDATE central_alerts
        SET escalated_deny_remark = :remark,
            escalate_status = '3',
            updated_date = CURRENT_TIMESTAMP,
            cancel_date = CURRENT_TIMESTAMP,
            cancel_by = :cancel_by
        WHERE alert_id = :alert_id
        
        """,
        {
            "remark": data.remark,
            "alert_id": data.alert_id,
            "cancel_by": data.cancel_by
        }
    )
 
    return {"message": "Alert cancelled successfully"}


import calendar  
@app.websocket("/ws/top-ambulances")
async def ws_top_ambulances(websocket: WebSocket):
    await websocket.accept()
    print("🔌 Top Ambulances WebSocket connected")

    # shared state
    month = datetime.now().month
    lock = asyncio.Lock()
    last_payload = None  # avoid sending same data again

    async def fetch_and_send(force=False):
        nonlocal last_payload

        async with lock:
            query = """
                SELECT
                    ambulance_no,
                    CAST(SUM(total_alerts) AS BIGINT)        AS total_alerts,
                    CAST(SUM(start_delay) AS BIGINT)         AS start_delay_count,
                    CAST(SUM(at_scene_delay) AS BIGINT)      AS at_scene_delay_count,
                    CAST(SUM(ack_delay) AS BIGINT)           AS ack_delay_count,
                    CAST(SUM(mdt_not_logged_in) AS BIGINT)   AS mdt_not_logged_in_count,
                    CAST(SUM(back_to_base_delay) AS BIGINT)  AS back_to_base_delay_count
                FROM alerts_performance_monthly
                WHERE month = :month
                  AND ambulance_no IS NOT NULL
                GROUP BY ambulance_no
                ORDER BY total_alerts DESC
                LIMIT 50;
            """ 

            # ✅ ONLY cached_query (Redis + memory)
            rows = await cached_query(
                query,
                params={"month": month},
                ttl=3,                 
                fetch="all",
                db=database2
            ) or []

            payload = {
                "month": month,
                "month_name": calendar.month_name[month],
                "top_ambulances": rows
            }

            # 🚀 Send only if changed (or forced)
            if force or payload != last_payload:
                await websocket.send_json(payload)
                last_payload = payload

    async def listen_filters():
        nonlocal month
        while True:
            data = await websocket.receive_json()

            if "month" in data:
                m = int(data["month"])
                if 1 <= m <= 12 and m != month:
                    month = m
                    # ⚡ instant response on filter
                    await fetch_and_send(force=True)

    async def auto_refresh():
        while True:
            await asyncio.sleep(3)
            await fetch_and_send()

    try:
        # ⚡ FIRST RESPONSE IMMEDIATELY (NO WAIT)
        await fetch_and_send(force=True)

        # 🔥 Run both loops in parallel
        await asyncio.gather(
            listen_filters(),
            auto_refresh()
        )

    except WebSocketDisconnect:
        print("❌ Top Ambulances WebSocket disconnected")

    except Exception as e:
        print("❌ WS ERROR:", e)
        await websocket.close()

#####################################################################################

@app.get("/api/alert-thresholds")
async def get_alert_thresholds():
    query = """
        SELECT *
        FROM alert_thresholds
        ORDER BY priority ASC;
    """

    rows = await cached_query(
        query,
        fetch="all",
        ttl=30,
        db=database2
    )

    return [dict(r) for r in rows]

#################################################################################

class AlertThresholdUpdate(BaseModel):
    threshold_seconds: Optional[int] = None
    severity: Optional[str] = None
    priority: Optional[int] = None


@app.put("/api/update-alert-threshold/update/{id}")
async def update_alert_threshold(id: int, data: AlertThresholdUpdate):

    # Check if ID exists
    check_query = """
        SELECT id
        FROM alert_thresholds
        WHERE id = :id
    """

    row = await database2.fetch_one(
        query=check_query,
        values={"id": id}
    )

    if not row:
        raise HTTPException(
            status_code=404,
            detail="Alert threshold not found."
        )

    update_fields = []
    values = {"id": id}

    if data.threshold_seconds is not None:
        update_fields.append("threshold_seconds = :threshold_seconds")
        values["threshold_seconds"] = data.threshold_seconds

    if data.severity is not None:
        update_fields.append("severity = :severity")
        values["severity"] = data.severity

    if data.priority is not None:
        update_fields.append("priority = :priority")
        values["priority"] = data.priority

    if not update_fields:
        raise HTTPException(
            status_code=400,
            detail="Please provide at least one field to update."
        )

    update_fields.append("updated_at = CURRENT_TIMESTAMP")   # 👈 FIX

    query = f"""
        UPDATE alert_thresholds
        SET {', '.join(update_fields)}
        WHERE id = :id
    """

    try:
        await database2.execute(
            query=query,
            values=values
        )

        return {
            "status": "success",
            "message": "Alert threshold updated successfully.",
            "id": id
        }

    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=str(e)
        )