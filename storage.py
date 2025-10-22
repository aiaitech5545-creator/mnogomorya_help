import aiosqlite
from typing import Optional, Tuple

DB_PATH='bot.db'

SCHEMA='''
CREATE TABLE IF NOT EXISTS users(
 user_id INTEGER PRIMARY KEY, username TEXT, first_name TEXT, last_name TEXT, created_at TEXT);
CREATE TABLE IF NOT EXISTS forms(
 id INTEGER PRIMARY KEY AUTOINCREMENT, user_id INTEGER, name TEXT, email TEXT, phone TEXT, topic TEXT, created_at TEXT,
 FOREIGN KEY(user_id) REFERENCES users(user_id));
CREATE TABLE IF NOT EXISTS bookings(
 id INTEGER PRIMARY KEY AUTOINCREMENT, user_id INTEGER, slot_iso TEXT UNIQUE, status TEXT, created_at TEXT,
 FOREIGN KEY(user_id) REFERENCES users(user_id));
'''
async def init_db():
    async with aiosqlite.connect(DB_PATH) as db:
        await db.executescript(SCHEMA)
        await db.commit()
async def upsert_user(user_id:int, username:str, first_name:str, last_name:str):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("INSERT INTO users(user_id, username, first_name, last_name, created_at) VALUES(?,?,?,?,datetime('now')) ON CONFLICT(user_id) DO UPDATE SET username=excluded.username, first_name=excluded.first_name, last_name=excluded.last_name",(user_id,username,first_name,last_name))
        await db.commit()
async def save_form(user_id:int, name:str, email:str, phone:str, topic:str):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("INSERT INTO forms(user_id, name, email, phone, topic, created_at) VALUES(?,?,?,?,?,datetime('now'))",(user_id,name,email,phone,topic))
        await db.commit()
async def is_slot_taken(slot_iso:str)->bool:
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute("SELECT 1 FROM bookings WHERE slot_iso=? AND status!='cancelled'",(slot_iso,)) as cur:
            row=await cur.fetchone(); return row is not None
async def reserve_slot(user_id:int, slot_iso:str)->int:
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("INSERT INTO bookings(user_id, slot_iso, status, created_at) VALUES(?,?, 'reserved', datetime('now'))",(user_id,slot_iso))
        await db.commit()
        async with db.execute("SELECT last_insert_rowid()") as cur:
            row=await cur.fetchone(); return int(row[0])
async def mark_paid(booking_id:int):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("UPDATE bookings SET status='paid' WHERE id=?",(booking_id,)); await db.commit()
async def latest_reserved_booking(user_id:int)->Optional[Tuple[int,str,str]]:
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute("SELECT id, slot_iso, status FROM bookings WHERE user_id=? ORDER BY id DESC LIMIT 1",(user_id,)) as cur:
            return await cur.fetchone()
