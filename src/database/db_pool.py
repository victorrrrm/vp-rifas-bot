# src/database/db_pool.py
import aiosqlite
import os
from contextlib import asynccontextmanager

DB_PATH = os.getenv('DB_PATH', 'data/rifas.db')

class DatabasePool:
    def __init__(self):
        self._conn = None

    async def init(self):
        self._conn = await aiosqlite.connect(DB_PATH)
        self._conn.row_factory = aiosqlite.Row
        await self._conn.execute("PRAGMA foreign_keys = ON")

    @asynccontextmanager
    async def connection(self):
        if self._conn is None:
            await self.init()
        try:
            yield self._conn
        except Exception:
            await self._conn.rollback()
            raise

    async def execute(self, sql, params=None):
        async with self.connection() as conn:
            cursor = await conn.execute(sql, params or ())
            await conn.commit()
            return cursor

    async def fetchone(self, sql, params=None):
        async with self.connection() as conn:
            cursor = await conn.execute(sql, params or ())
            return await cursor.fetchone()

    async def fetchall(self, sql, params=None):
        async with self.connection() as conn:
            cursor = await conn.execute(sql, params or ())
            return await cursor.fetchall()

db_pool = DatabasePool()
