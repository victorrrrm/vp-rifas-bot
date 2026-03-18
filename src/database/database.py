import aiosqlite
from datetime import datetime
import config
import os

os.makedirs('data', exist_ok=True)

class Database:
    def __init__(self):
        self.db_path = 'data/rifas.db'
    
    async def init_db(self):
        async with aiosqlite.connect(self.db_path) as db:
            await db.execute('''
                CREATE TABLE IF NOT EXISTS rifas (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    nombre TEXT NOT NULL,
                    premio TEXT NOT NULL,
                    valor_premio INTEGER NOT NULL,
                    precio_boleto INTEGER NOT NULL,
                    total_boletos INTEGER NOT NULL,
                    fecha_inicio TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    estado TEXT DEFAULT 'activa'
                )
            ''')
            
            await db.execute('''
                CREATE TABLE IF NOT EXISTS boletos (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    rifa_id INTEGER NOT NULL,
                    numero INTEGER NOT NULL,
                    comprador_id TEXT NOT NULL,
                    comprador_nick TEXT NOT NULL,
                    vendedor_id TEXT,
                    precio_pagado INTEGER NOT NULL,
                    fecha_compra TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    estado TEXT DEFAULT 'pagado',
                    FOREIGN KEY (rifa_id) REFERENCES rifas (id),
                    UNIQUE(rifa_id, numero)
                )
            ''')
            
            await db.execute('''
                CREATE TABLE IF NOT EXISTS vendedores (
                    discord_id TEXT PRIMARY KEY,
                    nombre TEXT NOT NULL,
                    comision INTEGER DEFAULT 15,
                    total_ventas INTEGER DEFAULT 0,
                    comisiones_pendientes INTEGER DEFAULT 0,
                    fecha_registro TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            ''')
            
            await db.execute('''
                CREATE TABLE IF NOT EXISTS clientes (
                    discord_id TEXT PRIMARY KEY,
                    nombre TEXT NOT NULL,
                    total_compras INTEGER DEFAULT 0,
                    total_gastado INTEGER DEFAULT 0,
                    ultima_compra TIMESTAMP
                )
            ''')
            
            await db.execute('''
                CREATE TABLE IF NOT EXISTS usuarios_balance (
                    discord_id TEXT PRIMARY KEY,
                    nombre TEXT NOT NULL,
                    balance INTEGER DEFAULT 0,
                    ultima_actualizacion TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            ''')
            
            await db.commit()
    
    async def crear_rifa(self, nombre, premio, valor_premio, precio_boleto, total_boletos):
        async with aiosqlite.connect(self.db_path) as db:
            cursor = await db.execute('''
                INSERT INTO rifas (nombre, premio, valor_premio, precio_boleto, total_boletos)
                VALUES (?, ?, ?, ?, ?)
            ''', (nombre, premio, valor_premio, precio_boleto, total_boletos))
            await db.commit()
            return cursor.lastrowid
    
    async def get_rifa_activa(self):
        async with aiosqlite.connect(self.db_path) as db:
            db.row_factory = aiosqlite.Row
            cursor = await db.execute('''
                SELECT * FROM rifas WHERE estado = 'activa' ORDER BY id DESC LIMIT 1
            ''')
            return await cursor.fetchone()
    
    async def get_boletos_vendidos(self, rifa_id):
        async with aiosqlite.connect(self.db_path) as db:
            cursor = await db.execute('''
                SELECT COUNT(*) FROM boletos WHERE rifa_id = ? AND estado = 'pagado'
            ''', (rifa_id,))
            result = await cursor.fetchone()
            return result[0] if result else 0
    
    async def get_boletos_disponibles(self, rifa_id):
        async with aiosqlite.connect(self.db_path) as db:
            cursor = await db.execute('''
                SELECT numero FROM boletos WHERE rifa_id = ? AND estado = 'pagado'
            ''', (rifa_id,))
            vendidos = await cursor.fetchall()
            vendidos = [v[0] for v in vendidos]
            
            cursor = await db.execute('SELECT total_boletos FROM rifas WHERE id = ?', (rifa_id,))
            total = await cursor.fetchone()
            total = total[0] if total else 0
            
            return [n for n in range(1, total + 1) if n not in vendidos]
    
    async def comprar_boleto(self, rifa_id, numero, comprador_id, comprador_nick, vendedor_id=None):
        rifa = await self.get_rifa_activa()
        if not rifa:
            return False, "No hay rifa activa"
        
        disponibles = await self.get_boletos_disponibles(rifa_id)
        if numero not in disponibles:
            return False, "Número no disponible"
        
        async with aiosqlite.connect(self.db_path) as db:
            await db.execute('''
                INSERT INTO boletos (rifa_id, numero, comprador_id, comprador_nick, vendedor_id, precio_pagado)
                VALUES (?, ?, ?, ?, ?, ?)
            ''', (rifa_id, numero, comprador_id, comprador_nick, vendedor_id, rifa['precio_boleto']))
            
            await db.execute('''
                INSERT INTO clientes (discord_id, nombre, total_compras, total_gastado, ultima_compra)
                VALUES (?, ?, 1, ?, CURRENT_TIMESTAMP)
                ON CONFLICT(discord_id) DO UPDATE SET
                    total_compras = total_compras + 1,
                    total_gastado = total_gastado + ?,
                    ultima_compra = CURRENT_TIMESTAMP
            ''', (comprador_id, comprador_nick, rifa['precio_boleto'], rifa['precio_boleto']))
            
            await db.commit()
            return True, "Compra exitosa"
    
    async def cerrar_rifa(self, rifa_id):
        async with aiosqlite.connect(self.db_path) as db:
            await db.execute('''
                UPDATE rifas SET estado = 'cerrada' WHERE id = ?
            ''', (rifa_id,))
            await db.commit()
