import aiosqlite
from datetime import datetime
import config

class Database:
    def __init__(self):
        self.db_path = config.DB_PATH
    
    async def init_db(self):
        """Crear todas las tablas"""
        async with aiosqlite.connect(self.db_path) as db:
            # Tabla de rifas
            await db.execute('''
                CREATE TABLE IF NOT EXISTS rifas (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    nombre TEXT NOT NULL,
                    premio TEXT NOT NULL,
                    valor_premio INTEGER NOT NULL,
                    precio_boleto INTEGER NOT NULL,
                    total_boletos INTEGER NOT NULL,
                    fecha_inicio TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    fecha_cierre TIMESTAMP,
                    estado TEXT DEFAULT 'activa'
                )
            ''')
            
            # Tabla de boletos
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
            
            # Tabla de vendedores
            await db.execute('''
                CREATE TABLE IF NOT EXISTS vendedores (
                    discord_id TEXT PRIMARY KEY,
                    nombre TEXT NOT NULL,
                    comision INTEGER DEFAULT 15,
                    total_ventas INTEGER DEFAULT 0,
                    comisiones_pendientes INTEGER DEFAULT 0,
                    comisiones_pagadas INTEGER DEFAULT 0,
                    fecha_registro TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            ''')
            
            # Tabla de clientes
            await db.execute('''
                CREATE TABLE IF NOT EXISTS clientes (
                    discord_id TEXT PRIMARY KEY,
                    nombre TEXT NOT NULL,
                    nick_ng TEXT,
                    total_compras INTEGER DEFAULT 0,
                    total_gastado INTEGER DEFAULT 0,
                    ultima_compra TIMESTAMP,
                    fecha_registro TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            ''')
            
            # Tabla de transacciones
            await db.execute('''
                CREATE TABLE IF NOT EXISTS transacciones (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    tipo TEXT NOT NULL,
                    boleto_id INTEGER,
                    monto INTEGER NOT NULL,
                    origen_id TEXT,
                    destino_id TEXT,
                    descripcion TEXT,
                    fecha TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (boleto_id) REFERENCES boletos (id)
                )
            ''')
            
            # Tabla de apartados
            await db.execute('''
                CREATE TABLE IF NOT EXISTS apartados (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    rifa_id INTEGER NOT NULL,
                    numero INTEGER NOT NULL,
                    usuario_id TEXT NOT NULL,
                    usuario_nick TEXT NOT NULL,
                    monto_apartado INTEGER NOT NULL,
                    fecha_apartado TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    fecha_limite TIMESTAMP,
                    estado TEXT DEFAULT 'activo',
                    FOREIGN KEY (rifa_id) REFERENCES rifas (id),
                    UNIQUE(rifa_id, numero)
                )
            ''')
            
            # Tabla de logs
            await db.execute('''
                CREATE TABLE IF NOT EXISTS logs (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    usuario_id TEXT,
                    accion TEXT NOT NULL,
                    detalles TEXT,
                    fecha TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            ''')
            
            # Tabla de balance de VP$
            await db.execute('''
                CREATE TABLE IF NOT EXISTS usuarios_balance (
                    discord_id TEXT PRIMARY KEY,
                    nombre TEXT NOT NULL,
                    balance INTEGER DEFAULT 0,
                    ultima_actualizacion TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            ''')
            
            await db.commit()
        print("✅ Base de datos inicializada")
    
    async def crear_rifa(self, nombre, premio, valor_premio, precio_boleto, total_boletos):
        """Crear nueva rifa"""
        async with aiosqlite.connect(self.db_path) as db:
            cursor = await db.execute('''
                INSERT INTO rifas (nombre, premio, valor_premio, precio_boleto, total_boletos, fecha_cierre)
                VALUES (?, ?, ?, ?, ?, datetime('now', '+7 days'))
            ''', (nombre, premio, valor_premio, precio_boleto, total_boletos))
            await db.commit()
            rifa_id = cursor.lastrowid
            await self.registrar_log(None, "crear_rifa", f"Rifa {rifa_id} creada: {nombre}")
            return rifa_id
    
    async def get_rifa_activa(self):
        """Obtener rifa activa actual"""
        async with aiosqlite.connect(self.db_path) as db:
            db.row_factory = aiosqlite.Row
            cursor = await db.execute('''
                SELECT * FROM rifas WHERE estado = 'activa' ORDER BY id DESC LIMIT 1
            ''')
            return await cursor.fetchone()
    
    async def get_boletos_vendidos(self, rifa_id):
        """Contar boletos vendidos"""
        async with aiosqlite.connect(self.db_path) as db:
            cursor = await db.execute('''
                SELECT COUNT(*) FROM boletos WHERE rifa_id = ? AND estado = 'pagado'
            ''', (rifa_id,))
            result = await cursor.fetchone()
            return result[0] if result else 0
    
    async def get_boletos_disponibles(self, rifa_id):
        """Obtener lista de números disponibles"""
        async with aiosqlite.connect(self.db_path) as db:
            cursor = await db.execute('''
                SELECT numero FROM boletos WHERE rifa_id = ? AND estado = 'pagado'
            ''', (rifa_id,))
            vendidos = await cursor.fetchall()
            vendidos = [v[0] for v in vendidos]
            
            cursor = await db.execute('SELECT total_boletos FROM rifas WHERE id = ?', (rifa_id,))
            total = await cursor.fetchone()
            total = total[0] if total else 0
            
            disponibles = [n for n in range(1, total + 1) if n not in vendidos]
            return disponibles
    
    async def comprar_boleto(self, rifa_id, numero, comprador_id, comprador_nick, vendedor_id=None):
        """Comprar un boleto"""
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
            
            if vendedor_id:
                comision = int(rifa['precio_boleto'] * config.DEFAULT_COMISION / 100)
                await db.execute('''
                    INSERT INTO vendedores (discord_id, nombre, total_ventas, comisiones_pendientes)
                    VALUES (?, ?, 1, ?)
                    ON CONFLICT(discord_id) DO UPDATE SET
                        total_ventas = total_ventas + 1,
                        comisiones_pendientes = comisiones_pendientes + ?
                ''', (vendedor_id, comprador_nick, comision, comision))
            
            await db.execute('''
                INSERT INTO transacciones (tipo, monto, origen_id, destino_id, descripcion)
                VALUES ('compra', ?, ?, ?, ?)
            ''', (rifa['precio_boleto'], comprador_id, vendedor_id, f"Boleto #{numero}"))
            
            await db.commit()
            await self.registrar_log(comprador_id, "compra", f"Boleto #{numero} en rifa {rifa_id}")
            return True, f"Boleto #{numero} comprado"
    
    async def registrar_log(self, usuario_id, accion, detalles):
        """Registrar acción en logs"""
        async with aiosqlite.connect(self.db_path) as db:
            await db.execute('''
                INSERT INTO logs (usuario_id, accion, detalles)
                VALUES (?, ?, ?)
            ''', (usuario_id, accion, detalles))
            await db.commit()
    
    async def cerrar_rifa(self, rifa_id):
        """Cerrar rifa para sorteo"""
        async with aiosqlite.connect(self.db_path) as db:
            await db.execute('''
                UPDATE rifas SET estado = 'cerrada' WHERE id = ?
            ''', (rifa_id,))
            await db.commit()
            await self.registrar_log(None, "cerrar_rifa", f"Rifa {rifa_id} cerrada")
    
    async def obtener_ganador(self, rifa_id, numero_ganador):
        """Registrar ganador de rifa"""
        async with aiosqlite.connect(self.db_path) as db:
            db.row_factory = aiosqlite.Row
            cursor = await db.execute('''
                SELECT * FROM boletos WHERE rifa_id = ? AND numero = ? AND estado = 'pagado'
            ''', (rifa_id, numero_ganador))
            boleto = await cursor.fetchone()
            
            if boleto:
                await db.execute('''
                    UPDATE rifas SET estado = 'finalizada' WHERE id = ?
                ''', (rifa_id,))
                
                await db.execute('''
                    INSERT INTO transacciones (tipo, monto, destino_id, descripcion)
                    VALUES ('premio', ?, ?, ?)
                ''', (0, boleto['comprador_id'], f"Ganó rifa {rifa_id} con #{numero_ganador}"))
                
                await db.commit()
                await self.registrar_log(None, "sorteo", f"Rifa {rifa_id} ganada por {boleto['comprador_nick']} #{numero_ganador}")
                return boleto
            return None
    
    async def get_estadisticas(self):
        """Obtener estadísticas generales"""
        async with aiosqlite.connect(self.db_path) as db:
            stats = {}
            
            cursor = await db.execute('SELECT COUNT(*) FROM rifas')
            stats['total_rifas'] = (await cursor.fetchone())[0]
            
            cursor = await db.execute('SELECT COUNT(*) FROM boletos')
            stats['total_boletos'] = (await cursor.fetchone())[0]
            
            cursor = await db.execute('SELECT SUM(monto) FROM transacciones WHERE tipo = "compra"')
            stats['total_recaudado'] = (await cursor.fetchone())[0] or 0
            
            cursor = await db.execute('SELECT COUNT(*) FROM clientes')
            stats['total_clientes'] = (await cursor.fetchone())[0]
            
            return stats
