import discord
from discord.ext import commands, tasks
import asyncio
import logging
import random
import aiosqlite
import csv
import shutil
import os
import sys
import traceback
import hashlib
import json
from datetime import datetime, timedelta
import config

# ============================================
# CONFIGURACIÓN DE PERSISTENCIA
# ============================================

if os.path.exists('/app/data'):
    DB_PATH = '/app/data/rifas.db'
    LOG_PATH = '/app/data/bot.log'
    VOLUME_MOUNTED = True
else:
    DB_PATH = 'data/rifas.db'
    LOG_PATH = 'src/logs/bot.log'
    VOLUME_MOUNTED = False

os.makedirs(os.path.dirname(DB_PATH) if os.path.dirname(DB_PATH) else 'data', exist_ok=True)
os.makedirs('src/logs', exist_ok=True)
os.makedirs('backups', exist_ok=True)

# ============================================
# CONFIGURACIÓN GLOBAL
# ============================================

VERSION = "7.0.0"
PREFIX = "!"
start_time = datetime.now()

# IDs de canales y roles (debes reemplazar con los tuyos)
CATEGORIA_RIFAS = 1482835014604296283
CATEGORIA_FRANQUICIAS = 1489816515774517278
CATEGORIA_TICKETS = None

ROLES_FRANQUICIA = {
    1: {'rol_id': 1489834608747876453, 'canal_id': 1489817105225486457},
    2: {'rol_id': 1489834653849358436, 'canal_id': 1489818797186486292},
    3: {'rol_id': 1489834697281245324, 'canal_id': 1489818846645981224},
    4: {'rol_id': 1489834736569552896, 'canal_id': 1489818896327377006},
    5: {'rol_id': 1489834781138096219, 'canal_id': 1489818943471357993}
}

ROLES_DISTRIBUIDORES = {
    'A': 1489828306801922221,
    'B': 1489828037703766288,
    'C': 1489827878152437870,
    'D': 1489827780181884998,
    'E': 1489827683629137940,
    'F': 1489827505715281980
}

ROLES = {
    'CEO': 1016130577595891713,
    'DIRECTOR': 1473799754457677967,
    'RIFAS': 1476836273493643438,
    'MIEMBRO': 1442736806234816603
}

ROLES_LOGROS = {
    'BALLENA': 1490113710012764192,
    'LEYENDA_CAJAS': 1490113919652462682,
    'INFLUENCER': 1490114070509125795,
    'MAGNATE': 1490114214046728355,
    'COLECCIONISTA': 1490114371634856039
}

COLORS = {
    'primary': 0xFFD700,
    'success': 0x00FF00,
    'error': 0xFF0000,
    'info': 0x0099FF,
    'warning': 0xFFA500,
}

# Variables globales de eventos (se persisten en DB)
evento_2x1 = False
evento_cashback_doble = False
evento_oferta_activa = False
evento_oferta_porcentaje = 0
jackpot_activo = False
jackpot_base = 0
jackpot_porcentaje = 0
jackpot_rifa_id = 0
jackpot_total = 0
jackpot_canal_id = 1486253228499931228
rifa_eliminacion_activa = False
rifa_eliminacion_total = 0
rifa_eliminacion_premio = ""
rifa_eliminacion_valor = 0
rifa_eliminacion_numeros = []
ranking_rifa = {}

# ============================================
# LOGGING
# ============================================

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(LOG_PATH),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# ============================================
# INTENTS Y BOT
# ============================================

intents = discord.Intents.default()
intents.message_content = True
intents.members = True

bot = commands.Bot(command_prefix=PREFIX, intents=intents)

# ============================================
# CLASE DATABASE (MÉTODOS PRINCIPALES)
# ============================================

class Database:
    async def init_db(self):
        async with aiosqlite.connect(DB_PATH) as db:
            await self._create_tables(db)
            await self._load_config(db)
    
    async def _create_tables(self, db):
        # Tabla de rifas
        await db.execute('''
            CREATE TABLE IF NOT EXISTS rifas (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                nombre TEXT NOT NULL,
                premio TEXT NOT NULL,
                valor_premio INTEGER NOT NULL,
                precio_boleto INTEGER NOT NULL,
                total_boletos INTEGER NOT NULL,
                numeros_bloqueados TEXT,
                fecha_inicio TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                estado TEXT DEFAULT 'activa',
                ganador_id TEXT,
                ganador_nick TEXT,
                ganador_numero INTEGER,
                fecha_finalizacion TIMESTAMP
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
                es_vip BOOLEAN DEFAULT 0,
                estado TEXT DEFAULT 'pagado',
                fecha_compra TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (rifa_id) REFERENCES rifas(id),
                UNIQUE(rifa_id, numero)
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
        # Tabla de balance
        await db.execute('''
            CREATE TABLE IF NOT EXISTS usuarios_balance (
                discord_id TEXT PRIMARY KEY,
                nombre TEXT NOT NULL,
                balance INTEGER DEFAULT 0,
                ultima_actualizacion TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        # Tabla de transacciones
        await db.execute('''
            CREATE TABLE IF NOT EXISTS transacciones (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                tipo TEXT NOT NULL,
                monto INTEGER NOT NULL,
                origen_id TEXT,
                destino_id TEXT,
                descripcion TEXT,
                estado TEXT DEFAULT 'pendiente',
                fecha TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        # Tabla de vendedores
        await db.execute('''
            CREATE TABLE IF NOT EXISTS vendedores (
                discord_id TEXT PRIMARY KEY,
                nombre TEXT NOT NULL,
                comision INTEGER DEFAULT 10,
                total_ventas INTEGER DEFAULT 0,
                comisiones_pendientes INTEGER DEFAULT 0,
                comisiones_pagadas INTEGER DEFAULT 0,
                fecha_registro TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        # Tabla de referidos
        await db.execute('''
            CREATE TABLE IF NOT EXISTS referidos_codigos (
                usuario_id TEXT PRIMARY KEY,
                codigo TEXT UNIQUE NOT NULL,
                fecha_creacion TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        await db.execute('''
            CREATE TABLE IF NOT EXISTS referidos_relaciones (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                referido_id TEXT UNIQUE NOT NULL,
                referidor_id TEXT NOT NULL,
                fecha_registro TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                primera_compra BOOLEAN DEFAULT 0,
                total_compras INTEGER DEFAULT 0,
                total_gastado INTEGER DEFAULT 0,
                comisiones_generadas INTEGER DEFAULT 0
            )
        ''')
        await db.execute('''
            CREATE TABLE IF NOT EXISTS referidos_config (
                id INTEGER PRIMARY KEY CHECK (id = 1),
                porcentaje_comision INTEGER DEFAULT 10,
                porcentaje_descuento INTEGER DEFAULT 10,
                descuento_activo BOOLEAN DEFAULT 1
            )
        ''')
        await db.execute('INSERT OR IGNORE INTO referidos_config (id) VALUES (1)')
        # Tabla de fidelización
        await db.execute('''
            CREATE TABLE IF NOT EXISTS fidelizacion (
                usuario_id TEXT PRIMARY KEY,
                gasto_total INTEGER DEFAULT 0,
                nivel TEXT DEFAULT 'BRONCE',
                fecha_actualizacion TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        await db.execute('''
            CREATE TABLE IF NOT EXISTS fidelizacion_config (
                nivel TEXT PRIMARY KEY,
                gasto_minimo INTEGER NOT NULL,
                gasto_maximo INTEGER,
                descuento INTEGER DEFAULT 0,
                boletos_gratis_por_cada INTEGER DEFAULT 0,
                cantidad_boletos_gratis INTEGER DEFAULT 0,
                acceso_anticipado_horas INTEGER DEFAULT 0,
                canal_vip BOOLEAN DEFAULT 0,
                rifas_exclusivas BOOLEAN DEFAULT 0
            )
        ''')
        niveles = [
            ('BRONCE', 0, 499999, 0, 0, 0, 0, 0, 0),
            ('PLATA', 500000, 999999, 5, 0, 0, 0, 0, 0),
            ('ORO', 1000000, 2499999, 10, 10, 2, 0, 0, 0),
            ('PLATINO', 2500000, 4999999, 15, 10, 2, 24, 0, 0),
            ('DIAMANTE', 5000000, 9999999, 20, 10, 3, 24, 1, 0),
            ('MASTER', 10000000, None, 25, 10, 4, 48, 1, 1)
        ]
        for n in niveles:
            await db.execute('INSERT OR IGNORE INTO fidelizacion_config VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)', n)
        # Tabla de cashback
        await db.execute('''
            CREATE TABLE IF NOT EXISTS cashback (
                usuario_id TEXT PRIMARY KEY,
                cashback_acumulado INTEGER DEFAULT 0,
                cashback_recibido INTEGER DEFAULT 0,
                ultima_actualizacion TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        await db.execute('''
            CREATE TABLE IF NOT EXISTS cashback_config (
                id INTEGER PRIMARY KEY CHECK (id = 1),
                porcentaje INTEGER DEFAULT 10,
                dia_pago TEXT DEFAULT 'LUNES',
                activo BOOLEAN DEFAULT 1
            )
        ''')
        await db.execute('INSERT OR IGNORE INTO cashback_config (id) VALUES (1)')
        # Tabla de cajas
        await db.execute('''
            CREATE TABLE IF NOT EXISTS cajas (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                tipo TEXT NOT NULL,
                nombre TEXT NOT NULL,
                precio INTEGER NOT NULL,
                premios TEXT NOT NULL,
                probabilidades TEXT NOT NULL,
                activo BOOLEAN DEFAULT 1
            )
        ''')
        await db.execute('''
            CREATE TABLE IF NOT EXISTS cajas_compradas (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                usuario_id TEXT NOT NULL,
                caja_id INTEGER NOT NULL,
                premio INTEGER,
                abierta BOOLEAN DEFAULT 0,
                fecha_compra TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                fecha_apertura TIMESTAMP,
                FOREIGN KEY (caja_id) REFERENCES cajas(id)
            )
        ''')
        await db.execute('''
            CREATE TABLE IF NOT EXISTS cajas_historial (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                usuario_id TEXT NOT NULL,
                usuario_nick TEXT NOT NULL,
                caja_nombre TEXT NOT NULL,
                premio_obtenido INTEGER NOT NULL,
                fecha TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        cajas_default = [
            ('comun', '📦 Caja Común', 5000, '[1000,2000,5000,10000,15000]', '[50,25,15,7,3]', 1),
            ('rara', '🎁 Caja Rara', 25000, '[5000,10000,25000,50000,100000]', '[45,25,15,10,5]', 1),
            ('epica', '✨ Caja Épica', 100000, '[25000,50000,100000,250000,500000]', '[40,25,15,12,8]', 1),
            ('legendaria', '👑 Caja Legendaria', 500000, '[100000,250000,500000,1000000,2000000]', '[35,25,18,12,10]', 1),
            ('misteriosa', '❓ Caja Misteriosa', 10000, '[0,1000,2500,5000,10000,25000,50000,100000,500000]', '[50,15,10,8,6,4,3,2,2]', 1)
        ]
        for c in cajas_default:
            await db.execute('INSERT OR IGNORE INTO cajas (tipo, nombre, precio, premios, probabilidades, activo) VALUES (?, ?, ?, ?, ?, ?)', c)
        # Tabla de distribuidores
        await db.execute('''
            CREATE TABLE IF NOT EXISTS distribuidores (
                discord_id TEXT PRIMARY KEY,
                nombre TEXT NOT NULL,
                nivel INTEGER DEFAULT 1,
                comision INTEGER DEFAULT 5,
                ventas_totales INTEGER DEFAULT 0,
                comisiones_pendientes INTEGER DEFAULT 0,
                comisiones_pagadas INTEGER DEFAULT 0,
                fecha_registro TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        await db.execute('''
            CREATE TABLE IF NOT EXISTS distribuidores_niveles (
                nivel INTEGER PRIMARY KEY,
                nombre TEXT,
                comision_base INTEGER,
                comision_extra INTEGER,
                descuento_mayorista INTEGER,
                productos_acceso TEXT,
                ventas_requeridas INTEGER,
                inversion_requerida INTEGER
            )
        ''')
        niveles_dist = [
            (1, 'Bronce', 5, 0, 5, '[]', 0, 0),
            (2, 'Plata', 7, 2, 10, '[]', 50, 100000),
            (3, 'Oro', 10, 3, 15, '[]', 200, 500000),
            (4, 'Platino', 12, 3, 20, '[]', 500, 1000000),
            (5, 'Diamante', 15, 5, 25, '[]', 1000, 2500000),
            (6, 'Elite', 20, 5, 30, '[]', 2500, 5000000)
        ]
        for nd in niveles_dist:
            await db.execute('INSERT OR IGNORE INTO distribuidores_niveles VALUES (?, ?, ?, ?, ?, ?, ?, ?)', nd)
        # Tabla de productos
        await db.execute('''
            CREATE TABLE IF NOT EXISTS productos (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                nombre TEXT NOT NULL,
                descripcion TEXT,
                precio_normal INTEGER NOT NULL,
                precio_mayorista INTEGER NOT NULL,
                nivel_minimo INTEGER DEFAULT 1,
                stock INTEGER DEFAULT -1,
                activo BOOLEAN DEFAULT 1
            )
        ''')
        await db.execute('''
            CREATE TABLE IF NOT EXISTS inventario_distribuidor (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                distribuidor_id TEXT NOT NULL,
                producto_id INTEGER NOT NULL,
                cantidad INTEGER DEFAULT 0,
                FOREIGN KEY (distribuidor_id) REFERENCES distribuidores(discord_id),
                FOREIGN KEY (producto_id) REFERENCES productos(id)
            )
        ''')
        productos_default = [
            ('Boleto Simple', 'Boleto para rifa normal', 25000, 22500, 1, -1, 1),
            ('Pack 5 Boletos', '5 boletos para rifa normal', 125000, 106250, 1, -1, 1),
            ('Pack 10 Boletos', '10 boletos para rifa normal', 250000, 200000, 2, -1, 1),
            ('Pack 50 Boletos', '50 boletos para rifa normal', 1250000, 937500, 3, -1, 1),
            ('Caja Común', 'Caja misteriosa común', 5000, 4250, 1, -1, 1),
            ('Caja Rara', 'Caja misteriosa rara', 25000, 21250, 2, -1, 1),
            ('Caja Épica', 'Caja misteriosa épica', 100000, 85000, 3, -1, 1),
            ('Rol VIP (30d)', 'Rol VIP por 30 días', 500000, 400000, 3, -1, 1),
            ('Multiplicador x2', 'Multiplicador x2 por 24h', 50000, 40000, 2, -1, 1)
        ]
        for p in productos_default:
            await db.execute('INSERT OR IGNORE INTO productos (nombre, descripcion, precio_normal, precio_mayorista, nivel_minimo, stock, activo) VALUES (?, ?, ?, ?, ?, ?, ?)', p)
        # Tabla de misiones diarias
        await db.execute('''
            CREATE TABLE IF NOT EXISTS misiones (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                nombre TEXT NOT NULL,
                descripcion TEXT,
                requisito TEXT NOT NULL,
                valor_requisito INTEGER NOT NULL,
                recompensa INTEGER NOT NULL,
                tipo TEXT DEFAULT 'diaria',
                activo BOOLEAN DEFAULT 1
            )
        ''')
        await db.execute('''
            CREATE TABLE IF NOT EXISTS progreso_misiones (
                usuario_id TEXT NOT NULL,
                mision_id INTEGER NOT NULL,
                progreso INTEGER DEFAULT 0,
                completada BOOLEAN DEFAULT 0,
                reclamada BOOLEAN DEFAULT 0,
                fecha TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (usuario_id, mision_id),
                FOREIGN KEY (mision_id) REFERENCES misiones(id)
            )
        ''')
        await db.execute('''
            CREATE TABLE IF NOT EXISTS rachas (
                usuario_id TEXT PRIMARY KEY,
                racha INTEGER DEFAULT 0,
                ultima_completada TIMESTAMP,
                mejor_racha INTEGER DEFAULT 0
            )
        ''')
        misiones_default = [
            ('Bienvenido', 'Envía 10 mensajes', 'mensajes', 10, 100, 'diaria', 1),
            ('Social', 'Reacciona a 5 mensajes', 'reacciones', 5, 50, 'diaria', 1),
            ('Comprador', 'Compra 1 boleto', 'compra', 1, 500, 'diaria', 1),
            ('Inversor', 'Invierte en el banco', 'inversion', 1, 1000, 'diaria', 1),
            ('Cajero', 'Abre 3 cajas', 'cajas', 3, 5000, 'diaria', 1),
            ('Maratón', 'Completa 7 días seguidos', 'racha', 7, 25000, 'especial', 1)
        ]
        for m in misiones_default:
            await db.execute('INSERT OR IGNORE INTO misiones (nombre, descripcion, requisito, valor_requisito, recompensa, tipo, activo) VALUES (?, ?, ?, ?, ?, ?, ?)', m)
        # Tabla de misiones semanales
        await db.execute('''
            CREATE TABLE IF NOT EXISTS misiones_semanales (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                nombre TEXT NOT NULL,
                descripcion TEXT,
                emoji TEXT,
                categoria TEXT,
                requisito_tipo TEXT NOT NULL,
                requisito_valor INTEGER NOT NULL,
                recompensa_vp INTEGER DEFAULT 0,
                recompensa_caja_tipo TEXT,
                recompensa_titulo TEXT,
                semana INTEGER,
                año INTEGER,
                activo BOOLEAN DEFAULT 1
            )
        ''')
        await db.execute('''
            CREATE TABLE IF NOT EXISTS misiones_semanales_progreso (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                usuario_id TEXT NOT NULL,
                mision_id INTEGER NOT NULL,
                progreso INTEGER DEFAULT 0,
                completada BOOLEAN DEFAULT 0,
                reclamada BOOLEAN DEFAULT 0,
                fecha_completada TIMESTAMP,
                FOREIGN KEY (mision_id) REFERENCES misiones_semanales(id),
                UNIQUE(usuario_id, mision_id)
            )
        ''')
        misiones_sem = [
            ('Comprador Semanal', 'Compra 50 boletos', '🎲', 'compras', 'cantidad', 50, 25000, 'epica', None, 0, 0, 1),
            ('Cajero Semanal', 'Abre 30 cajas', '📦', 'cajas', 'cantidad', 30, 15000, 'epica', None, 0, 0, 1),
            ('Referidor Semanal', '5 referidos nuevos', '👥', 'referidos', 'cantidad', 5, 50000, 'legendaria', None, 0, 0, 1),
            ('Subastador', 'Gana 3 subastas', '🎫', 'subastas', 'cantidad', 3, 30000, 'epica', 'Pujador', 0, 0, 1),
            ('Comerciante', 'Vende 10 boletos en marketplace', '🛒', 'marketplace', 'cantidad', 10, 20000, 'rara', None, 0, 0, 1),
            ('Generoso', 'Envía 5 regalos', '🎁', 'regalos', 'cantidad', 5, 10000, 'comun', None, 0, 0, 1)
        ]
        for ms in misiones_sem:
            await db.execute('INSERT OR IGNORE INTO misiones_semanales (nombre, descripcion, emoji, categoria, requisito_tipo, requisito_valor, recompensa_vp, recompensa_caja_tipo, recompensa_titulo, semana, año, activo) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)', ms)
        # Tabla de banco
        await db.execute('''
            CREATE TABLE IF NOT EXISTS inversiones (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                usuario_id TEXT NOT NULL,
                producto TEXT NOT NULL,
                monto INTEGER NOT NULL,
                interes INTEGER NOT NULL,
                fecha_inicio TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                fecha_fin TIMESTAMP,
                estado TEXT DEFAULT 'activa'
            )
        ''')
        await db.execute('''
            CREATE TABLE IF NOT EXISTS banco_productos (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                nombre TEXT,
                duracion_dias INTEGER,
                interes_porcentaje INTEGER,
                monto_minimo INTEGER,
                monto_maximo INTEGER,
                penalizacion_retiro INTEGER DEFAULT 10,
                activo BOOLEAN DEFAULT 1
            )
        ''')
        productos_banco = [
            ('Básico', 7, 5, 10000, 500000, 10, 1),
            ('Plus', 14, 12, 50000, 2000000, 15, 1),
            ('VIP', 30, 25, 200000, 10000000, 20, 1),
            ('Elite', 60, 40, 1000000, 50000000, 25, 1)
        ]
        for pb in productos_banco:
            await db.execute('INSERT OR IGNORE INTO banco_productos (nombre, duracion_dias, interes_porcentaje, monto_minimo, monto_maximo, penalizacion_retiro, activo) VALUES (?, ?, ?, ?, ?, ?, ?)', pb)
        await db.execute('''
            CREATE TABLE IF NOT EXISTS prestamos (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                usuario_id TEXT NOT NULL,
                monto INTEGER NOT NULL,
                interes INTEGER NOT NULL,
                monto_a_pagar INTEGER NOT NULL,
                fecha_inicio TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                fecha_fin TIMESTAMP,
                pagado BOOLEAN DEFAULT 0
            )
        ''')
        await db.execute('''
            CREATE TABLE IF NOT EXISTS transacciones_cambio (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                usuario_id TEXT NOT NULL,
                tipo TEXT NOT NULL,
                cantidad INTEGER NOT NULL,
                comision INTEGER NOT NULL,
                fecha TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        await db.execute('''
            CREATE TABLE IF NOT EXISTS banco_config (
                id INTEGER PRIMARY KEY CHECK (id = 1),
                tasa_compra REAL DEFAULT 0.9,
                tasa_venta REAL DEFAULT 1.1,
                interes_basico INTEGER DEFAULT 5,
                interes_plus INTEGER DEFAULT 12,
                interes_vip INTEGER DEFAULT 25
            )
        ''')
        await db.execute('INSERT OR IGNORE INTO banco_config (id) VALUES (1)')
        await db.execute('''
            CREATE TABLE IF NOT EXISTS banco_tasas_historial (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                tasa_compra REAL,
                tasa_venta REAL,
                fecha TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                modificado_por TEXT
            )
        ''')
        # Tabla de pagos pendientes
        await db.execute('''
            CREATE TABLE IF NOT EXISTS pagos_pendientes (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                usuario_id TEXT NOT NULL,
                monto INTEGER NOT NULL,
                metodo TEXT,
                captura TEXT,
                estado TEXT DEFAULT 'pendiente',
                fecha TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        # Tabla de tickets cambio
        await db.execute('''
            CREATE TABLE IF NOT EXISTS tickets_cambio (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                usuario_id TEXT NOT NULL,
                usuario_nick TEXT NOT NULL,
                canal_id TEXT NOT NULL,
                cantidad_ng INTEGER NOT NULL,
                tasa_compra REAL NOT NULL,
                cantidad_vp INTEGER NOT NULL,
                estado TEXT DEFAULT 'pendiente',
                fecha_creacion TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                fecha_procesado TIMESTAMP,
                procesado_por TEXT,
                mensaje_proceso_id TEXT,
                mensaje_completado_id TEXT
            )
        ''')
        await db.execute('''
            CREATE TABLE IF NOT EXISTS tickets_historial (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                ticket_id INTEGER,
                usuario_id TEXT,
                cantidad_ng INTEGER,
                cantidad_vp INTEGER,
                tasa_utilizada REAL,
                tiempo_procesado INTEGER,
                fecha_completado TIMESTAMP,
                procesado_por TEXT
            )
        ''')
        # Tabla de jackpot
        await db.execute('''
            CREATE TABLE IF NOT EXISTS jackpot (
                id INTEGER PRIMARY KEY CHECK (id = 1),
                rifa_id INTEGER NOT NULL,
                base INTEGER NOT NULL,
                porcentaje INTEGER NOT NULL,
                total INTEGER DEFAULT 0,
                activo BOOLEAN DEFAULT 0
            )
        ''')
        # Tabla de eventos activos
        await db.execute('''
            CREATE TABLE IF NOT EXISTS eventos_activos (
                id INTEGER PRIMARY KEY CHECK (id = 1),
                evento_2x1 BOOLEAN DEFAULT 0,
                cashback_doble BOOLEAN DEFAULT 0,
                oferta_activa BOOLEAN DEFAULT 0,
                oferta_porcentaje INTEGER DEFAULT 0,
                jackpot_activo BOOLEAN DEFAULT 0,
                jackpot_total INTEGER DEFAULT 0,
                jackpot_base INTEGER DEFAULT 0,
                jackpot_porcentaje INTEGER DEFAULT 0,
                jackpot_rifa_id INTEGER DEFAULT 0,
                ultima_actualizacion TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        await db.execute('INSERT OR IGNORE INTO eventos_activos (id) VALUES (1)')
        # Tabla de eventos programados
        await db.execute('''
            CREATE TABLE IF NOT EXISTS eventos_programados (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                nombre TEXT,
                tipo TEXT,
                valor INTEGER,
                fecha_inicio TIMESTAMP,
                fecha_fin TIMESTAMP,
                activo BOOLEAN DEFAULT 0,
                creado_por TEXT
            )
        ''')
        # Tabla de config global
        await db.execute('''
            CREATE TABLE IF NOT EXISTS config_global (
                key TEXT PRIMARY KEY,
                value TEXT,
                descripcion TEXT,
                actualizado_por TEXT,
                fecha_actualizacion TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        configs_default = [
            ('comision_vendedor', '10', '% comisión para vendedores'),
            ('comision_distribuidor_base', '5', '% base distribuidores'),
            ('cashback_porcentaje', '10', '% cashback normal'),
            ('referidos_porcentaje', '10', '% comisión referidos'),
            ('referidos_descuento', '10', '% descuento por referido'),
            ('precio_boleto_default', '25000', 'Precio por defecto'),
            ('max_boletos_por_compra', '50', 'Límite por transacción'),
            ('min_balance_retiro', '10000', 'Mínimo para retirar'),
            ('jackpot_porcentaje_default', '5', '% por compra al jackpot'),
            ('mantenimiento', 'false', 'Modo mantenimiento'),
            ('tasa_compra', '0.9', 'Tasa de cambio NG$ → VP$'),
            ('tasa_venta', '1.1', 'Tasa de cambio VP$ → NG$'),
            ('cambio_ng_minimo', '100000', 'Monto mínimo para cambiar NG$'),
            ('cambio_ng_maximo', '10000000', 'Monto máximo para cambiar NG$'),
            ('ticket_auto_cerrar_segundos', '10', 'Segundos antes de cerrar ticket'),
            ('categoria_tickets', '0', 'ID de la categoría de tickets')
        ]
        for key, val, desc in configs_default:
            await db.execute('INSERT OR IGNORE INTO config_global (key, value, descripcion) VALUES (?, ?, ?)', (key, val, desc))
        # Tabla de puntos revancha
        await db.execute('''
            CREATE TABLE IF NOT EXISTS puntos_revancha (
                usuario_id TEXT PRIMARY KEY,
                puntos INTEGER DEFAULT 0,
                ultima_actualizacion TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        # Tabla de códigos promocionales
        await db.execute('''
            CREATE TABLE IF NOT EXISTS codigos_promocionales (
                codigo TEXT PRIMARY KEY,
                recompensa INTEGER NOT NULL,
                creador_id TEXT NOT NULL,
                fecha_creacion TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                activo BOOLEAN DEFAULT 1
            )
        ''')
        await db.execute('''
            CREATE TABLE IF NOT EXISTS codigos_canjeados (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                codigo TEXT NOT NULL,
                usuario_id TEXT NOT NULL,
                fecha_canje TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (codigo) REFERENCES codigos_promocionales(codigo),
                UNIQUE(codigo, usuario_id)
            )
        ''')
        # Tabla de ganadores históricos
        await db.execute('''
            CREATE TABLE IF NOT EXISTS ganadores_historicos (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                usuario_id TEXT NOT NULL,
                usuario_nick TEXT NOT NULL,
                premio TEXT NOT NULL,
                rifa_nombre TEXT NOT NULL,
                numero INTEGER NOT NULL,
                fecha TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        # Tabla de subastas
        await db.execute('''
            CREATE TABLE IF NOT EXISTS subastas (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                item_nombre TEXT NOT NULL,
                item_descripcion TEXT,
                item_tipo TEXT,
                item_valor INTEGER,
                precio_base INTEGER NOT NULL,
                precio_actual INTEGER NOT NULL,
                ganador_id TEXT,
                ganador_nick TEXT,
                canal_id TEXT NOT NULL,
                mensaje_id TEXT,
                fecha_inicio TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                fecha_fin TIMESTAMP NOT NULL,
                estado TEXT DEFAULT 'activa',
                creada_por TEXT,
                ticket_creado BOOLEAN DEFAULT 0
            )
        ''')
        await db.execute('''
            CREATE TABLE IF NOT EXISTS pujas (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                subasta_id INTEGER NOT NULL,
                usuario_id TEXT NOT NULL,
                usuario_nick TEXT NOT NULL,
                monto INTEGER NOT NULL,
                fecha TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (subasta_id) REFERENCES subastas(id)
            )
        ''')
        await db.execute('''
            CREATE TABLE IF NOT EXISTS tickets_subasta (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                subasta_id INTEGER NOT NULL,
                usuario_id TEXT NOT NULL,
                canal_id TEXT NOT NULL,
                premio_entregado BOOLEAN DEFAULT 0,
                fecha_creacion TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                fecha_cierre TIMESTAMP,
                FOREIGN KEY (subasta_id) REFERENCES subastas(id)
            )
        ''')
        # Tabla de logros
        await db.execute('''
            CREATE TABLE IF NOT EXISTS logros (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                nombre TEXT NOT NULL,
                descripcion TEXT NOT NULL,
                emoji TEXT,
                categoria TEXT,
                condicion_tipo TEXT NOT NULL,
                condicion_valor INTEGER NOT NULL,
                recompensa_vp INTEGER DEFAULT 0,
                recompensa_rol_id TEXT,
                recompensa_titulo TEXT,
                orden INTEGER DEFAULT 0,
                activo BOOLEAN DEFAULT 1
            )
        ''')
        await db.execute('''
            CREATE TABLE IF NOT EXISTS usuarios_logros (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                usuario_id TEXT NOT NULL,
                logro_id INTEGER NOT NULL,
                progreso INTEGER DEFAULT 0,
                fecha_desbloqueo TIMESTAMP,
                notificado BOOLEAN DEFAULT 0,
                FOREIGN KEY (logro_id) REFERENCES logros(id),
                UNIQUE(usuario_id, logro_id)
            )
        ''')
        await db.execute('''
            CREATE TABLE IF NOT EXISTS usuarios_stats (
                usuario_id TEXT PRIMARY KEY,
                total_compras INTEGER DEFAULT 0,
                total_gastado INTEGER DEFAULT 0,
                total_cajas_abiertas INTEGER DEFAULT 0,
                total_referidos INTEGER DEFAULT 0,
                total_inversiones INTEGER DEFAULT 0,
                total_subastas_ganadas INTEGER DEFAULT 0,
                total_regalos_enviados INTEGER DEFAULT 0,
                total_regalos_recibidos INTEGER DEFAULT 0,
                total_ventas_marketplace INTEGER DEFAULT 0,
                total_compras_marketplace INTEGER DEFAULT 0,
                mejor_racha_misiones INTEGER DEFAULT 0,
                fecha_actualizacion TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        logros_default = [
            ('Novato', 'Compra 10 boletos', '🎲', 'compras', 'cantidad', 10, 500, None, None, 1, 1),
            ('Apostador', 'Compra 100 boletos', '🎲', 'compras', 'cantidad', 100, 5000, None, None, 2, 1),
            ('Ballena', 'Compra 1000 boletos', '🐳', 'compras', 'cantidad', 1000, 50000, str(ROLES_LOGROS['BALLENA']), None, 3, 1),
            ('Curioso', 'Abre 10 cajas', '📦', 'cajas', 'cantidad', 10, 1000, None, None, 4, 1),
            ('Coleccionista', 'Abre 100 cajas', '📦', 'cajas', 'cantidad', 100, 10000, None, None, 5, 1),
            ('Leyenda de Cajas', 'Abre 1000 cajas', '👑', 'cajas', 'cantidad', 1000, 100000, str(ROLES_LOGROS['LEYENDA_CAJAS']), None, 6, 1),
            ('Influencer', '10 referidos', '👥', 'referidos', 'cantidad', 10, 10000, None, None, 7, 1),
            ('Rey de Referidos', '50 referidos', '👑', 'referidos', 'cantidad', 50, 100000, str(ROLES_LOGROS['INFLUENCER']), None, 8, 1),
            ('Inversor', 'Invierte 500k VP$', '💰', 'inversiones', 'total_gastado', 500000, 5000, None, None, 9, 1),
            ('Magnate', 'Invierte 10M VP$', '💎', 'inversiones', 'total_gastado', 10000000, 500000, str(ROLES_LOGROS['MAGNATE']), None, 10, 1),
            ('Constancia', 'Racha de 7 días', '🔥', 'racha', 'racha', 7, 5000, None, None, 11, 1),
            ('Leyenda', 'Racha de 100 días', '🔥', 'racha', 'racha', 100, 500000, None, None, 12, 1),
            ('Pujador', 'Gana 1 subasta', '🎫', 'subastas', 'cantidad', 1, 2500, None, None, 13, 1),
            ('Coleccionista Subastas', 'Gana 10 subastas', '🏆', 'subastas', 'cantidad', 10, 50000, str(ROLES_LOGROS['COLECCIONISTA']), None, 14, 1)
        ]
        for l in logros_default:
            await db.execute('INSERT OR IGNORE INTO logros (nombre, descripcion, emoji, categoria, condicion_tipo, condicion_valor, recompensa_vp, recompensa_rol_id, recompensa_titulo, orden, activo) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)', l)
        # Tabla de notificaciones
        await db.execute('''
            CREATE TABLE IF NOT EXISTS notificaciones_preferencias (
                usuario_id TEXT PRIMARY KEY,
                notificar_nueva_rifa BOOLEAN DEFAULT 1,
                notificar_subasta BOOLEAN DEFAULT 1,
                notificar_logro BOOLEAN DEFAULT 1,
                notificar_cashback BOOLEAN DEFAULT 1,
                notificar_inversion BOOLEAN DEFAULT 1,
                notificar_ticket BOOLEAN DEFAULT 1,
                notificar_promocion BOOLEAN DEFAULT 1,
                horario_inicio INTEGER DEFAULT 8,
                horario_fin INTEGER DEFAULT 22,
                ultima_modificacion TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        await db.execute('''
            CREATE TABLE IF NOT EXISTS notificaciones_historial (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                usuario_id TEXT NOT NULL,
                tipo TEXT NOT NULL,
                titulo TEXT NOT NULL,
                mensaje TEXT NOT NULL,
                leida BOOLEAN DEFAULT 0,
                fecha_envio TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        await db.execute('''
            CREATE TABLE IF NOT EXISTS notificaciones_masivas (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                creador_id TEXT NOT NULL,
                rol_destino TEXT,
                titulo TEXT NOT NULL,
                mensaje TEXT NOT NULL,
                enviada BOOLEAN DEFAULT 0,
                fecha_creacion TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                fecha_envio TIMESTAMP
            )
        ''')
        # Tabla de regalos
        await db.execute('''
            CREATE TABLE IF NOT EXISTS regalos (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                remitente_id TEXT NOT NULL,
                remitente_nick TEXT NOT NULL,
                destinatario_id TEXT NOT NULL,
                destinatario_nick TEXT NOT NULL,
                tipo TEXT NOT NULL,
                cantidad INTEGER NOT NULL,
                item_id INTEGER,
                item_nombre TEXT,
                mensaje TEXT,
                estado TEXT DEFAULT 'pendiente',
                fecha_envio TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                fecha_respuesta TIMESTAMP
            )
        ''')
        await db.execute('''
            CREATE TABLE IF NOT EXISTS regalos_limites (
                usuario_id TEXT PRIMARY KEY,
                regalos_hoy INTEGER DEFAULT 0,
                ultimo_reset DATE,
                total_regalos_enviados INTEGER DEFAULT 0,
                total_vp_regalado INTEGER DEFAULT 0
            )
        ''')
        await db.execute('''
            CREATE TABLE IF NOT EXISTS regalos_config (
                id INTEGER PRIMARY KEY CHECK (id = 1),
                max_regalos_por_dia INTEGER DEFAULT 5,
                max_vp_por_regalo INTEGER DEFAULT 1000000,
                min_vp_por_regalo INTEGER DEFAULT 1000,
                requiere_confirmacion BOOLEAN DEFAULT 1,
                comision_porcentaje INTEGER DEFAULT 0,
                cooldown_segundos INTEGER DEFAULT 60
            )
        ''')
        await db.execute('INSERT OR IGNORE INTO regalos_config (id) VALUES (1)')
        # Tabla de marketplace
        await db.execute('''
            CREATE TABLE IF NOT EXISTS marketplace_listings (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                vendedor_id TEXT NOT NULL,
                vendedor_nick TEXT NOT NULL,
                rifa_id INTEGER NOT NULL,
                rifa_nombre TEXT NOT NULL,
                numero INTEGER NOT NULL,
                precio_venta INTEGER NOT NULL,
                acepta_ofertas BOOLEAN DEFAULT 1,
                precio_minimo INTEGER,
                estado TEXT DEFAULT 'activo',
                fecha_publicacion TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                fecha_expiracion TIMESTAMP,
                comprador_id TEXT,
                comprador_nick TEXT,
                fecha_venta TIMESTAMP
            )
        ''')
        await db.execute('''
            CREATE TABLE IF NOT EXISTS marketplace_ofertas (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                listing_id INTEGER NOT NULL,
                comprador_id TEXT NOT NULL,
                comprador_nick TEXT NOT NULL,
                monto INTEGER NOT NULL,
                mensaje TEXT,
                estado TEXT DEFAULT 'pendiente',
                fecha_oferta TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                fecha_respuesta TIMESTAMP,
                FOREIGN KEY (listing_id) REFERENCES marketplace_listings(id)
            )
        ''')
        await db.execute('''
            CREATE TABLE IF NOT EXISTS marketplace_config (
                id INTEGER PRIMARY KEY CHECK (id = 1),
                comision_porcentaje INTEGER DEFAULT 5,
                duracion_dias INTEGER DEFAULT 7,
                precio_minimo INTEGER DEFAULT 1000,
                oferta_minima_incremento INTEGER DEFAULT 500,
                activo BOOLEAN DEFAULT 1
            )
        ''')
        await db.execute('INSERT OR IGNORE INTO marketplace_config (id) VALUES (1)')
        await db.execute('''
            CREATE TABLE IF NOT EXISTS marketplace_historial (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                listing_id INTEGER,
                vendedor_id TEXT,
                comprador_id TEXT,
                numero INTEGER,
                precio_final INTEGER,
                comision INTEGER,
                fecha TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        # Tabla de personalización
        await db.execute('''
            CREATE TABLE IF NOT EXISTS personalizacion_items (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                tipo TEXT NOT NULL,
                nombre TEXT NOT NULL,
                descripcion TEXT,
                emoji TEXT,
                precio INTEGER NOT NULL,
                imagen_url TEXT,
                rareza TEXT DEFAULT 'comun',
                exclusivo BOOLEAN DEFAULT 0,
                activo BOOLEAN DEFAULT 1,
                orden INTEGER DEFAULT 0
            )
        ''')
        await db.execute('''
            CREATE TABLE IF NOT EXISTS personalizacion_usuarios (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                usuario_id TEXT NOT NULL,
                item_id INTEGER NOT NULL,
                equipado BOOLEAN DEFAULT 0,
                fecha_compra TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (item_id) REFERENCES personalizacion_items(id),
                UNIQUE(usuario_id, item_id)
            )
        ''')
        await db.execute('''
            CREATE TABLE IF NOT EXISTS perfiles_cache (
                usuario_id TEXT PRIMARY KEY,
                background_id INTEGER,
                marco_id INTEGER,
                badge_id INTEGER,
                efecto_id INTEGER,
                titulo_personalizado TEXT,
                biografia TEXT,
                color_embed INTEGER,
                ultima_actualizacion TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        items_pers = [
            ('background', 'Atardecer VP', 'Un hermoso atardecer dorado', '🌅', 10000, None, 'comun', 0, 1, 1),
            ('background', 'Casino Royale', 'Luces de neón y fortuna', '🎰', 50000, None, 'rara', 0, 1, 2),
            ('background', 'Cielo Estrellado', 'Estrellas infinitas', '🌌', 100000, None, 'epica', 0, 1, 3),
            ('background', 'Infinito VP', 'El infinito de la fortuna', '♾️', 500000, None, 'legendaria', 0, 1, 4),
            ('marco', 'Dorado', 'Marco dorado brillante', '✨', 25000, None, 'rara', 0, 1, 1),
            ('marco', 'Diamante', 'Marco de diamantes', '💎', 100000, None, 'epica', 0, 1, 2),
            ('marco', 'Fuego', 'Marco ardiente', '🔥', 250000, None, 'legendaria', 0, 1, 3),
            ('badge', 'Veterano', 'Por años de servicio', '🎖️', 0, None, 'especial', 1, 1, 1),
            ('badge', 'Ballena', 'Por comprar 1000 boletos', '🐳', 0, None, 'legendaria', 1, 1, 2),
            ('efecto', 'Partículas', 'Partículas brillantes', '✨', 50000, None, 'rara', 0, 1, 1),
            ('efecto', 'Arcoíris', 'Colores del arcoíris', '🌈', 150000, None, 'epica', 0, 1, 2)
        ]
        for it in items_pers:
            await db.execute('INSERT OR IGNORE INTO personalizacion_items (tipo, nombre, descripcion, emoji, precio, imagen_url, rareza, exclusivo, activo, orden) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)', it)
        # Tabla de ruleta
        await db.execute('''
            CREATE TABLE IF NOT EXISTS ruleta_config (
                id INTEGER PRIMARY KEY CHECK (id = 1),
                activo BOOLEAN DEFAULT 1,
                cooldown_horas INTEGER DEFAULT 24,
                premios TEXT NOT NULL,
                probabilidades TEXT NOT NULL,
                colores TEXT,
                descripciones TEXT,
                ultima_modificacion TIMESTAMP,
                modificado_por TEXT
            )
        ''')
        premios_def = json.dumps([100, 500, 1000, 5000, 10000, 50000, 100000, 500000])
        probs_def = json.dumps([30, 25, 20, 12, 8, 3, 1.5, 0.5])
        colores_def = json.dumps(["⚪", "🟢", "🟢", "🟡", "🟡", "🟠", "🔴", "🟣"])
        descs_def = json.dumps(["😅 Casi...", "👍 No está mal", "🙂 Bien", "🎉 Buena suerte", "🎊 Excelente", "🤯 Increíble", "💎 ESPECTACULAR", "👑 JACKPOT"])
        await db.execute('INSERT OR IGNORE INTO ruleta_config (id, activo, cooldown_horas, premios, probabilidades, colores, descripciones) VALUES (1, 1, 24, ?, ?, ?, ?)', (premios_def, probs_def, colores_def, descs_def))
        await db.execute('''
            CREATE TABLE IF NOT EXISTS ruleta_historial (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                usuario_id TEXT NOT NULL,
                usuario_nick TEXT NOT NULL,
                premio INTEGER NOT NULL,
                fecha TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        await db.execute('''
            CREATE TABLE IF NOT EXISTS ruleta_usuarios (
                usuario_id TEXT PRIMARY KEY,
                ultimo_giro TIMESTAMP,
                proximo_giro TIMESTAMP,
                racha_buena INTEGER DEFAULT 0,
                racha_mala INTEGER DEFAULT 0
            )
        ''')
        await db.execute('''
            CREATE TABLE IF NOT EXISTS ruleta_stats (
                id INTEGER PRIMARY KEY CHECK (id = 1),
                total_giros INTEGER DEFAULT 0,
                total_premiado INTEGER DEFAULT 0,
                mayor_premio INTEGER DEFAULT 0,
                ganador_mayor_premio TEXT,
                ultimo_gran_ganador TEXT,
                ultimo_gran_premio INTEGER,
                fecha_actualizacion TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        await db.execute('INSERT OR IGNORE INTO ruleta_stats (id) VALUES (1)')
        # Tabla de apuestas
        await db.execute('''
            CREATE TABLE IF NOT EXISTS apuestas (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                rifa_id INTEGER NOT NULL,
                usuario_id TEXT NOT NULL,
                usuario_nick TEXT NOT NULL,
                numero_apostado INTEGER NOT NULL,
                monto INTEGER NOT NULL,
                ganancia_potencial INTEGER,
                fecha_apuesta TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                estado TEXT DEFAULT 'activa',
                pagada BOOLEAN DEFAULT 0
            )
        ''')
        await db.execute('''
            CREATE TABLE IF NOT EXISTS apuestas_config (
                id INTEGER PRIMARY KEY CHECK (id = 1),
                activo BOOLEAN DEFAULT 1,
                apuesta_minima INTEGER DEFAULT 1000,
                apuesta_maxima INTEGER DEFAULT 100000,
                multiplicador_base INTEGER DEFAULT 10,
                multiplicador_especial INTEGER DEFAULT 50,
                numero_especial INTEGER DEFAULT 7,
                comision_porcentaje INTEGER DEFAULT 5,
                cooldown_segundos INTEGER DEFAULT 30
            )
        ''')
        await db.execute('INSERT OR IGNORE INTO apuestas_config (id) VALUES (1)')
        await db.execute('''
            CREATE TABLE IF NOT EXISTS apuestas_historial (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                apuesta_id INTEGER,
                rifa_id INTEGER,
                usuario_id TEXT,
                numero_apostado INTEGER,
                monto INTEGER,
                resultado TEXT,
                ganancia INTEGER,
                fecha_resolucion TIMESTAMP
            )
        ''')
        await db.execute('''
            CREATE TABLE IF NOT EXISTS apuestas_stats (
                usuario_id TEXT PRIMARY KEY,
                total_apuestas INTEGER DEFAULT 0,
                apuestas_ganadas INTEGER DEFAULT 0,
                total_apostado INTEGER DEFAULT 0,
                total_ganado INTEGER DEFAULT 0,
                mejor_ganancia INTEGER DEFAULT 0
            )
        ''')
        # Tabla de franquicias
        await db.execute('''
            CREATE TABLE IF NOT EXISTS franquicias (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                dueno_id TEXT UNIQUE,
                nivel INTEGER DEFAULT 1,
                nombre_franquicia TEXT,
                fecha_creacion TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                ventas_totales INTEGER DEFAULT 0,
                comisiones_acumuladas INTEGER DEFAULT 0
            )
        ''')
        await db.execute('''
            CREATE TABLE IF NOT EXISTS franquicias_rifas (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                franquicia_id INTEGER,
                rifa_id INTEGER,
                fecha_creacion TIMESTAMP,
                FOREIGN KEY (rifa_id) REFERENCES rifas(id)
            )
        ''')
        # Tabla de rifa eliminación (para persistencia)
        await db.execute('''
            CREATE TABLE IF NOT EXISTS rifa_eliminacion (
                id INTEGER PRIMARY KEY CHECK (id = 1),
                activa BOOLEAN DEFAULT 0,
                total INTEGER DEFAULT 0,
                premio TEXT,
                valor INTEGER DEFAULT 0,
                numeros TEXT,
                canal_id TEXT
            )
        ''')
        await db.commit()
    
    async def _load_config(self, db):
        global evento_2x1, evento_cashback_doble, evento_oferta_activa, evento_oferta_porcentaje
        global jackpot_activo, jackpot_total, jackpot_base, jackpot_porcentaje, jackpot_rifa_id
        cur = await db.execute('SELECT * FROM eventos_activos WHERE id = 1')
        ev = await cur.fetchone()
        if ev:
            evento_2x1 = ev[1] == 1
            evento_cashback_doble = ev[2] == 1
            evento_oferta_activa = ev[3] == 1
            evento_oferta_porcentaje = ev[4] or 0
            jackpot_activo = ev[5] == 1
            jackpot_total = ev[6] or 0
            jackpot_base = ev[7] or 0
            jackpot_porcentaje = ev[8] or 0
            jackpot_rifa_id = ev[9] or 0
        # Cargar rifa eliminación
        cur = await db.execute('SELECT * FROM rifa_eliminacion WHERE id = 1')
        re = await cur.fetchone()
        if re and re[1]:
            global rifa_eliminacion_activa, rifa_eliminacion_total, rifa_eliminacion_premio, rifa_eliminacion_valor, rifa_eliminacion_numeros
            rifa_eliminacion_activa = re[1] == 1
            rifa_eliminacion_total = re[2]
            rifa_eliminacion_premio = re[3]
            rifa_eliminacion_valor = re[4]
            rifa_eliminacion_numeros = json.loads(re[5]) if re[5] else []
    
    async def get_rifa_activa(self):
        async with aiosqlite.connect(DB_PATH) as db:
            db.row_factory = aiosqlite.Row
            cur = await db.execute('SELECT * FROM rifas WHERE estado = "activa" ORDER BY id DESC LIMIT 1')
            return await cur.fetchone()
    
    async def get_boletos_disponibles(self, rifa_id):
        async with aiosqlite.connect(DB_PATH) as db:
            cur = await db.execute('SELECT numero FROM boletos WHERE rifa_id = ?', (rifa_id,))
            vendidos = [r[0] for r in await cur.fetchall()]
            cur = await db.execute('SELECT total_boletos FROM rifas WHERE id = ?', (rifa_id,))
            total = (await cur.fetchone())[0]
            return [n for n in range(1, total+1) if n not in vendidos]
    
    async def get_boletos_vendidos(self, rifa_id):
        async with aiosqlite.connect(DB_PATH) as db:
            cur = await db.execute('SELECT COUNT(*) FROM boletos WHERE rifa_id = ?', (rifa_id,))
            return (await cur.fetchone())[0]

bot.db = Database()

# ============================================
# FUNCIONES AUXILIARES
# ============================================

async def tiene_rol(miembro, role_id):
    return any(r.id == role_id for r in miembro.roles)

async def check_ceo(ctx):
    if not ctx.guild:
        return False
    m = ctx.guild.get_member(ctx.author.id)
    return m and await tiene_rol(m, ROLES['CEO'])

async def check_admin(ctx):
    if not ctx.guild:
        return False
    m = ctx.guild.get_member(ctx.author.id)
    return m and (await tiene_rol(m, ROLES['CEO']) or await tiene_rol(m, ROLES['DIRECTOR']))

async def check_vendedor(ctx):
    if not ctx.guild:
        return False
    m = ctx.guild.get_member(ctx.author.id)
    return m and (await tiene_rol(m, ROLES['CEO']) or await tiene_rol(m, ROLES['DIRECTOR']) or await tiene_rol(m, ROLES['RIFAS']))

async def check_franquicia(ctx, nivel=None):
    if not ctx.guild:
        return False
    m = ctx.guild.get_member(ctx.author.id)
    if nivel:
        return m and (await tiene_rol(m, ROLES_FRANQUICIA[nivel]['rol_id']) or await tiene_rol(m, ROLES['CEO']))
    for n, d in ROLES_FRANQUICIA.items():
        if await tiene_rol(m, d['rol_id']):
            return True
    return await tiene_rol(m, ROLES['CEO'])

async def check_distribuidor(ctx):
    if not ctx.guild:
        return False
    m = ctx.guild.get_member(ctx.author.id)
    for rid in ROLES_DISTRIBUIDORES.values():
        if await tiene_rol(m, rid):
            return True
    return await tiene_rol(m, ROLES['CEO'])

async def verificar_canal(ctx, categoria_id=None):
    if not ctx.guild:
        await ctx.send("❌ Solo en servidores")
        return False
    cat = categoria_id if categoria_id else CATEGORIA_RIFAS
    if ctx.channel.category_id != cat:
        await ctx.send("❌ Comando no disponible aquí")
        return False
    return True

async def enviar_dm(usuario_id, titulo, mensaje):
    try:
        u = await bot.fetch_user(int(usuario_id))
        e = discord.Embed(title=titulo, description=mensaje, color=COLORS['info'])
        await u.send(embed=e)
    except:
        pass

async def obtener_descuento_usuario(usuario_id):
    global evento_oferta_activa, evento_oferta_porcentaje
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute('SELECT nivel FROM fidelizacion WHERE usuario_id = ?', (usuario_id,))
        r = await cur.fetchone()
        if r:
            cur2 = await db.execute('SELECT descuento FROM fidelizacion_config WHERE nivel = ?', (r[0],))
            desc = (await cur2.fetchone() or [0])[0]
        else:
            desc = 0
        if evento_oferta_activa:
            desc += evento_oferta_porcentaje
        return min(desc, 50)

async def actualizar_fidelizacion(usuario_id, monto):
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute('SELECT gasto_total FROM fidelizacion WHERE usuario_id = ?', (usuario_id,))
        r = await cur.fetchone()
        nuevo = (r[0] + monto) if r else monto
        await db.execute('INSERT OR REPLACE INTO fidelizacion (usuario_id, gasto_total) VALUES (?, ?)', (usuario_id, nuevo))
        cur2 = await db.execute('SELECT nivel FROM fidelizacion_config WHERE gasto_minimo <= ? AND (gasto_maximo >= ? OR gasto_maximo IS NULL) ORDER BY gasto_minimo DESC LIMIT 1', (nuevo, nuevo))
        niv = await cur2.fetchone()
        if niv:
            await db.execute('UPDATE fidelizacion SET nivel = ? WHERE usuario_id = ?', (niv[0], usuario_id))
        await db.commit()

async def aplicar_cashback(usuario_id, monto):
    global evento_cashback_doble
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute('SELECT porcentaje FROM cashback_config WHERE id = 1')
        porc = (await cur.fetchone() or [10])[0]
        if evento_cashback_doble:
            porc *= 2
        cb = int(monto * porc / 100)
        await db.execute('INSERT INTO cashback (usuario_id, cashback_acumulado) VALUES (?, ?) ON CONFLICT(usuario_id) DO UPDATE SET cashback_acumulado = cashback_acumulado + ?', (usuario_id, cb, cb))
        await db.commit()
        return cb

async def procesar_comision_referido(comprador_id, monto):
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute('SELECT referidor_id FROM referidos_relaciones WHERE referido_id = ?', (comprador_id,))
        ref = await cur.fetchone()
        if ref:
            com = int(monto * 10 / 100)
            await db.execute('UPDATE usuarios_balance SET balance = balance + ? WHERE discord_id = ?', (com, ref[0]))
            await db.commit()

async def procesar_comision_vendedor(vendedor_id, monto):
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute('SELECT value FROM config_global WHERE key = "comision_vendedor"')
        porc = int((await cur.fetchone() or [10])[0])
        com = int(monto * porc / 100)
        await db.execute('INSERT INTO vendedores (discord_id, nombre, comisiones_pendientes) VALUES (?, ?, ?) ON CONFLICT(discord_id) DO UPDATE SET comisiones_pendientes = comisiones_pendientes + ?', (vendedor_id, vendedor_id, com, com))
        await db.commit()

async def es_numero_vip(rifa_id, numero):
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute('SELECT numeros_bloqueados FROM rifas WHERE id = ?', (rifa_id,))
        r = await cur.fetchone()
        if not r or not r[0]:
            return False
        for parte in r[0].split(','):
            if '-' in parte:
                ini, fin = map(int, parte.split('-'))
                if ini <= numero <= fin:
                    return True
            elif int(parte) == numero:
                return True
        return False

async def actualizar_jackpot(monto):
    global jackpot_activo, jackpot_total, jackpot_porcentaje, jackpot_rifa_id
    if not jackpot_activo:
        return
    rifa = await bot.db.get_rifa_activa()
    if not rifa or rifa['id'] != jackpot_rifa_id:
        return
    aporte = int(monto * jackpot_porcentaje / 100)
    jackpot_total += aporte
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute('UPDATE jackpot SET total = ? WHERE id = 1', (jackpot_total,))
        await db.commit()

async def generar_codigo_unico(uid):
    return f"VP-{hashlib.md5(uid.encode()).hexdigest()[:8].upper()}"

async def obtener_o_crear_codigo(uid, nombre):
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute('SELECT codigo FROM referidos_codigos WHERE usuario_id = ?', (uid,))
        r = await cur.fetchone()
        if r:
            return r[0]
        cod = await generar_codigo_unico(uid)
        await db.execute('INSERT INTO referidos_codigos (usuario_id, codigo) VALUES (?, ?)', (uid, cod))
        await db.commit()
        return cod

async def verificar_logros(uid, nick, tipo, valor=1):
    async with aiosqlite.connect(DB_PATH) as db:
        # Actualizar stats
        cur = await db.execute('SELECT * FROM usuarios_stats WHERE usuario_id = ?', (uid,))
        stats = await cur.fetchone()
        if not stats:
            await db.execute('INSERT INTO usuarios_stats (usuario_id) VALUES (?)', (uid,))
            stats = (uid, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, datetime.now())
        if tipo == 'compra':
            nc = stats[1] + valor
            ng = stats[2] + valor
            await db.execute('UPDATE usuarios_stats SET total_compras = ?, total_gastado = ? WHERE usuario_id = ?', (nc, ng, uid))
            stats = list(stats); stats[1]=nc; stats[2]=ng
        elif tipo == 'caja':
            nc = stats[3] + valor
            await db.execute('UPDATE usuarios_stats SET total_cajas_abiertas = ? WHERE usuario_id = ?', (nc, uid))
            stats[3] = nc
        elif tipo == 'referido':
            nr = stats[4] + valor
            await db.execute('UPDATE usuarios_stats SET total_referidos = ? WHERE usuario_id = ?', (nr, uid))
            stats[4] = nr
        elif tipo == 'subasta':
            ns = stats[6] + valor
            await db.execute('UPDATE usuarios_stats SET total_subastas_ganadas = ? WHERE usuario_id = ?', (ns, uid))
            stats[6] = ns
        # Verificar logros
        cur = await db.execute('SELECT * FROM logros WHERE activo = 1')
        logros = await cur.fetchall()
        for l in logros:
            cur2 = await db.execute('SELECT * FROM usuarios_logros WHERE usuario_id = ? AND logro_id = ?', (uid, l[0]))
            if await cur2.fetchone():
                continue
            cumplido = False
            if l[6] == 'cantidad':
                if l[5] == 'compras' and stats[1] >= l[7]:
                    cumplido = True
                elif l[5] == 'cajas' and stats[3] >= l[7]:
                    cumplido = True
                elif l[5] == 'referidos' and stats[4] >= l[7]:
                    cumplido = True
                elif l[5] == 'subastas' and stats[6] >= l[7]:
                    cumplido = True
            elif l[6] == 'total_gastado' and stats[2] >= l[7]:
                cumplido = True
            elif l[6] == 'racha':
                cur3 = await db.execute('SELECT mejor_racha FROM rachas WHERE usuario_id = ?', (uid,))
                rr = await cur3.fetchone()
                if rr and rr[0] >= l[7]:
                    cumplido = True
            if cumplido:
                await db.execute('INSERT INTO usuarios_logros (usuario_id, logro_id, fecha_desbloqueo) VALUES (?, ?, ?)', (uid, l[0], datetime.now()))
                if l[8] > 0:
                    await db.execute('UPDATE usuarios_balance SET balance = balance + ? WHERE discord_id = ?', (l[8], uid))
                if l[9]:
                    g = bot.get_guild(config.GUILD_ID)
                    if g:
                        m = g.get_member(int(uid))
                        rol = g.get_role(int(l[9]))
                        if m and rol and rol not in m.roles:
                            await m.add_roles(rol)
                await enviar_dm(uid, f"🏆 Logro: {l[1]}", f"Desbloqueaste {l[3]} **{l[1]}**\nRecompensa: ${l[8]:,} VP$")
        await db.commit()

async def enviar_notificacion(uid, tipo, titulo, mensaje):
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute('SELECT * FROM notificaciones_preferencias WHERE usuario_id = ?', (uid,))
        pref = await cur.fetchone()
        if pref:
            if tipo == 'logro' and not pref[3]:
                return
            if tipo == 'subasta' and not pref[2]:
                return
            if tipo == 'cashback' and not pref[4]:
                return
        await db.execute('INSERT INTO notificaciones_historial (usuario_id, tipo, titulo, mensaje) VALUES (?, ?, ?, ?)', (uid, tipo, titulo, mensaje))
        await db.commit()
    await enviar_dm(uid, titulo, mensaje)

# ============================================
# TAREAS AUTOMÁTICAS
# ============================================

@tasks.loop(minutes=1)
async def verificar_subastas():
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute('SELECT * FROM subastas WHERE estado = "activa" AND fecha_fin <= datetime("now")')
        subs = await cur.fetchall()
        for s in subs:
            await finalizar_subasta(s[0])

async def finalizar_subasta(sid):
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        cur = await db.execute('SELECT * FROM subastas WHERE id = ?', (sid,))
        s = await cur.fetchone()
        if not s:
            return
        cur = await db.execute('SELECT * FROM pujas WHERE subasta_id = ? ORDER BY monto DESC LIMIT 1', (sid,))
        ganador = await cur.fetchone()
        if ganador:
            await db.execute('UPDATE subastas SET estado = "finalizada", ganador_id = ?, ganador_nick = ?, precio_actual = ? WHERE id = ?', (ganador['usuario_id'], ganador['usuario_nick'], ganador['monto'], sid))
            await crear_ticket_subasta(sid, ganador['usuario_id'], ganador['usuario_nick'], s['item_nombre'], ganador['monto'])
            await verificar_logros(ganador['usuario_id'], ganador['usuario_nick'], 'subasta', 1)
            await enviar_notificacion(ganador['usuario_id'], 'subasta', '🎉 Ganaste una subasta', f"Ganaste {s['item_nombre']} por ${ganador['monto']:,} VP$")
        else:
            await db.execute('UPDATE subastas SET estado = "cancelada" WHERE id = ?', (sid,))
        await db.commit()

async def crear_ticket_subasta(sid, uid, nick, item, monto):
    guild = bot.get_guild(config.GUILD_ID)
    if not guild:
        return
    cat = guild.get_channel(CATEGORIA_TICKETS)
    if not cat:
        return
    overwrites = {guild.default_role: discord.PermissionOverwrite(read_messages=False), guild.get_member(int(uid)): discord.PermissionOverwrite(read_messages=True, send_messages=True)}
    for rid in [ROLES['CEO'], ROLES['DIRECTOR']]:
        r = guild.get_role(rid)
        if r:
            overwrites[r] = discord.PermissionOverwrite(read_messages=True, send_messages=True)
    canal = await cat.create_text_channel(f"ticket-subasta-{sid}", overwrites=overwrites)
    e = discord.Embed(title="🎉 FELICIDADES", description=f"Ganaste la subasta #{sid}", color=COLORS['success'])
    e.add_field(name="🏆 Premio", value=item, inline=True)
    e.add_field(name="💰 Monto", value=f"${monto:,} VP$", inline=True)
    e.set_footer(text="Un staff te entregará el premio. Usa !ticketcerrar para cerrar")
    await canal.send(f"<@{uid}>", embed=e)
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute('INSERT INTO tickets_subasta (subasta_id, usuario_id, canal_id) VALUES (?, ?, ?)', (sid, uid, str(canal.id)))
        await db.commit()

@tasks.loop(hours=24)
async def reset_misiones_semanales():
    semana = datetime.now().isocalendar()[1]
    año = datetime.now().year
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute('DELETE FROM misiones_semanales_progreso')
        await db.execute('UPDATE misiones_semanales SET semana = ?, año = ?', (semana, año))
        await db.commit()

@tasks.loop(hours=24)
async def backup_automatico():
    fecha = datetime.now().strftime("%Y%m%d_%H%M%S")
    shutil.copy2(DB_PATH, f"backups/backup_{fecha}.db")
    backups = sorted(os.listdir('backups'))
    while len(backups) > 30:
        os.remove(os.path.join('backups', backups.pop(0)))

@tasks.loop(minutes=5)
async def actualizar_estado_jackpot():
    global jackpot_activo, jackpot_total, jackpot_canal_id
    if not jackpot_activo:
        return
    canal = bot.get_channel(jackpot_canal_id)
    if not canal:
        return
    async for msg in canal.history(limit=50):
        if msg.author == bot.user and msg.pinned:
            e = discord.Embed(title="🎰 JACKPOT ACTIVO", description=f"**Premio:** ${jackpot_total:,} VP$", color=0xFFD700)
            e.add_field(name="💎 Base", value=f"${jackpot_base:,}", inline=True)
            e.add_field(name="📊 % por boleto", value=f"{jackpot_porcentaje}%", inline=True)
            await msg.edit(embed=e)
            return
    msg = await canal.send(embed=discord.Embed(title="🎰 JACKPOT ACTIVO", description=f"**Premio:** ${jackpot_total:,} VP$", color=0xFFD700))
    await msg.pin()

# ============================================
# EVENTO ON_READY
# ============================================

@bot.event
async def on_ready():
    await bot.db.init_db()
    await crear_categoria_tickets()
    verificar_subastas.start()
    reset_misiones_semanales.start()
    backup_automatico.start()
    actualizar_estado_jackpot.start()
    logger.info(f"✅ Bot conectado como {bot.user}")
    logger.info(f"🌐 En {len(bot.guilds)} servidores")
    logger.info(f"📦 VP Rifas Bot v{VERSION}")

async def crear_categoria_tickets():
    global CATEGORIA_TICKETS
    g = bot.get_guild(config.GUILD_ID)
    if g:
        for c in g.categories:
            if c.name == "🎫 TICKETS":
                CATEGORIA_TICKETS = c.id
                return
        c = await g.create_category("🎫 TICKETS")
        CATEGORIA_TICKETS = c.id
        await c.set_permissions(g.default_role, read_messages=False)

# ============================================
# COMANDO AYUDA (PAGINADO)
# ============================================

@bot.command(name="ayuda")
async def cmd_ayuda(ctx):
    if not await verificar_canal(ctx):
        return
    m = ctx.guild.get_member(ctx.author.id)
    es_ceo = await tiene_rol(m, ROLES['CEO'])
    es_admin = await tiene_rol(m, ROLES['DIRECTOR'])
    es_vendedor = await tiene_rol(m, ROLES['RIFAS'])
    
    p1 = discord.Embed(title="🎟️ SISTEMA DE RIFAS VP", description=f"Prefijo: `{PREFIX}` | Versión: {VERSION}\n📄 **Página 1/8**", color=COLORS['primary'])
    p1.add_field(name="👤 BÁSICOS", value="```\n!rifa\n!comprarrandom [cantidad]\n!misboletos\n!balance [@user]\n!topvp\n!ranking\n!historial\n!celiminacion [num]\n!beliminacion\n!mispuntos\n```", inline=False)
    p2 = discord.Embed(title="🎟️ SISTEMA DE RIFAS VP", description=f"Prefijo: `{PREFIX}` | Versión: {VERSION}\n📄 **Página 2/8**", color=COLORS['primary'])
    p2.add_field(name="🤝 REFERIDOS", value="```\n!codigo\n!usar [codigo]\n!misreferidos\n```", inline=False)
    p2.add_field(name="🏆 FIDELIZACIÓN", value="```\n!nivel\n!topgastadores\n!cashback\n!topcashback\n!verniveles\n```", inline=False)
    p3 = discord.Embed(title="🎟️ SISTEMA DE RIFAS VP", description=f"Prefijo: `{PREFIX}` | Versión: {VERSION}\n📄 **Página 3/8**", color=COLORS['primary'])
    p3.add_field(name="📦 CAJAS", value="```\n!cajas\n!comprarcaja [tipo] [cant]\n!miscajas\n!abrircaja [id]\n!topcajas\n```", inline=False)
    p3.add_field(name="🎡 RULETA", value="```\n!ruleta\n!ruleta_stats\n```", inline=False)
    p3.add_field(name="🎲 APUESTAS", value="```\n!apostar [num] [monto]\n!mis_apuestas\n```", inline=False)
    p4 = discord.Embed(title="🎟️ SISTEMA DE RIFAS VP", description=f"Prefijo: `{PREFIX}` | Versión: {VERSION}\n📄 **Página 4/8**", color=COLORS['primary'])
    p4.add_field(name="🏦 BANCO", value="```\n!banco\n!invertir [prod] [monto]\n!misinversiones\n!retirar [id]\n!cambiarng [monto]\n```", inline=False)
    p4.add_field(name="🎫 SUBASTAS", value="```\n!subastas\n!pujar [id] [monto]\n!mis_pujas\n```", inline=False)
    p5 = discord.Embed(title="🎟️ SISTEMA DE RIFAS VP", description=f"Prefijo: `{PREFIX}` | Versión: {VERSION}\n📄 **Página 5/8**", color=COLORS['primary'])
    p5.add_field(name="🛒 MARKETPLACE", value="```\n!marketplace\n!vender_boleto [num] [precio]\n!comprar_boleto [id]\n!ofertar [id] [monto]\n!mis_listados\n```", inline=False)
    p5.add_field(name="🎁 REGALOS", value="```\n!regalar [@user] [monto]\n!solicitudes\n!aceptar [id]\n!rechazar [id]\n```", inline=False)
    p6 = discord.Embed(title="🎟️ SISTEMA DE RIFAS VP", description=f"Prefijo: `{PREFIX}` | Versión: {VERSION}\n📄 **Página 6/8**", color=COLORS['primary'])
    p6.add_field(name="📋 MISIONES", value="```\n!misiones\n!misiones_semanales\n!miracha\n!reclamar [id]\n```", inline=False)
    p6.add_field(name="🎨 PERFIL", value="```\n!perfil [@user]\n!tienda_perfil\n!comprar_perfil [id]\n!equipar [id]\n!perfil_set [campo] [valor]\n```", inline=False)
    p7 = discord.Embed(title="🎟️ SISTEMA DE RIFAS VP", description=f"Prefijo: `{PREFIX}` | Versión: {VERSION}\n📄 **Página 7/8**", color=COLORS['primary'])
    p7.add_field(name="💰 VENDEDORES", value="```\n!vender [@user] [num]\n!venderrandom [@user] [cant]\n!misventas\n!listaboletos\n```", inline=False)
    p7.add_field(name="📦 DISTRIBUIDORES", value="```\n!distribuidor\n!productos\n!comprar_producto [nombre] [cant]\n!mis_productos\n```", inline=False)
    p7.add_field(name="👑 FRANQUICIAS", value="```\n!franquicia\n!franquicia_rifa [premio] [precio] [total]\n!franquicia_stats\n```", inline=False)
    p8 = discord.Embed(title="🎟️ SISTEMA DE RIFAS VP", description=f"Prefijo: `{PREFIX}` | Versión: {VERSION}\n📄 **Página 8/8**", color=COLORS['primary'])
    p8.add_field(name="🎯 DIRECTORES", value="```\n!crearifa [premio] [precio] [total] [bloq]\n!cerrarifa\n!iniciarsorteo [num]\n!finalizarrifa [id] [num]\n!vendedoradd [@user] [%]\n!vercomisiones\n!pagarcomisiones\n!verboletos [@user]\n!subasta crear [item] [base] [horas]\n!rifaeliminacion [total] [premio] [valor]\n```", inline=False)
    p8.add_field(name="👑 CEO", value="```\n!acreditarvp [@user] [monto]\n!retirarvp [@user] [monto]\n!procesarvp [@user]\n!procesadovp [@user] [vp]\n!setrefcomision [%]\n!setcashback [%]\n!pagarcashback\n!evento 2x1/cashbackdoble/oferta\n!config get/set/list\n!backup\n!resetallsistema\n!version\n!caja crear/editar\n!mision crear/editar\n!ruleta_config set\n!apuestas_config set\n!ticketcerrar\n```", inline=False)
    
    paginas = [p1, p2, p3, p4, p5, p6, p7, p8]
    if not es_vendedor and not es_admin and not es_ceo:
        paginas = paginas[:6]
    elif not es_admin and not es_ceo:
        paginas = paginas[:7]
    
    msg = await ctx.send(embed=paginas[0])
    if len(paginas) > 1:
        await msg.add_reaction("⬅️")
        await msg.add_reaction("➡️")
        actual = 0
        def check(r, u):
            return u == ctx.author and r.message.id == msg.id and str(r.emoji) in ["⬅️", "➡️"]
        while True:
            try:
                r, u = await bot.wait_for("reaction_add", timeout=60, check=check)
                if str(r.emoji) == "➡️" and actual < len(paginas)-1:
                    actual += 1
                    await msg.edit(embed=paginas[actual])
                elif str(r.emoji) == "⬅️" and actual > 0:
                    actual -= 1
                    await msg.edit(embed=paginas[actual])
                await msg.remove_reaction(r.emoji, u)
            except:
                try:
                    await msg.clear_reactions()
                except:
                    pass
                break

# ============================================
# COMANDOS BÁSICOS (YA FUNCIONAN)
# ============================================

@bot.command(name="version")
async def cmd_version(ctx):
    await ctx.send(embed=discord.Embed(title="🤖 VP RIFAS BOT", description=f"**Versión:** `{VERSION}`\n**Estado:** 🟢 Activo", color=COLORS['primary']))

@bot.command(name="rifa")
async def cmd_rifa(ctx):
    if not await verificar_canal(ctx):
        return
    r = await bot.db.get_rifa_activa()
    if not r:
        return await ctx.send(embed=discord.Embed(title="❌ Error", description="No hay rifa activa", color=COLORS['error']))
    e = discord.Embed(title=f"🎟️ {r['nombre']}", description=f"**{r['premio']}**", color=COLORS['primary'])
    e.add_field(name="💰 Precio", value=f"${r['precio_boleto']:,} VP$")
    await ctx.send(embed=e)

@bot.command(name="comprarrandom")
async def cmd_comprar_random(ctx, cantidad: int = 1):
    if not await verificar_canal(ctx):
        return
    if not 1 <= cantidad <= 50:
        return await ctx.send(embed=discord.Embed(title="❌ Error", description="Cantidad 1-50", color=COLORS['error']))
    r = await bot.db.get_rifa_activa()
    if not r:
        return await ctx.send(embed=discord.Embed(title="❌ Error", description="No hay rifa activa", color=COLORS['error']))
    disp = await bot.db.get_boletos_disponibles(r['id'])
    disp_filt = [n for n in disp if not await es_numero_vip(r['id'], n)]
    if len(disp_filt) < cantidad:
        return await ctx.send(embed=discord.Embed(title="❌ Error", description="No hay suficientes boletos disponibles", color=COLORS['error']))
    desc = await obtener_descuento_usuario(str(ctx.author.id))
    global evento_2x1
    pagar = cantidad // 2 + (cantidad % 2) if evento_2x1 else cantidad
    total = r['precio_boleto'] * pagar
    final = int(total * (100 - desc) / 100)
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute('SELECT balance FROM usuarios_balance WHERE discord_id = ?', (str(ctx.author.id),))
        bal = (await cur.fetchone() or [0])[0]
        if bal < final:
            return await ctx.send(embed=discord.Embed(title="❌ Error", description=f"Necesitas ${final:,} VP$", color=COLORS['error']))
        sel = random.sample(disp_filt, cantidad)
        await db.execute('UPDATE usuarios_balance SET balance = balance - ? WHERE discord_id = ?', (final, str(ctx.author.id)))
        for n in sel:
            await db.execute('INSERT INTO boletos (rifa_id, numero, comprador_id, comprador_nick, precio_pagado) VALUES (?, ?, ?, ?, ?)', (r['id'], n, str(ctx.author.id), ctx.author.name, r['precio_boleto']))
        await db.commit()
    await actualizar_fidelizacion(str(ctx.author.id), final)
    await aplicar_cashback(str(ctx.author.id), final)
    await procesar_comision_referido(str(ctx.author.id), final)
    await actualizar_jackpot(final)
    await verificar_logros(str(ctx.author.id), ctx.author.name, 'compra', cantidad)
    await enviar_dm(str(ctx.author.id), "✅ Compra realizada", f"Compraste {len(sel)} boletos: {', '.join(map(str, sel))}\nTotal: ${final:,}\nDescuento: {desc}%")
    await ctx.message.delete()
    await ctx.send(embed=discord.Embed(title="✅ Compra realizada", description="Revisa tu DM", color=COLORS['success']))

@bot.command(name="misboletos")
async def cmd_mis_boletos(ctx):
    if not await verificar_canal(ctx):
        return
    r = await bot.db.get_rifa_activa()
    if not r:
        return await ctx.send(embed=discord.Embed(title="❌ Error", description="No hay rifa activa", color=COLORS['error']))
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute('SELECT numero FROM boletos WHERE rifa_id = ? AND comprador_id = ?', (r['id'], str(ctx.author.id)))
        b = await cur.fetchall()
    if not b:
        return await ctx.send(embed=discord.Embed(title="❌ Info", description="No tienes boletos", color=COLORS['info']))
    await ctx.send(embed=discord.Embed(title="🎟️ Tus boletos", description=f"Números: {', '.join(str(x[0]) for x in b)}", color=COLORS['primary']))

@bot.command(name="balance")
async def cmd_balance(ctx, usuario: discord.Member = None):
    if not await verificar_canal(ctx):
        return
    target = usuario or ctx.author
    if usuario and not await check_admin(ctx):
        return await ctx.send(embed=discord.Embed(title="❌ Error", description="Sin permiso", color=COLORS['error']))
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute('SELECT balance FROM usuarios_balance WHERE discord_id = ?', (str(target.id),))
        bal = (await cur.fetchone() or [0])[0]
    await ctx.send(embed=discord.Embed(title=f"💰 Balance de {target.name}", description=f"**{bal:,} VP$**", color=COLORS['primary']))

@bot.command(name="topvp")
async def cmd_top_vp(ctx):
    if not await verificar_canal(ctx):
        return
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute('SELECT nombre, balance FROM usuarios_balance WHERE balance > 0 ORDER BY balance DESC LIMIT 10')
        top = await cur.fetchall()
    if not top:
        return await ctx.send(embed=discord.Embed(title="❌ Info", description="Sin datos", color=COLORS['info']))
    e = discord.Embed(title="🏆 TOP 10 VP$", color=COLORS['primary'])
    for i, (n, b) in enumerate(top, 1):
        e.add_field(name=f"{i}. {n}", value=f"**{b:,} VP$**", inline=False)
    await ctx.send(embed=e)

@bot.command(name="ranking")
async def cmd_ranking(ctx):
    if not await verificar_canal(ctx):
        return
    r = await bot.db.get_rifa_activa()
    if not r:
        return await ctx.send(embed=discord.Embed(title="❌ Error", description="No hay rifa activa", color=COLORS['error']))
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute('SELECT comprador_nick, COUNT(*) as c FROM boletos WHERE rifa_id = ? GROUP BY comprador_id ORDER BY c DESC LIMIT 10', (r['id'],))
        rank = await cur.fetchall()
    if not rank:
        return await ctx.send(embed=discord.Embed(title="❌ Info", description="Sin compras", color=COLORS['info']))
    e = discord.Embed(title="🏆 TOP COMPRADORES", color=COLORS['primary'])
    for i, (n, c) in enumerate(rank, 1):
        e.add_field(name=f"{i}. {n}", value=f"{c} boletos", inline=False)
    await ctx.send(embed=e)

@bot.command(name="historial")
async def cmd_historial(ctx):
    if not await verificar_canal(ctx):
        return
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute('SELECT b.numero, r.nombre FROM boletos b JOIN rifas r ON b.rifa_id = r.id WHERE b.comprador_id = ? ORDER BY b.fecha_compra DESC LIMIT 10', (str(ctx.author.id),))
        h = await cur.fetchall()
    if not h:
        return await ctx.send(embed=discord.Embed(title="❌ Info", description="Sin historial", color=COLORS['info']))
    e = discord.Embed(title="📜 Tu historial", color=COLORS['primary'])
    for n, nom in h:
        e.add_field(name=f"#{n}", value=nom, inline=False)
    await ctx.send(embed=e)

# ============================================
# RIFA ELIMINACIÓN (COMANDOS)
# ============================================

@bot.command(name="celiminacion")
async def cmd_compra_eliminacion(ctx, numero: int):
    if not await verificar_canal(ctx):
        return
    global rifa_eliminacion_activa, rifa_eliminacion_numeros, rifa_eliminacion_valor
    if not rifa_eliminacion_activa:
        return await ctx.send(embed=discord.Embed(title="❌ Error", description="No hay rifa eliminación activa", color=COLORS['error']))
    if numero not in rifa_eliminacion_numeros:
        return await ctx.send(embed=discord.Embed(title="❌ Error", description=f"Número {numero} no disponible", color=COLORS['error']))
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute('SELECT balance FROM usuarios_balance WHERE discord_id = ?', (str(ctx.author.id),))
        bal = (await cur.fetchone() or [0])[0]
        if bal < rifa_eliminacion_valor:
            return await ctx.send(embed=discord.Embed(title="❌ Error", description=f"Necesitas ${rifa_eliminacion_valor:,} VP$", color=COLORS['error']))
        await db.execute('UPDATE usuarios_balance SET balance = balance - ? WHERE discord_id = ?', (rifa_eliminacion_valor, str(ctx.author.id)))
        await db.commit()
    rifa_eliminacion_numeros.remove(numero)
    # Persistir estado
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute('UPDATE rifa_eliminacion SET numeros = ? WHERE id = 1', (json.dumps(rifa_eliminacion_numeros),))
        await db.commit()
    if len(rifa_eliminacion_numeros) == 1:
        ganador = rifa_eliminacion_numeros[0]
        e = discord.Embed(title="🏆 RIFA ELIMINACIÓN FINALIZADA", description=f"¡Número {ganador} sobrevivió!\nGanador: {ctx.author.mention}\nPremio: {rifa_eliminacion_premio}", color=COLORS['success'])
        await ctx.send(embed=e)
        await enviar_dm(str(ctx.author.id), "🏆 GANASTE", f"Ganaste la rifa eliminación con #{ganador}\nPremio: {rifa_eliminacion_premio}")
        rifa_eliminacion_activa = False
        async with aiosqlite.connect(DB_PATH) as db:
            await db.execute('UPDATE rifa_eliminacion SET activa = 0 WHERE id = 1')
            await db.commit()
    else:
        await ctx.message.delete()
        await ctx.send(embed=discord.Embed(title="✅ Compra realizada", description=f"Compraste #{numero} por ${rifa_eliminacion_valor:,} VP$", color=COLORS['success']))
        await enviar_dm(str(ctx.author.id), "🎟️ Compra Eliminación", f"Compraste #{numero} por ${rifa_eliminacion_valor:,} VP$")

@bot.command(name="beliminacion")
async def cmd_ver_eliminacion(ctx):
    if not await verificar_canal(ctx):
        return
    global rifa_eliminacion_activa, rifa_eliminacion_numeros, rifa_eliminacion_total, rifa_eliminacion_valor
    if not rifa_eliminacion_activa:
        return await ctx.send(embed=discord.Embed(title="❌ Error", description="No hay rifa eliminación activa", color=COLORS['error']))
    await ctx.send(embed=discord.Embed(title="🔪 RIFA ELIMINACIÓN", description=f"**Boletos disponibles:** {len(rifa_eliminacion_numeros)}/{rifa_eliminacion_total}\n**Precio:** ${rifa_eliminacion_valor:,} VP$", color=COLORS['info']))

@bot.command(name="rifaeliminacion")
async def cmd_rifa_eliminacion(ctx, total: int, premio: str, valor: int):
    if not await check_admin(ctx):
        return
    global rifa_eliminacion_activa, rifa_eliminacion_total, rifa_eliminacion_premio, rifa_eliminacion_valor, rifa_eliminacion_numeros
    rifa_eliminacion_activa = True
    rifa_eliminacion_total = total
    rifa_eliminacion_premio = premio
    rifa_eliminacion_valor = valor
    rifa_eliminacion_numeros = list(range(1, total+1))
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute('INSERT OR REPLACE INTO rifa_eliminacion (id, activa, total, premio, valor, numeros) VALUES (1, 1, ?, ?, ?, ?)', (total, premio, valor, json.dumps(rifa_eliminacion_numeros)))
        await db.commit()
    await ctx.send(embed=discord.Embed(title="🔪 RIFA ELIMINACIÓN INICIADA", description=f"Premio: {premio}\nValor: ${valor}\nTotal: {total} boletos\n¡El último número que quede GANA!", color=COLORS['primary']))

@bot.command(name="rifaeliminacionr")
async def cmd_rifa_eliminacion_eliminar(ctx, numero: int):
    if not await check_admin(ctx):
        return
    global rifa_eliminacion_activa, rifa_eliminacion_numeros
    if not rifa_eliminacion_activa:
        return await ctx.send(embed=discord.Embed(title="❌ Error", description="No hay rifa eliminación activa", color=COLORS['error']))
    if numero not in rifa_eliminacion_numeros:
        return await ctx.send(embed=discord.Embed(title="❌ Error", description=f"Número {numero} ya eliminado", color=COLORS['error']))
    rifa_eliminacion_numeros.remove(numero)
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute('UPDATE rifa_eliminacion SET numeros = ? WHERE id = 1', (json.dumps(rifa_eliminacion_numeros),))
        await db.commit()
    await ctx.message.delete()
    await ctx.send(embed=discord.Embed(title="✅ Número eliminado", description=f"Número {numero} eliminado. Quedan {len(rifa_eliminacion_numeros)} números", color=COLORS['success']))

# ============================================
# PUNTOS REVANCHA
# ============================================

@bot.command(name="mispuntos")
async def cmd_mis_puntos(ctx):
    if not await verificar_canal(ctx):
        return
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute('SELECT puntos FROM puntos_revancha WHERE usuario_id = ?', (str(ctx.author.id),))
        p = (await cur.fetchone() or [0])[0]
    await ctx.send(embed=discord.Embed(title="🔄 Puntos de Revancha", description=f"Tienes **{p}** puntos", color=COLORS['primary']))

# ============================================
# REFERIDOS (YA FUNCIONAN)
# ============================================

@bot.command(name="codigo")
async def cmd_codigo(ctx):
    if not await verificar_canal(ctx):
        return
    c = await obtener_o_crear_codigo(str(ctx.author.id), ctx.author.name)
    await ctx.send(embed=discord.Embed(title="🔗 Tu código", description=f"`{c}`", color=COLORS['primary']))

@bot.command(name="usar")
async def cmd_usar(ctx, codigo: str):
    if not await verificar_canal(ctx):
        return
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute('SELECT * FROM referidos_relaciones WHERE referido_id = ?', (str(ctx.author.id),))
        if await cur.fetchone():
            return await ctx.send(embed=discord.Embed(title="❌ Error", description="Ya tienes referidor", color=COLORS['error']))
        cur = await db.execute('SELECT usuario_id FROM referidos_codigos WHERE codigo = ?', (codigo.upper(),))
        ref = await cur.fetchone()
        if not ref:
            return await ctx.send(embed=discord.Embed(title="❌ Error", description="Código inválido", color=COLORS['error']))
        await db.execute('INSERT INTO referidos_relaciones (referido_id, referidor_id) VALUES (?, ?)', (str(ctx.author.id), ref[0]))
        await db.commit()
    await verificar_logros(ref[0], None, 'referido', 1)
    await ctx.send(embed=discord.Embed(title="✅ Código aplicado", color=COLORS['success']))

@bot.command(name="misreferidos")
async def cmd_mis_referidos(ctx):
    if not await verificar_canal(ctx):
        return
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute('SELECT COUNT(*) FROM referidos_relaciones WHERE referidor_id = ?', (str(ctx.author.id),))
        c = (await cur.fetchone())[0]
    await ctx.send(embed=discord.Embed(title="👥 Tus referidos", description=f"Total: **{c}**", color=COLORS['primary']))

# ============================================
# FIDELIZACIÓN (YA FUNCIONA)
# ============================================

@bot.command(name="nivel")
async def cmd_nivel(ctx):
    if not await verificar_canal(ctx):
        return
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute('SELECT gasto_total, nivel FROM fidelizacion WHERE usuario_id = ?', (str(ctx.author.id),))
        d = await cur.fetchone()
    if not d:
        return await ctx.send(embed=discord.Embed(title="❌ Info", description="Sin compras", color=COLORS['info']))
    await ctx.send(embed=discord.Embed(title=f"🏆 Nivel: {d[1]}", description=f"Gasto total: **${d[0]:,} VP$**", color=COLORS['primary']))

@bot.command(name="topgastadores")
async def cmd_top_gastadores(ctx):
    if not await verificar_canal(ctx):
        return
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute('SELECT c.nombre, f.gasto_total FROM fidelizacion f LEFT JOIN clientes c ON f.usuario_id = c.discord_id WHERE f.gasto_total > 0 ORDER BY f.gasto_total DESC LIMIT 10')
        top = await cur.fetchall()
    if not top:
        return await ctx.send(embed=discord.Embed(title="❌ Info", description="Sin datos", color=COLORS['info']))
    e = discord.Embed(title="🏆 TOP GASTADORES", color=COLORS['primary'])
    for i, (n, g) in enumerate(top, 1):
        e.add_field(name=f"{i}. {n or 'Usuario'}", value=f"${g:,} VP$", inline=False)
    await ctx.send(embed=e)

@bot.command(name="cashback")
async def cmd_cashback(ctx):
    if not await verificar_canal(ctx):
        return
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute('SELECT cashback_acumulado FROM cashback WHERE usuario_id = ?', (str(ctx.author.id),))
        cb = (await cur.fetchone() or [0])[0]
    await ctx.send(embed=discord.Embed(title="💰 Cashback", description=f"Acumulado: **${cb:,} VP$**", color=COLORS['primary']))

@bot.command(name="topcashback")
async def cmd_top_cashback(ctx):
    if not await verificar_canal(ctx):
        return
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute('SELECT cl.nombre, c.cashback_acumulado FROM cashback c LEFT JOIN clientes cl ON c.usuario_id = cl.discord_id WHERE c.cashback_acumulado > 0 ORDER BY c.cashback_acumulado DESC LIMIT 10')
        top = await cur.fetchall()
    if not top:
        return await ctx.send(embed=discord.Embed(title="❌ Info", description="Sin datos", color=COLORS['info']))
    e = discord.Embed(title="💰 TOP CASHBACK", color=COLORS['primary'])
    for i, (n, cb) in enumerate(top, 1):
        e.add_field(name=f"{i}. {n or 'Usuario'}", value=f"${cb:,} VP$", inline=False)
    await ctx.send(embed=e)

@bot.command(name="verniveles")
async def cmd_verniveles(ctx):
    if not await verificar_canal(ctx):
        return
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute('SELECT * FROM fidelizacion_config ORDER BY gasto_minimo ASC')
        niv = await cur.fetchall()
    e = discord.Embed(title="📊 CONFIGURACIÓN DE NIVELES", color=COLORS['primary'])
    for n in niv:
        e.add_field(name=n[0], value=f"💰 {n[3]}% descuento\n💵 Desde ${n[1]:,}", inline=False)
    await ctx.send(embed=e)

# ============================================
# CAJAS (YA FUNCIONAN)
# ============================================

@bot.command(name="cajas")
async def cmd_cajas(ctx):
    if not await verificar_canal(ctx):
        return
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute('SELECT * FROM cajas WHERE activo = 1')
        cs = await cur.fetchall()
    e = discord.Embed(title="🎁 CAJAS MISTERIOSAS", color=COLORS['primary'])
    for c in cs:
        e.add_field(name=f"{c[2]} - ${c[3]:,} VP$", value="Premios variables", inline=False)
    await ctx.send(embed=e)

@bot.command(name="comprarcaja")
async def cmd_comprar_caja(ctx, tipo: str, cantidad: int = 1):
    if not await verificar_canal(ctx):
        return
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute('SELECT * FROM cajas WHERE tipo = ? AND activo = 1', (tipo.lower(),))
        c = await cur.fetchone()
        if not c:
            return await ctx.send(embed=discord.Embed(title="❌ Error", description="Tipo inválido", color=COLORS['error']))
        total = c[3] * cantidad
        cur2 = await db.execute('SELECT balance FROM usuarios_balance WHERE discord_id = ?', (str(ctx.author.id),))
        bal = (await cur2.fetchone() or [0])[0]
        if bal < total:
            return await ctx.send(embed=discord.Embed(title="❌ Error", description=f"Necesitas ${total:,} VP$", color=COLORS['error']))
        await db.execute('UPDATE usuarios_balance SET balance = balance - ? WHERE discord_id = ?', (total, str(ctx.author.id)))
        for _ in range(cantidad):
            await db.execute('INSERT INTO cajas_compradas (usuario_id, caja_id) VALUES (?, ?)', (str(ctx.author.id), c[0]))
        await db.commit()
    await ctx.send(embed=discord.Embed(title="✅ Compra realizada", description=f"Compraste {cantidad}x {c[2]}", color=COLORS['success']))

@bot.command(name="miscajas")
async def cmd_mis_cajas(ctx):
    if not await verificar_canal(ctx):
        return
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute('SELECT cc.id, c.nombre FROM cajas_compradas cc JOIN cajas c ON cc.caja_id = c.id WHERE cc.usuario_id = ? AND cc.abierta = 0', (str(ctx.author.id),))
        cs = await cur.fetchall()
    if not cs:
        return await ctx.send(embed=discord.Embed(title="❌ Info", description="No tienes cajas sin abrir", color=COLORS['info']))
    e = discord.Embed(title="📦 Tus cajas sin abrir", color=COLORS['primary'])
    for cid, nom in cs:
        e.add_field(name=f"ID: {cid}", value=nom, inline=False)
    await ctx.send(embed=e)

@bot.command(name="abrircaja")
async def cmd_abrir_caja(ctx, caja_id: int):
    if not await verificar_canal(ctx):
        return
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute('SELECT cc.*, c.premios, c.probabilidades, c.nombre FROM cajas_compradas cc JOIN cajas c ON cc.caja_id = c.id WHERE cc.id = ? AND cc.usuario_id = ? AND cc.abierta = 0', (caja_id, str(ctx.author.id)))
        c = await cur.fetchone()
        if not c:
            return await ctx.send(embed=discord.Embed(title="❌ Error", description="Caja no encontrada o ya abierta", color=COLORS['error']))
        premios = json.loads(c[7])
        probs = json.loads(c[8])
        elegido = random.choices(premios, weights=probs, k=1)[0]
        await db.execute('UPDATE cajas_compradas SET abierta = 1, premio = ?, fecha_apertura = CURRENT_TIMESTAMP WHERE id = ?', (elegido, caja_id))
        if elegido > 0:
            await db.execute('UPDATE usuarios_balance SET balance = balance + ? WHERE discord_id = ?', (elegido, str(ctx.author.id)))
        await db.execute('INSERT INTO cajas_historial (usuario_id, usuario_nick, caja_nombre, premio_obtenido) VALUES (?, ?, ?, ?)', (str(ctx.author.id), ctx.author.name, c[9], elegido))
        await db.commit()
    await verificar_logros(str(ctx.author.id), ctx.author.name, 'caja', 1)
    if elegido > 0:
        await ctx.send(embed=discord.Embed(title="🎉 CAJA ABIERTA", description=f"Has obtenido **${elegido:,} VP$**", color=COLORS['success']))
    else:
        await ctx.send(embed=discord.Embed(title="😢 CAJA ABIERTA", description="No has ganado nada", color=COLORS['error']))

@bot.command(name="topcajas")
async def cmd_top_cajas(ctx):
    if not await verificar_canal(ctx):
        return
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute('SELECT usuario_nick, SUM(premio_obtenido) as total FROM cajas_historial GROUP BY usuario_id ORDER BY total DESC LIMIT 10')
        top = await cur.fetchall()
    if not top:
        return await ctx.send(embed=discord.Embed(title="❌ Info", description="Sin datos", color=COLORS['info']))
    e = discord.Embed(title="🏆 TOP GANADORES DE CAJAS", color=COLORS['primary'])
    for i, (n, t) in enumerate(top, 1):
        e.add_field(name=f"{i}. {n}", value=f"**${t:,} VP$**", inline=False)
    await ctx.send(embed=e)

# ============================================
# BANCO (YA FUNCIONA)
# ============================================

@bot.command(name="banco")
async def cmd_banco(ctx):
    if not await verificar_canal(ctx):
        return
    e = discord.Embed(title="🏦 BANCO VP", color=COLORS['primary'])
    e.add_field(name="📈 Básico", value="7 días - 5% interés\nMonto: 10k - 500k VP$", inline=False)
    e.add_field(name="📈 Plus", value="14 días - 12% interés\nMonto: 50k - 2M VP$", inline=False)
    e.add_field(name="📈 VIP", value="30 días - 25% interés\nMonto: 200k - 10M VP$", inline=False)
    e.add_field(name="📈 Elite", value="60 días - 40% interés\nMonto: 1M - 50M VP$", inline=False)
    await ctx.send(embed=e)

@bot.command(name="invertir")
async def cmd_invertir(ctx, producto: str, monto: int):
    if not await verificar_canal(ctx):
        return
    prods = {'basico': (7, 5, 10000, 500000), 'plus': (14, 12, 50000, 2000000), 'vip': (30, 25, 200000, 10000000), 'elite': (60, 40, 1000000, 50000000)}
    if producto.lower() not in prods:
        return await ctx.send(embed=discord.Embed(title="❌ Error", description="Producto: basico, plus, vip, elite", color=COLORS['error']))
    dias, interes, minimo, maximo = prods[producto.lower()]
    if monto < minimo or monto > maximo:
        return await ctx.send(embed=discord.Embed(title="❌ Error", description=f"Monto entre {minimo:,} y {maximo:,} VP$", color=COLORS['error']))
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute('SELECT balance FROM usuarios_balance WHERE discord_id = ?', (str(ctx.author.id),))
        bal = (await cur.fetchone() or [0])[0]
        if bal < monto:
            return await ctx.send(embed=discord.Embed(title="❌ Error", description=f"Necesitas ${monto:,} VP$", color=COLORS['error']))
        fecha_fin = datetime.now() + timedelta(days=dias)
        await db.execute('UPDATE usuarios_balance SET balance = balance - ? WHERE discord_id = ?', (monto, str(ctx.author.id)))
        await db.execute('INSERT INTO inversiones (usuario_id, producto, monto, interes, fecha_fin) VALUES (?, ?, ?, ?, ?)', (str(ctx.author.id), producto, monto, interes, fecha_fin))
        await db.commit()
    await ctx.send(embed=discord.Embed(title="✅ Inversión realizada", description=f"Invertiste ${monto:,} VP$ al {interes}% por {dias} días", color=COLORS['success']))

@bot.command(name="misinversiones")
async def cmd_mis_inversiones(ctx):
    if not await verificar_canal(ctx):
        return
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute('SELECT id, producto, monto, interes, fecha_fin FROM inversiones WHERE usuario_id = ? AND estado = "activa"', (str(ctx.author.id),))
        inv = await cur.fetchall()
    if not inv:
        return await ctx.send(embed=discord.Embed(title="❌ Info", description="Sin inversiones activas", color=COLORS['info']))
    e = discord.Embed(title="📊 Tus inversiones", color=COLORS['primary'])
    for i in inv:
        dias = (datetime.fromisoformat(i[4]) - datetime.now()).days
        e.add_field(name=f"#{i[0]} - {i[1]}", value=f"${i[2]:,} - {i[3]}% - {dias} días", inline=False)
    await ctx.send(embed=e)

@bot.command(name="retirar")
async def cmd_retirar(ctx, inv_id: int):
    if not await verificar_canal(ctx):
        return
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute('SELECT * FROM inversiones WHERE id = ? AND usuario_id = ? AND estado = "activa"', (inv_id, str(ctx.author.id)))
        inv = await cur.fetchone()
        if not inv:
            return await ctx.send(embed=discord.Embed(title="❌ Error", description="Inversión no encontrada", color=COLORS['error']))
        fecha_fin = datetime.fromisoformat(inv[6])
        if datetime.now() < fecha_fin:
            dias = (fecha_fin - datetime.now()).days
            return await ctx.send(embed=discord.Embed(title="❌ Error", description=f"Faltan {dias} días", color=COLORS['error']))
        ganancia = int(inv[3] * inv[4] / 100)
        comision = int(ganancia * 5 / 100)
        total = inv[3] + ganancia - comision
        await db.execute('UPDATE usuarios_balance SET balance = balance + ? WHERE discord_id = ?', (total, str(ctx.author.id)))
        await db.execute('UPDATE inversiones SET estado = "completada" WHERE id = ?', (inv_id,))
        await db.commit()
    await ctx.send(embed=discord.Embed(title="✅ Inversión retirada", description=f"Recibiste ${total:,} VP$", color=COLORS['success']))

@bot.command(name="cambiarng")
async def cmd_cambiar_ng(ctx, cantidad: int):
    if not await verificar_canal(ctx):
        return
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute('SELECT value FROM config_global WHERE key = "cambio_ng_minimo"')
        minc = int((await cur.fetchone() or [100000])[0])
        cur = await db.execute('SELECT value FROM config_global WHERE key = "cambio_ng_maximo"')
        maxc = int((await cur.fetchone() or [10000000])[0])
        cur = await db.execute('SELECT value FROM config_global WHERE key = "tasa_compra"')
        tasa = float((await cur.fetchone() or [0.9])[0])
    if cantidad < minc or cantidad > maxc:
        return await ctx.send(embed=discord.Embed(title="❌ Error", description=f"Monto entre {minc:,} y {maxc:,} NG$", color=COLORS['error']))
    vp = int(cantidad * tasa)
    guild = ctx.guild
    cat = guild.get_channel(CATEGORIA_TICKETS)
    if not cat:
        return await ctx.send(embed=discord.Embed(title="❌ Error", description="Error con tickets", color=COLORS['error']))
    overwrites = {guild.default_role: discord.PermissionOverwrite(read_messages=False), ctx.author: discord.PermissionOverwrite(read_messages=True, send_messages=True)}
    for rid in [ROLES['CEO'], ROLES['DIRECTOR']]:
        r = guild.get_role(rid)
        if r:
            overwrites[r] = discord.PermissionOverwrite(read_messages=True, send_messages=True)
    ticket = await cat.create_text_channel(f"ticket-{ctx.author.name}", overwrites=overwrites)
    e = discord.Embed(title="🎫 SOLICITUD DE CAMBIO NG$ → VP$", description=f"Usuario: {ctx.author.mention}\nCantidad NG$: {cantidad:,}\nVP$ a recibir: {vp:,}\nEstado: ⏰ PENDIENTE", color=COLORS['info'])
    await ticket.send(embed=e)
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute('INSERT INTO tickets_cambio (usuario_id, usuario_nick, canal_id, cantidad_ng, tasa_compra, cantidad_vp) VALUES (?, ?, ?, ?, ?, ?)', (str(ctx.author.id), ctx.author.name, str(ticket.id), cantidad, tasa, vp))
        await db.commit()
    await ctx.send(embed=discord.Embed(title="✅ Ticket creado", description=f"Revisa {ticket.mention}", color=COLORS['success']))

# ============================================
# VENDEDORES
# ============================================

@bot.command(name="vender")
async def cmd_vender(ctx, usuario: discord.Member, numero: int):
    if not await verificar_canal(ctx) or not await check_vendedor(ctx):
        return
    r = await bot.db.get_rifa_activa()
    if not r:
        return await ctx.send(embed=discord.Embed(title="❌ Error", description="No hay rifa activa", color=COLORS['error']))
    disp = await bot.db.get_boletos_disponibles(r['id'])
    if numero not in disp:
        return await ctx.send(embed=discord.Embed(title="❌ Error", description=f"Número {numero} no disponible", color=COLORS['error']))
    desc = await obtener_descuento_usuario(str(usuario.id))
    final = int(r['precio_boleto'] * (100 - desc) / 100)
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute('SELECT balance FROM usuarios_balance WHERE discord_id = ?', (str(usuario.id),))
        bal = (await cur.fetchone() or [0])[0]
        if bal < final:
            return await ctx.send(embed=discord.Embed(title="❌ Error", description=f"Usuario necesita ${final:,} VP$", color=COLORS['error']))
        await db.execute('UPDATE usuarios_balance SET balance = balance - ? WHERE discord_id = ?', (final, str(usuario.id)))
        await db.execute('INSERT INTO boletos (rifa_id, numero, comprador_id, comprador_nick, vendedor_id, precio_pagado) VALUES (?, ?, ?, ?, ?, ?)', (r['id'], numero, str(usuario.id), usuario.name, str(ctx.author.id), r['precio_boleto']))
        await db.commit()
    await actualizar_fidelizacion(str(usuario.id), final)
    await aplicar_cashback(str(usuario.id), final)
    await procesar_comision_referido(str(usuario.id), final)
    await procesar_comision_vendedor(str(ctx.author.id), final)
    await verificar_logros(str(usuario.id), usuario.name, 'compra', 1)
    await enviar_dm(str(usuario.id), "🎟️ Boleto comprado", f"Compraste #{numero} por ${final:,} VP$")
    await enviar_dm(str(ctx.author.id), "💰 Venta realizada", f"Vendiste #{numero} a {usuario.name} por ${final:,} VP$")
    await ctx.message.delete()
    await ctx.send(embed=discord.Embed(title="✅ Venta realizada", description="Revisa tu DM", color=COLORS['success']))

@bot.command(name="venderrandom")
async def cmd_vender_random(ctx, usuario: discord.Member, cantidad: int = 1):
    if not await verificar_canal(ctx) or not await check_vendedor(ctx):
        return
    if not 1 <= cantidad <= 50:
        return await ctx.send(embed=discord.Embed(title="❌ Error", description="Cantidad 1-50", color=COLORS['error']))
    r = await bot.db.get_rifa_activa()
    if not r:
        return await ctx.send(embed=discord.Embed(title="❌ Error", description="No hay rifa activa", color=COLORS['error']))
    disp = await bot.db.get_boletos_disponibles(r['id'])
    disp_filt = [n for n in disp if not await es_numero_vip(r['id'], n)]
    if len(disp_filt) < cantidad:
        return await ctx.send(embed=discord.Embed(title="❌ Error", description="No hay suficientes boletos disponibles", color=COLORS['error']))
    desc = await obtener_descuento_usuario(str(usuario.id))
    global evento_2x1
    pagar = cantidad // 2 + (cantidad % 2) if evento_2x1 else cantidad
    total = r['precio_boleto'] * pagar
    final = int(total * (100 - desc) / 100)
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute('SELECT balance FROM usuarios_balance WHERE discord_id = ?', (str(usuario.id),))
        bal = (await cur.fetchone() or [0])[0]
        if bal < final:
            return await ctx.send(embed=discord.Embed(title="❌ Error", description=f"Usuario necesita ${final:,} VP$", color=COLORS['error']))
        sel = random.sample(disp_filt, cantidad)
        await db.execute('UPDATE usuarios_balance SET balance = balance - ? WHERE discord_id = ?', (final, str(usuario.id)))
        for n in sel:
            await db.execute('INSERT INTO boletos (rifa_id, numero, comprador_id, comprador_nick, vendedor_id, precio_pagado) VALUES (?, ?, ?, ?, ?, ?)', (r['id'], n, str(usuario.id), usuario.name, str(ctx.author.id), r['precio_boleto']))
        await db.commit()
    await actualizar_fidelizacion(str(usuario.id), final)
    await aplicar_cashback(str(usuario.id), final)
    await procesar_comision_referido(str(usuario.id), final)
    await procesar_comision_vendedor(str(ctx.author.id), final)
    await verificar_logros(str(usuario.id), usuario.name, 'compra', cantidad)
    await enviar_dm(str(usuario.id), "🎟️ Compra realizada", f"Compraste {cantidad} boletos por ${final:,} VP$")
    await enviar_dm(str(ctx.author.id), "💰 Venta realizada", f"Vendiste {cantidad} boletos a {usuario.name} por ${final:,} VP$")
    await ctx.message.delete()
    await ctx.send(embed=discord.Embed(title="✅ Venta realizada", description="Revisa tu DM", color=COLORS['success']))

@bot.command(name="misventas")
async def cmd_mis_ventas(ctx):
    if not await verificar_canal(ctx) or not await check_vendedor(ctx):
        return
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute('SELECT comisiones_pendientes FROM vendedores WHERE discord_id = ?', (str(ctx.author.id),))
        pend = (await cur.fetchone() or [0])[0]
    await ctx.send(embed=discord.Embed(title="💰 Tus ventas", description=f"Comisiones pendientes: **${pend:,} VP$**", color=COLORS['primary']))

@bot.command(name="listaboletos")
async def cmd_lista_boletos(ctx):
    if not await verificar_canal(ctx) or not await check_vendedor(ctx):
        return
    r = await bot.db.get_rifa_activa()
    if not r:
        return await ctx.send(embed=discord.Embed(title="❌ Error", description="No hay rifa activa", color=COLORS['error']))
    disp = await bot.db.get_boletos_disponibles(r['id'])
    vend = await bot.db.get_boletos_vendidos(r['id'])
    await ctx.send(embed=discord.Embed(title="📋 Boletos", description=f"**Total:** {r['total_boletos']}\n**Vendidos:** {vend}\n**Disponibles:** {len(disp)}", color=COLORS['info']))

# ============================================
# DIRECTORES
# ============================================

@bot.command(name="crearifa")
async def cmd_crear_rifa(ctx, premio: str, precio: int, total: int, bloqueados: str = None):
    if not await check_admin(ctx):
        return
    nombre = f"Rifa {datetime.now().strftime('%d/%m')}"
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute('INSERT INTO rifas (nombre, premio, valor_premio, precio_boleto, total_boletos, numeros_bloqueados, estado) VALUES (?, ?, ?, ?, ?, ?, "activa")', (nombre, premio, precio, precio, total, bloqueados))
        await db.commit()
    await ctx.message.delete()
    await ctx.send(embed=discord.Embed(title="✅ Rifa creada", description=f"Premio: {premio}\nPrecio: ${precio}\nTotal: {total} boletos", color=COLORS['success']))

@bot.command(name="cerrarifa")
async def cmd_cerrar_rifa(ctx):
    if not await check_admin(ctx):
        return
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute('UPDATE rifas SET estado = "cerrada" WHERE estado = "activa"')
        await db.commit()
    await ctx.message.delete()
    await ctx.send(embed=discord.Embed(title="✅ Rifa cerrada", color=COLORS['success']))

@bot.command(name="iniciarsorteo")
async def cmd_iniciar_sorteo(ctx, ganadores: int = 1):
    if not await check_admin(ctx):
        return
    r = await bot.db.get_rifa_activa()
    if not r:
        return await ctx.send(embed=discord.Embed(title="❌ Error", description="No hay rifa activa", color=COLORS['error']))
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute('SELECT numero, comprador_id, comprador_nick FROM boletos WHERE rifa_id = ?', (r['id'],))
        b = await cur.fetchall()
    if len(b) < ganadores:
        return await ctx.send(embed=discord.Embed(title="❌ Error", description=f"Solo {len(b)} boletos vendidos", color=COLORS['error']))
    await ctx.send("🎲 Sorteo en 5 segundos...")
    await asyncio.sleep(5)
    g = random.sample(b, min(ganadores, len(b)))
    e = discord.Embed(title="🎉 GANADORES", color=COLORS['success'])
    for num, uid, nick in g:
        e.add_field(name=f"#{num}", value=nick, inline=False)
        await enviar_dm(uid, "🎉 GANASTE", f"Ganaste la rifa {r['nombre']} con #{num}")
        await verificar_logros(uid, nick, 'subasta', 1)
    await ctx.send(embed=e)

@bot.command(name="finalizarrifa")
async def cmd_finalizar_rifa(ctx, id_rifa: int = None, ganadores: int = 1):
    if not await check_admin(ctx):
        return
    if not id_rifa:
        r = await bot.db.get_rifa_activa()
        if not r:
            return await ctx.send(embed=discord.Embed(title="❌ Error", description="Especifica ID de rifa", color=COLORS['error']))
        id_rifa = r['id']
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute('SELECT * FROM rifas WHERE id = ?', (id_rifa,))
        rifa = await cur.fetchone()
        if not rifa:
            return await ctx.send(embed=discord.Embed(title="❌ Error", description="Rifa no encontrada", color=COLORS['error']))
        cur = await db.execute('SELECT numero, comprador_id, comprador_nick FROM boletos WHERE rifa_id = ?', (id_rifa,))
        b = await cur.fetchall()
    if not b:
        return await ctx.send(embed=discord.Embed(title="❌ Error", description="Sin boletos vendidos", color=COLORS['error']))
    g = random.sample(b, min(ganadores, len(b)))
    e = discord.Embed(title=f"🎉 Rifa #{id_rifa} finalizada", description=f"Premio: {rifa[2]}", color=COLORS['success'])
    for num, uid, nick in g:
        e.add_field(name=f"#{num}", value=nick, inline=False)
        await enviar_dm(uid, "🎉 GANASTE", f"Ganaste la rifa {rifa[1]} con #{num}")
        await verificar_logros(uid, nick, 'subasta', 1)
    await ctx.send(embed=e)

@bot.command(name="vendedoradd")
async def cmd_vendedor_add(ctx, usuario: discord.Member, comision: int = 10):
    if not await check_admin(ctx):
        return
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute('INSERT INTO vendedores (discord_id, nombre, comision) VALUES (?, ?, ?) ON CONFLICT(discord_id) DO UPDATE SET comision = ?', (str(usuario.id), usuario.name, comision, comision))
        await db.commit()
    await ctx.message.delete()
    await ctx.send(embed=discord.Embed(title="✅ Vendedor añadido", description=f"{usuario.name} - {comision}%", color=COLORS['success']))

@bot.command(name="vercomisiones")
async def cmd_ver_comisiones(ctx):
    if not await check_admin(ctx):
        return
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute('SELECT nombre, comisiones_pendientes FROM vendedores WHERE comisiones_pendientes > 0')
        v = await cur.fetchall()
    if not v:
        return await ctx.send(embed=discord.Embed(title="❌ Info", description="No hay comisiones pendientes", color=COLORS['info']))
    e = discord.Embed(title="💰 Comisiones pendientes", color=COLORS['primary'])
    for n, c in v:
        e.add_field(name=n, value=f"${c:,} VP$", inline=True)
    await ctx.send(embed=e)

@bot.command(name="pagarcomisiones")
async def cmd_pagar_comisiones(ctx):
    if not await check_admin(ctx):
        return
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute('SELECT discord_id, nombre, comisiones_pendientes FROM vendedores WHERE comisiones_pendientes > 0')
        v = await cur.fetchall()
        if not v:
            return await ctx.send(embed=discord.Embed(title="❌ Info", description="No hay comisiones pendientes", color=COLORS['info']))
        total = 0
        for vid, nom, m in v:
            await db.execute('UPDATE usuarios_balance SET balance = balance + ? WHERE discord_id = ?', (m, vid))
            await db.execute('UPDATE vendedores SET comisiones_pendientes = 0, comisiones_pagadas = comisiones_pagadas + ? WHERE discord_id = ?', (m, vid))
            total += m
            await enviar_dm(vid, "💰 Comisiones pagadas", f"Has recibido ${m:,} VP$ por tus ventas")
        await db.commit()
    await ctx.message.delete()
    await ctx.send(embed=discord.Embed(title="✅ Comisiones pagadas", description=f"Total pagado: ${total:,} VP$", color=COLORS['success']))

@bot.command(name="verboletos")
async def cmd_ver_boletos(ctx, usuario: discord.Member):
    if not await check_admin(ctx):
        return
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute('SELECT numero, rifa_id FROM boletos WHERE comprador_id = ?', (str(usuario.id),))
        b = await cur.fetchall()
    if not b:
        return await ctx.send(embed=discord.Embed(title="❌ Info", description=f"{usuario.name} no tiene boletos", color=COLORS['info']))
    await ctx.send(embed=discord.Embed(title=f"🎟️ Boletos de {usuario.name}", description=f"Total: {len(b)} boletos", color=COLORS['primary']))

@bot.command(name="subasta crear")
async def cmd_subasta_crear(ctx, item: str, precio_base: int, horas: int):
    if not await check_admin(ctx):
        return
    fecha_fin = datetime.now() + timedelta(hours=horas)
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute('INSERT INTO subastas (item_nombre, precio_base, precio_actual, canal_id, fecha_fin, creada_por) VALUES (?, ?, ?, ?, ?, ?)', (item, precio_base, precio_base, str(ctx.channel.id), fecha_fin, str(ctx.author.id)))
        sid = cur.lastrowid
        await db.commit()
    e = discord.Embed(title=f"🎫 SUBASTA: {item}", description=f"Precio base: ${precio_base:,} VP$\nDuración: {horas} horas", color=COLORS['primary'])
    e.add_field(name="💰 Puja actual", value=f"${precio_base:,} VP$", inline=False)
    e.add_field(name="📝 Cómo pujar", value=f"Usa `!pujar {sid} [monto]`", inline=False)
    e.set_footer(text=f"ID: {sid} | Finaliza: {fecha_fin.strftime('%H:%M')}")
    msg = await ctx.send(embed=e)
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute('UPDATE subastas SET mensaje_id = ? WHERE id = ?', (str(msg.id), sid))
        await db.commit()

@bot.command(name="pujar")
async def cmd_pujar(ctx, subasta_id: int, monto: int):
    if not await verificar_canal(ctx):
        return
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute('SELECT * FROM subastas WHERE id = ? AND estado = "activa"', (subasta_id,))
        s = await cur.fetchone()
        if not s:
            return await ctx.send(embed=discord.Embed(title="❌ Error", description="Subasta no encontrada o finalizada", color=COLORS['error']))
        if monto <= s[4]:
            return await ctx.send(embed=discord.Embed(title="❌ Error", description=f"La puja debe ser mayor a ${s[4]:,} VP$", color=COLORS['error']))
        cur = await db.execute('SELECT balance FROM usuarios_balance WHERE discord_id = ?', (str(ctx.author.id),))
        bal = (await cur.fetchone() or [0])[0]
        if bal < monto:
            return await ctx.send(embed=discord.Embed(title="❌ Error", description=f"Necesitas ${monto:,} VP$", color=COLORS['error']))
        await db.execute('UPDATE subastas SET precio_actual = ? WHERE id = ?', (monto, subasta_id))
        await db.execute('INSERT INTO pujas (subasta_id, usuario_id, usuario_nick, monto) VALUES (?, ?, ?, ?)', (subasta_id, str(ctx.author.id), ctx.author.name, monto))
        await db.commit()
    # Actualizar embed del mensaje
    try:
        canal = ctx.channel
        msg = await canal.fetch_message(int(s[7]))
        if msg.embeds:
            emb = msg.embeds[0]
            for i, f in enumerate(emb.fields):
                if f.name == "💰 Puja actual":
                    emb.set_field_at(i, name="💰 Puja actual", value=f"${monto:,} VP$ por {ctx.author.name}", inline=False)
                    await msg.edit(embed=emb)
                    break
    except:
        pass
    await ctx.message.delete()
    await ctx.send(embed=discord.Embed(title="✅ Puja registrada", description=f"Has pujado ${monto:,} VP$ en la subasta #{subasta_id}", color=COLORS['success']))

@bot.command(name="subastas")
async def cmd_subastas(ctx):
    if not await verificar_canal(ctx):
        return
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute('SELECT * FROM subastas WHERE estado = "activa" ORDER BY fecha_fin ASC')
        subs = await cur.fetchall()
    if not subs:
        return await ctx.send(embed=discord.Embed(title="❌ Info", description="No hay subastas activas", color=COLORS['info']))
    e = discord.Embed(title="🎫 SUBASTAS ACTIVAS", color=COLORS['primary'])
    for s in subs:
        resto = datetime.fromisoformat(s[9]) - datetime.now()
        horas = resto.total_seconds() // 3600
        mins = (resto.total_seconds() % 3600) // 60
        e.add_field(name=f"#{s[0]} - {s[1]}", value=f"💰 ${s[4]:,} VP$\n⏰ {int(horas)}h {int(mins)}m\n`!pujar {s[0]} [monto]`", inline=False)
    await ctx.send(embed=e)

@bot.command(name="mis_pujas")
async def cmd_mis_pujas(ctx):
    if not await verificar_canal(ctx):
        return
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute('SELECT p.subasta_id, s.item_nombre, p.monto, s.estado FROM pujas p JOIN subastas s ON p.subasta_id = s.id WHERE p.usuario_id = ? ORDER BY p.fecha DESC LIMIT 10', (str(ctx.author.id),))
        pujas = await cur.fetchall()
    if not pujas:
        return await ctx.send(embed=discord.Embed(title="❌ Info", description="No has participado en subastas", color=COLORS['info']))
    e = discord.Embed(title="📊 Tus pujas", color=COLORS['primary'])
    for sid, item, monto, estado in pujas:
        e.add_field(name=f"#{sid} - {item}", value=f"${monto:,} VP$ - {estado}", inline=False)
    await ctx.send(embed=e)

# ============================================
# CEO
# ============================================

@bot.command(name="acreditarvp")
async def cmd_acreditarvp(ctx, usuario: discord.Member, cantidad: int):
    if not await check_ceo(ctx):
        return
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute('INSERT INTO usuarios_balance (discord_id, nombre, balance) VALUES (?, ?, ?) ON CONFLICT(discord_id) DO UPDATE SET balance = balance + ?', (str(usuario.id), usuario.name, cantidad, cantidad))
        await db.commit()
    await enviar_dm(str(usuario.id), "💰 Acreditación de VP$", f"Se te han acreditado ${cantidad:,} VP$")
    await ctx.message.delete()
    await ctx.send(embed=discord.Embed(title="✅ Acreditado", description=f"${cantidad:,} VP$ a {usuario.name}", color=COLORS['success']))

@bot.command(name="retirarvp")
async def cmd_retirarvp(ctx, usuario: discord.Member, cantidad: int):
    if not await check_ceo(ctx):
        return
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute('SELECT balance FROM usuarios_balance WHERE discord_id = ?', (str(usuario.id),))
        bal = (await cur.fetchone() or [0])[0]
        if bal < cantidad:
            return await ctx.send(embed=discord.Embed(title="❌ Error", description="Saldo insuficiente", color=COLORS['error']))
        await db.execute('UPDATE usuarios_balance SET balance = balance - ? WHERE discord_id = ?', (cantidad, str(usuario.id)))
        await db.commit()
    await enviar_dm(str(usuario.id), "💰 Retiro de VP$", f"Se te han retirado ${cantidad:,} VP$")
    await ctx.message.delete()
    await ctx.send(embed=discord.Embed(title="✅ Retirado", description=f"${cantidad:,} VP$ de {usuario.name}", color=COLORS['success']))

@bot.command(name="procesarvp")
async def cmd_procesar_vp(ctx, usuario: discord.Member):
    if not await check_ceo(ctx):
        return
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute('SELECT id, canal_id, cantidad_ng FROM tickets_cambio WHERE usuario_id = ? AND estado = "pendiente" ORDER BY fecha_creacion DESC LIMIT 1', (str(usuario.id),))
        t = await cur.fetchone()
        if not t:
            return await ctx.send(embed=discord.Embed(title="❌ Error", description=f"{usuario.name} no tiene tickets pendientes", color=COLORS['error']))
        await db.execute('UPDATE tickets_cambio SET estado = "procesando", fecha_procesado = CURRENT_TIMESTAMP, procesado_por = ? WHERE id = ?', (str(ctx.author.id), t[0]))
        await db.commit()
        canal = bot.get_channel(int(t[1]))
        if canal:
            await canal.send(embed=discord.Embed(title="🔄 ESTADO ACTUALIZADO", description=f"⏳ Su pago de {t[2]:,} NG$ está siendo **PROCESADO**\n✅ Pronto recibirá sus VP$", color=COLORS['info']))
    await ctx.message.delete()
    await ctx.send(embed=discord.Embed(title="✅ Ticket en proceso", description=f"Ticket de {usuario.name} marcado como procesando", color=COLORS['success']))

@bot.command(name="procesadovp")
async def cmd_procesado_vp(ctx, usuario: discord.Member, cantidad_vp: int):
    if not await check_ceo(ctx):
        return
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute('SELECT id, canal_id FROM tickets_cambio WHERE usuario_id = ? AND estado = "procesando" ORDER BY fecha_creacion DESC LIMIT 1', (str(usuario.id),))
        t = await cur.fetchone()
        if not t:
            return await ctx.send(embed=discord.Embed(title="❌ Error", description=f"{usuario.name} no tiene tickets en proceso", color=COLORS['error']))
        await db.execute('UPDATE usuarios_balance SET balance = balance + ? WHERE discord_id = ?', (cantidad_vp, str(usuario.id)))
        await db.execute('UPDATE tickets_cambio SET estado = "completado", cantidad_vp = ? WHERE id = ?', (cantidad_vp, t[0]))
        await db.commit()
        canal = bot.get_channel(int(t[1]))
        if canal:
            await canal.send(embed=discord.Embed(title="✅ PAGO CONFIRMADO", description=f"💰 Se le han acreditado **{cantidad_vp:,} VP$**\n📊 Nuevo balance: {cantidad_vp:,} VP$\n🎫 Este ticket se cerrará en 10 segundos...", color=COLORS['success']))
            await asyncio.sleep(10)
            await canal.delete()
    await ctx.message.delete()
    await ctx.send(embed=discord.Embed(title="✅ Pago completado", description=f"Acreditados ${cantidad_vp:,} VP$ a {usuario.name}", color=COLORS['success']))

@bot.command(name="ticketcerrar")
async def cmd_ticket_cerrar(ctx):
    if not await check_admin(ctx):
        return
    if not ctx.channel.name.startswith("ticket"):
        return await ctx.send(embed=discord.Embed(title="❌ Error", description="Este comando solo funciona en tickets", color=COLORS['error']))
    await ctx.send(embed=discord.Embed(title="🔒 Cerrando ticket", description="Este ticket se cerrará en 5 segundos...", color=COLORS['info']))
    await asyncio.sleep(5)
    await ctx.channel.delete()

@bot.command(name="setrefcomision")
async def cmd_set_ref_comision(ctx, porcentaje: int):
    if not await check_ceo(ctx):
        return
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute('UPDATE referidos_config SET porcentaje_comision = ? WHERE id = 1', (porcentaje,))
        await db.commit()
    await ctx.message.delete()
    await ctx.send(embed=discord.Embed(title="✅ Configurado", description=f"Comisión referidos: {porcentaje}%", color=COLORS['success']))

@bot.command(name="setcashback")
async def cmd_set_cashback(ctx, porcentaje: int):
    if not await check_ceo(ctx):
        return
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute('UPDATE cashback_config SET porcentaje = ? WHERE id = 1', (porcentaje,))
        await db.commit()
    await ctx.message.delete()
    await ctx.send(embed=discord.Embed(title="✅ Configurado", description=f"Cashback: {porcentaje}%", color=COLORS['success']))

@bot.command(name="pagarcashback")
async def cmd_pagar_cashback(ctx):
    if not await check_ceo(ctx):
        return
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute('SELECT usuario_id, cashback_acumulado FROM cashback WHERE cashback_acumulado > 0')
        users = await cur.fetchall()
        if not users:
            return await ctx.send(embed=discord.Embed(title="❌ Info", description="No hay cashback pendiente", color=COLORS['info']))
        total = 0
        for uid, m in users:
            await db.execute('UPDATE usuarios_balance SET balance = balance + ? WHERE discord_id = ?', (m, uid))
            await db.execute('UPDATE cashback SET cashback_acumulado = 0, cashback_recibido = cashback_recibido + ? WHERE usuario_id = ?', (m, uid))
            total += m
            await enviar_dm(uid, "💰 Cashback pagado", f"Has recibido ${m:,} VP$ por cashback")
        await db.commit()
    await ctx.message.delete()
    await ctx.send(embed=discord.Embed(title="✅ Cashback pagado", description=f"Total pagado: ${total:,} VP$", color=COLORS['success']))

@bot.command(name="evento")
async def cmd_evento(ctx, tipo: str, accion: str, valor: int = None):
    if not await check_ceo(ctx):
        return
    global evento_2x1, evento_cashback_doble, evento_oferta_activa, evento_oferta_porcentaje
    if tipo == "2x1":
        evento_2x1 = accion.lower() == "on"
        estado = "ACTIVADO" if evento_2x1 else "DESACTIVADO"
    elif tipo == "cashbackdoble":
        evento_cashback_doble = accion.lower() == "on"
        estado = "ACTIVADO" if evento_cashback_doble else "DESACTIVADO"
    elif tipo == "oferta" and valor is not None:
        evento_oferta_activa = accion.lower() == "on"
        evento_oferta_porcentaje = valor if evento_oferta_activa else 0
        estado = f"ACTIVADO {valor}%" if evento_oferta_activa else "DESACTIVADO"
    else:
        return await ctx.send(embed=discord.Embed(title="❌ Error", description="Uso: !evento 2x1 on/off\n!evento cashbackdoble on/off\n!evento oferta on/off [%]", color=COLORS['error']))
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute('UPDATE eventos_activos SET evento_2x1 = ?, cashback_doble = ?, oferta_activa = ?, oferta_porcentaje = ? WHERE id = 1', (evento_2x1, evento_cashback_doble, evento_oferta_activa, evento_oferta_porcentaje))
        await db.commit()
    await ctx.message.delete()
    await ctx.send(embed=discord.Embed(title="🎉 Evento", description=f"{tipo.upper()} {estado}", color=COLORS['success']))

@bot.command(name="config")
async def cmd_config(ctx, accion: str, key: str = None, valor: str = None):
    if not await check_ceo(ctx):
        return
    if accion == "get" and key:
        async with aiosqlite.connect(DB_PATH) as db:
            cur = await db.execute('SELECT value, descripcion FROM config_global WHERE key = ?', (key,))
            r = await cur.fetchone()
            if r:
                await ctx.send(embed=discord.Embed(title="⚙️ Configuración", description=f"**{key}** = `{r[0]}`\n📝 {r[1]}", color=COLORS['info']))
            else:
                await ctx.send(embed=discord.Embed(title="❌ Error", description="Clave no encontrada", color=COLORS['error']))
    elif accion == "set" and key and valor:
        async with aiosqlite.connect(DB_PATH) as db:
            await db.execute('UPDATE config_global SET value = ?, actualizado_por = ?, fecha_actualizacion = CURRENT_TIMESTAMP WHERE key = ?', (valor, str(ctx.author.id), key))
            await db.commit()
        await ctx.message.delete()
        await ctx.send(embed=discord.Embed(title="✅ Configurado", description=f"{key} = {valor}", color=COLORS['success']))
    elif accion == "list":
        async with aiosqlite.connect(DB_PATH) as db:
            cur = await db.execute('SELECT key, value FROM config_global ORDER BY key')
            confs = await cur.fetchall()
        e = discord.Embed(title="📋 Configuración global", color=COLORS['primary'])
        for k, v in confs:
            e.add_field(name=k, value=f"`{v}`", inline=True)
        await ctx.send(embed=e)
    else:
        await ctx.send(embed=discord.Embed(title="❌ Error", description="Uso: !config get [key] | !config set [key] [valor] | !config list", color=COLORS['error']))

@bot.command(name="backup")
async def cmd_backup(ctx):
    if not await check_ceo(ctx):
        return
    fecha = datetime.now().strftime("%Y%m%d_%H%M%S")
    archivo = f"backups/backup_{fecha}.db"
    shutil.copy2(DB_PATH, archivo)
    await ctx.author.send(file=discord.File(archivo))
    await ctx.message.delete()
    await ctx.send(embed=discord.Embed(title="✅ Backup creado", description="Revisa tu DM", color=COLORS['success']))

@bot.command(name="resetallsistema")
async def cmd_reset_all_sistema(ctx):
    if not await check_ceo(ctx):
        return
    await ctx.send(embed=discord.Embed(title="⚠️ REINICIO TOTAL", description="Escribe `!confirmarreset` en 30 segundos", color=COLORS['warning']))
    def check(m):
        return m.author.id == ctx.author.id and m.content == "!confirmarreset"
    try:
        await bot.wait_for("message", timeout=30, check=check)
        async with aiosqlite.connect(DB_PATH) as db:
            await db.execute("DELETE FROM transacciones")
            await db.execute("DELETE FROM boletos")
            await db.execute("DELETE FROM vendedores")
            await db.execute("DELETE FROM clientes")
            await db.execute("DELETE FROM usuarios_balance")
            await db.execute("DELETE FROM rifas")
            await db.execute("DELETE FROM referidos_codigos")
            await db.execute("DELETE FROM referidos_relaciones")
            await db.execute("DELETE FROM fidelizacion")
            await db.execute("DELETE FROM cashback")
            await db.execute("DELETE FROM cajas_compradas")
            await db.execute("DELETE FROM cajas_historial")
            await db.execute("DELETE FROM inversiones")
            await db.execute("DELETE FROM subastas")
            await db.execute("DELETE FROM pujas")
            await db.execute("DELETE FROM tickets_cambio")
            await db.execute("DELETE FROM regalos")
            await db.execute("DELETE FROM marketplace_listings")
            await db.execute("DELETE FROM marketplace_ofertas")
            await db.execute("DELETE FROM ruleta_historial")
            await db.execute("DELETE FROM apuestas")
            await db.execute("DELETE FROM sqlite_sequence")
            await db.commit()
        await ctx.send(embed=discord.Embed(title="✅ Sistema reiniciado", color=COLORS['success']))
    except asyncio.TimeoutError:
        await ctx.send(embed=discord.Embed(title="❌ Tiempo expirado", color=COLORS['error']))

# ============================================
# COMANDOS FALTANTES (RULETA, APUESTAS, MARKETPLACE, REGALOS, ETC)
# ============================================

@bot.command(name="ruleta")
async def cmd_ruleta(ctx):
    if not await verificar_canal(ctx):
        return
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute('SELECT activo, cooldown_horas, premios, probabilidades FROM ruleta_config WHERE id = 1')
        cfg = await cur.fetchone()
        if not cfg or not cfg[0]:
            return await ctx.send(embed=discord.Embed(title="❌ Error", description="Ruleta desactivada", color=COLORS['error']))
        cur = await db.execute('SELECT ultimo_giro FROM ruleta_usuarios WHERE usuario_id = ?', (str(ctx.author.id),))
        last = await cur.fetchone()
        if last and last[0]:
            prox = datetime.fromisoformat(last[0]) + timedelta(hours=cfg[1])
            if datetime.now() < prox:
                resto = prox - datetime.now()
                h = resto.total_seconds() // 3600
                m = (resto.total_seconds() % 3600) // 60
                return await ctx.send(embed=discord.Embed(title="❌ Error", description=f"Próximo giro en {int(h)}h {int(m)}m", color=COLORS['error']))
        premios = json.loads(cfg[2])
        probs = json.loads(cfg[3])
        elegido = random.choices(premios, weights=probs, k=1)[0]
        await db.execute('INSERT OR REPLACE INTO ruleta_usuarios (usuario_id, ultimo_giro) VALUES (?, ?)', (str(ctx.author.id), datetime.now()))
        await db.execute('INSERT INTO ruleta_historial (usuario_id, usuario_nick, premio) VALUES (?, ?, ?)', (str(ctx.author.id), ctx.author.name, elegido))
        if elegido > 0:
            await db.execute('UPDATE usuarios_balance SET balance = balance + ? WHERE discord_id = ?', (elegido, str(ctx.author.id)))
        await db.commit()
    await ctx.send(embed=discord.Embed(title="🎡 RULETA", description=f"Giraste y obtuviste **${elegido:,} VP$**", color=COLORS['success'] if elegido > 0 else COLORS['error']))

@bot.command(name="ruleta_stats")
async def cmd_ruleta_stats(ctx, usuario: discord.Member = None):
    if not await verificar_canal(ctx):
        return
    target = usuario or ctx.author
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute('SELECT COUNT(*), SUM(premio) FROM ruleta_historial WHERE usuario_id = ?', (str(target.id),))
        stats = await cur.fetchone()
    total = stats[0] or 0
    ganado = stats[1] or 0
    await ctx.send(embed=discord.Embed(title=f"🎡 Estadísticas de ruleta - {target.name}", description=f"Giros: {total}\nGanado: ${ganado:,} VP$", color=COLORS['primary']))

@bot.command(name="apostar")
async def cmd_apostar(ctx, numero: int, cantidad: int):
    if not await verificar_canal(ctx):
        return
    r = await bot.db.get_rifa_activa()
    if not r:
        return await ctx.send(embed=discord.Embed(title="❌ Error", description="No hay rifa activa", color=COLORS['error']))
    if numero < 1 or numero > r['total_boletos']:
        return await ctx.send(embed=discord.Embed(title="❌ Error", description=f"Número 1-{r['total_boletos']}", color=COLORS['error']))
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute('SELECT activo, apuesta_minima, apuesta_maxima, multiplicador_base, multiplicador_especial, numero_especial FROM apuestas_config WHERE id = 1')
        cfg = await cur.fetchone()
        if not cfg or not cfg[0]:
            return await ctx.send(embed=discord.Embed(title="❌ Error", description="Apuestas desactivadas", color=COLORS['error']))
        if cantidad < cfg[1] or cantidad > cfg[2]:
            return await ctx.send(embed=discord.Embed(title="❌ Error", description=f"Apuesta entre ${cfg[1]:,} y ${cfg[2]:,} VP$", color=COLORS['error']))
        cur = await db.execute('SELECT balance FROM usuarios_balance WHERE discord_id = ?', (str(ctx.author.id),))
        bal = (await cur.fetchone() or [0])[0]
        if bal < cantidad:
            return await ctx.send(embed=discord.Embed(title="❌ Error", description=f"Necesitas ${cantidad:,} VP$", color=COLORS['error']))
        multi = cfg[4] if numero == cfg[5] else cfg[3]
        ganancia = cantidad * multi
        await db.execute('UPDATE usuarios_balance SET balance = balance - ? WHERE discord_id = ?', (cantidad, str(ctx.author.id)))
        await db.execute('INSERT INTO apuestas (rifa_id, usuario_id, usuario_nick, numero_apostado, monto, ganancia_potencial) VALUES (?, ?, ?, ?, ?, ?)', (r['id'], str(ctx.author.id), ctx.author.name, numero, cantidad, ganancia))
        await db.commit()
    await ctx.send(embed=discord.Embed(title="🎲 Apuesta registrada", description=f"Apostaste ${cantidad:,} VP$ al número **{numero}**\nGanancia potencial: **${ganancia:,} VP$**", color=COLORS['info']))

@bot.command(name="mis_apuestas")
async def cmd_mis_apuestas(ctx):
    if not await verificar_canal(ctx):
        return
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute('SELECT numero_apostado, monto, ganancia_potencial FROM apuestas WHERE usuario_id = ? AND estado = "activa"', (str(ctx.author.id),))
        ap = await cur.fetchall()
    if not ap:
        return await ctx.send(embed=discord.Embed(title="❌ Info", description="No tienes apuestas activas", color=COLORS['info']))
    e = discord.Embed(title="🎲 Tus apuestas activas", color=COLORS['primary'])
    for n, m, g in ap:
        e.add_field(name=f"#{n}", value=f"${m:,} → ${g:,}", inline=False)
    await ctx.send(embed=e)

# ============================================
# MARKETPLACE (COMANDOS BÁSICOS)
# ============================================

@bot.command(name="marketplace")
async def cmd_marketplace(ctx, pagina: int = 1):
    if not await verificar_canal(ctx):
        return
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute('SELECT * FROM marketplace_listings WHERE estado = "activo" ORDER BY fecha_publicacion DESC')
        listings = await cur.fetchall()
    if not listings:
        return await ctx.send(embed=discord.Embed(title="🛒 Marketplace", description="No hay boletos en venta", color=COLORS['info']))
    items_por_pag = 10
    total_pag = (len(listings) + items_por_pag - 1) // items_por_pag
    if pagina > total_pag:
        pagina = total_pag
    inicio = (pagina-1)*items_por_pag
    fin = inicio+items_por_pag
    e = discord.Embed(title="🛒 MARKETPLACE", description=f"Página {pagina}/{total_pag}", color=COLORS['primary'])
    for l in listings[inicio:fin]:
        e.add_field(name=f"#{l[4]} - {l[5]}", value=f"💰 ${l[6]:,} VP$\n👤 Vendedor: {l[2]}\n`!comprar_boleto {l[0]}`", inline=False)
    await ctx.send(embed=e)

@bot.command(name="vender_boleto")
async def cmd_vender_boleto(ctx, numero: int, precio: int):
    if not await verificar_canal(ctx):
        return
    r = await bot.db.get_rifa_activa()
    if not r:
        return await ctx.send(embed=discord.Embed(title="❌ Error", description="No hay rifa activa", color=COLORS['error']))
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute('SELECT * FROM boletos WHERE rifa_id = ? AND numero = ? AND comprador_id = ? AND estado = "pagado"', (r['id'], numero, str(ctx.author.id)))
        b = await cur.fetchone()
        if not b:
            return await ctx.send(embed=discord.Embed(title="❌ Error", description="No tienes ese boleto o ya está en venta", color=COLORS['error']))
        fecha_exp = datetime.now() + timedelta(days=7)
        await db.execute('INSERT INTO marketplace_listings (vendedor_id, vendedor_nick, rifa_id, rifa_nombre, numero, precio_venta, fecha_expiracion) VALUES (?, ?, ?, ?, ?, ?, ?)', (str(ctx.author.id), ctx.author.name, r['id'], r['nombre'], numero, precio, fecha_exp))
        await db.execute('UPDATE boletos SET estado = "en_venta" WHERE rifa_id = ? AND numero = ? AND comprador_id = ?', (r['id'], numero, str(ctx.author.id)))
        await db.commit()
    await ctx.send(embed=discord.Embed(title="✅ Boleto en venta", description=f"Has puesto a la venta el boleto #{numero} por ${precio:,} VP$", color=COLORS['success']))

@bot.command(name="comprar_boleto")
async def cmd_comprar_boleto(ctx, listing_id: int):
    if not await verificar_canal(ctx):
        return
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute('SELECT * FROM marketplace_listings WHERE id = ? AND estado = "activo"', (listing_id,))
        l = await cur.fetchone()
        if not l:
            return await ctx.send(embed=discord.Embed(title="❌ Error", description="Listado no encontrado", color=COLORS['error']))
        if l[1] == str(ctx.author.id):
            return await ctx.send(embed=discord.Embed(title="❌ Error", description="No puedes comprar tus propios boletos", color=COLORS['error']))
        cur2 = await db.execute('SELECT balance FROM usuarios_balance WHERE discord_id = ?', (str(ctx.author.id),))
        bal = (await cur2.fetchone() or [0])[0]
        if bal < l[6]:
            return await ctx.send(embed=discord.Embed(title="❌ Error", description=f"Necesitas ${l[6]:,} VP$", color=COLORS['error']))
        comision_porc = 5
        comision = int(l[6] * comision_porc / 100)
        vendedor_recibe = l[6] - comision
        await db.execute('UPDATE usuarios_balance SET balance = balance - ? WHERE discord_id = ?', (l[6], str(ctx.author.id)))
        await db.execute('UPDATE usuarios_balance SET balance = balance + ? WHERE discord_id = ?', (vendedor_recibe, l[1]))
        await db.execute('UPDATE boletos SET comprador_id = ?, comprador_nick = ?, estado = "pagado" WHERE rifa_id = ? AND numero = ?', (str(ctx.author.id), ctx.author.name, l[3], l[4]))
        await db.execute('UPDATE marketplace_listings SET estado = "vendido", comprador_id = ?, comprador_nick = ?, fecha_venta = CURRENT_TIMESTAMP WHERE id = ?', (str(ctx.author.id), ctx.author.name, listing_id))
        await db.commit()
    await verificar_logros(str(ctx.author.id), ctx.author.name, 'compra_marketplace', 1)
    await verificar_logros(l[1], l[2], 'venta_marketplace', 1)
    await ctx.send(embed=discord.Embed(title="✅ Compra realizada", description=f"Compraste el boleto #{l[4]} por ${l[6]:,} VP$", color=COLORS['success']))
    await enviar_dm(l[1], "🛒 Boleto vendido", f"{ctx.author.name} compró tu boleto #{l[4]} por ${l[6]:,} VP$")

@bot.command(name="mis_listados")
async def cmd_mis_listados(ctx):
    if not await verificar_canal(ctx):
        return
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute('SELECT id, numero, precio_venta FROM marketplace_listings WHERE vendedor_id = ? AND estado = "activo"', (str(ctx.author.id),))
        listados = await cur.fetchall()
    if not listados:
        return await ctx.send(embed=discord.Embed(title="❌ Info", description="No tienes listados activos", color=COLORS['info']))
    e = discord.Embed(title="📊 Tus listados", color=COLORS['primary'])
    for lid, num, precio in listados:
        e.add_field(name=f"#{lid} - Boleto #{num}", value=f"Precio: ${precio:,} VP$", inline=False)
    await ctx.send(embed=e)

# ============================================
# REGALOS
# ============================================

@bot.command(name="regalar")
async def cmd_regalar(ctx, usuario: discord.Member, cantidad: int, *, mensaje: str = None):
    if not await verificar_canal(ctx):
        return
    if usuario.id == ctx.author.id:
        return await ctx.send(embed=discord.Embed(title="❌ Error", description="No puedes regalarte a ti mismo", color=COLORS['error']))
    if cantidad <= 0:
        return await ctx.send(embed=discord.Embed(title="❌ Error", description="Cantidad positiva", color=COLORS['error']))
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute('SELECT balance FROM usuarios_balance WHERE discord_id = ?', (str(ctx.author.id),))
        bal = (await cur.fetchone() or [0])[0]
        if bal < cantidad:
            return await ctx.send(embed=discord.Embed(title="❌ Error", description=f"No tienes suficientes VP$. Necesitas {cantidad:,} VP$", color=COLORS['error']))
        await db.execute('UPDATE usuarios_balance SET balance = balance - ? WHERE discord_id = ?', (cantidad, str(ctx.author.id)))
        await db.execute('INSERT INTO regalos (remitente_id, remitente_nick, destinatario_id, destinatario_nick, tipo, cantidad, mensaje) VALUES (?, ?, ?, ?, "vp", ?, ?)', (str(ctx.author.id), ctx.author.name, str(usuario.id), usuario.name, cantidad, mensaje))
        await db.commit()
    await ctx.send(embed=discord.Embed(title="🎁 Regalo enviado", description=f"Has enviado ${cantidad:,} VP$ a {usuario.mention}", color=COLORS['success']))
    await enviar_dm(str(usuario.id), "🎁 Recibiste un regalo", f"{ctx.author.name} te ha enviado ${cantidad:,} VP$\nMensaje: {mensaje or 'Sin mensaje'}\nUsa `!solicitudes` para aceptar")

@bot.command(name="solicitudes")
async def cmd_solicitudes(ctx):
    if not await verificar_canal(ctx):
        return
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute('SELECT id, remitente_nick, cantidad, mensaje FROM regalos WHERE destinatario_id = ? AND estado = "pendiente"', (str(ctx.author.id),))
        regalos = await cur.fetchall()
    if not regalos:
        return await ctx.send(embed=discord.Embed(title="❌ Info", description="No tienes regalos pendientes", color=COLORS['info']))
    e = discord.Embed(title="🎁 Regalos pendientes", color=COLORS['primary'])
    for rid, rem, cant, msg in regalos:
        e.add_field(name=f"ID: {rid} - De: {rem}", value=f"Monto: ${cant:,} VP$\nMensaje: {msg or 'Sin mensaje'}\n`!aceptar {rid}` o `!rechazar {rid}`", inline=False)
    await ctx.send(embed=e)

@bot.command(name="aceptar")
async def cmd_aceptar(ctx, regalo_id: int):
    if not await verificar_canal(ctx):
        return
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute('SELECT * FROM regalos WHERE id = ? AND destinatario_id = ? AND estado = "pendiente"', (regalo_id, str(ctx.author.id)))
        r = await cur.fetchone()
        if not r:
            return await ctx.send(embed=discord.Embed(title="❌ Error", description="Regalo no encontrado", color=COLORS['error']))
        await db.execute('UPDATE usuarios_balance SET balance = balance + ? WHERE discord_id = ?', (r[6], str(ctx.author.id)))
        await db.execute('UPDATE regalos SET estado = "aceptado", fecha_respuesta = CURRENT_TIMESTAMP WHERE id = ?', (regalo_id,))
        await db.commit()
    await ctx.send(embed=discord.Embed(title="✅ Regalo aceptado", description=f"Aceptaste ${r[6]:,} VP$ de {r[2]}", color=COLORS['success']))
    await enviar_dm(r[1], "🎁 Regalo aceptado", f"{ctx.author.name} aceptó tu regalo de ${r[6]:,} VP$")

@bot.command(name="rechazar")
async def cmd_rechazar(ctx, regalo_id: int):
    if not await verificar_canal(ctx):
        return
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute('SELECT * FROM regalos WHERE id = ? AND destinatario_id = ? AND estado = "pendiente"', (regalo_id, str(ctx.author.id)))
        r = await cur.fetchone()
        if not r:
            return await ctx.send(embed=discord.Embed(title="❌ Error", description="Regalo no encontrado", color=COLORS['error']))
        await db.execute('UPDATE usuarios_balance SET balance = balance + ? WHERE discord_id = ?', (r[6], r[1]))
        await db.execute('UPDATE regalos SET estado = "rechazado", fecha_respuesta = CURRENT_TIMESTAMP WHERE id = ?', (regalo_id,))
        await db.commit()
    await ctx.send(embed=discord.Embed(title="❌ Regalo rechazado", description=f"Rechazaste ${r[6]:,} VP$ de {r[2]}", color=COLORS['error']))
    await enviar_dm(r[1], "🎁 Regalo rechazado", f"{ctx.author.name} rechazó tu regalo de ${r[6]:,} VP$. Los VP$ han sido devueltos.")

# ============================================
# EJECUCIÓN
# ============================================

if __name__ == "__main__":
    try:
        if not config.BOT_TOKEN:
            print("❌ No hay BOT_TOKEN en config.py")
            sys.exit(1)
        bot.run(config.BOT_TOKEN)
    except Exception as e:
        logger.error(f"Error fatal: {e}")
        traceback.print_exc()
