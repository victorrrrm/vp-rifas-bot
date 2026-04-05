import discord
from discord.ext import commands, tasks
from discord.ui import Select, View
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
from src.database.database import Database
import src.utils.embeds as embeds

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
reconnect_attempts = 0
MAX_RECONNECT_ATTEMPTS = 999999

# Variables de eventos activos (se guardan en DB)
evento_2x1 = False
evento_cashback_doble = False
evento_oferta_porcentaje = 0
evento_oferta_activa = False
sorteo_en_curso = False
sorteo_cancelado = False

# Configuración de sistemas (se cargan desde DB)
REFERIDOS_PORCENTAJE = 10
REFERIDOS_DESCUENTO = 10
CASHBACK_PORCENTAJE = 10
COMISION_VENDEDOR = 10

# Jackpot
jackpot_activo = False
jackpot_base = 0
jackpot_porcentaje = 0
jackpot_rifa_id = 0
jackpot_total = 0
jackpot_canal_id = 1486253228499931228

# Rifa Eliminación
rifa_eliminacion_activa = False
rifa_eliminacion_total = 0
rifa_eliminacion_premio = ""
rifa_eliminacion_valor = 0
rifa_eliminacion_numeros = []
rifa_eliminacion_canal_id = 1486257489560342548

# Ranking de compradores por rifa
ranking_rifa = {}

# IDs de canales y roles
CATEGORIA_RIFAS = 1482835014604296283
CATEGORIA_FRANQUICIAS = 1489816515774517278
CATEGORIA_TICKETS = None  # se crea automáticamente

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

ROLES_FIDELIZACION = {
    'BRONCE': 1483720270496661515,
    'PLATA': 1483720387178139758,
    'ORO': 1483720490601418822,
    'PLATINO': 1483720672185155625,
    'DIAMANTE': 1483720783422296165,
    'MASTER': 1483721013144584192
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
    'premium': 0x9B59B6
}

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

class VPRifasBot(commands.Bot):
    def __init__(self):
        super().__init__(
            command_prefix=PREFIX,
            intents=intents,
            activity=discord.Activity(
                type=discord.ActivityType.watching,
                name=f"{PREFIX}ayuda | Rifas VP v{VERSION}"
            )
        )
        self.db = Database()
        self.update_channel_id = 1483378335831560202
        self.reconnecting = False
        self.ultimo_heartbeat = datetime.now()
        self.volumen_montado = VOLUME_MOUNTED

    async def setup_hook(self):
        logger.info("🚀 Iniciando configuración...")
        if self.volumen_montado:
            logger.info("✅ Volumen persistente detectado")
        else:
            logger.warning("⚠️ Volumen no detectado")
        try:
            await self.db.init_db()
            await self.init_sistemas_tablas()
            await self.cargar_configuraciones()
            await self.cargar_eventos_activos()
            await self.crear_categoria_tickets()
            logger.info("✅ Base de datos inicializada")
        except Exception as e:
            logger.error(f"❌ Error: {e}")
            traceback.print_exc()
        self.keep_alive_task.start()
        self.status_task.start()
        self.actualizar_jackpot_task.start()
        self.verificar_subastas_task.start()
        self.verificar_eventos_task.start()
        self.backup_automatico_task.start()
        self.reset_misiones_semanales_task.start()
        logger.info("✅ Tareas automáticas iniciadas")

    async def crear_categoria_tickets(self):
        global CATEGORIA_TICKETS
        guild = self.get_guild(config.GUILD_ID)
        if guild:
            for cat in guild.categories:
                if cat.name == "🎫 TICKETS":
                    CATEGORIA_TICKETS = cat.id
                    break
            if not CATEGORIA_TICKETS:
                categoria = await guild.create_category("🎫 TICKETS")
                CATEGORIA_TICKETS = categoria.id
                await categoria.set_permissions(guild.default_role, read_messages=False)

    async def cargar_configuraciones(self):
        global COMISION_VENDEDOR, CASHBACK_PORCENTAJE, REFERIDOS_PORCENTAJE, REFERIDOS_DESCUENTO
        async with aiosqlite.connect(DB_PATH) as db:
            cursor = await db.execute('SELECT * FROM config_global WHERE id = 1')
            row = await cursor.fetchone()
            if row:
                COMISION_VENDEDOR = row[2]
                CASHBACK_PORCENTAJE = row[1]
                REFERIDOS_PORCENTAJE = row[3]
                REFERIDOS_DESCUENTO = row[4]

    async def cargar_eventos_activos(self):
        global evento_2x1, evento_cashback_doble, evento_oferta_activa, evento_oferta_porcentaje
        global jackpot_activo, jackpot_total, jackpot_base, jackpot_porcentaje, jackpot_rifa_id
        async with aiosqlite.connect(DB_PATH) as db:
            cursor = await db.execute('SELECT * FROM eventos_activos WHERE id = 1')
            evento = await cursor.fetchone()
            if evento:
                evento_2x1 = evento[1] == 1
                evento_cashback_doble = evento[2] == 1
                evento_oferta_activa = evento[3] == 1
                evento_oferta_porcentaje = evento[4] or 0
                jackpot_activo = evento[5] == 1
                jackpot_total = evento[6] or 0
                jackpot_base = evento[7] or 0
                jackpot_porcentaje = evento[8] or 0
                jackpot_rifa_id = evento[9] or 0
        # Cargar rifa eliminación
        async with aiosqlite.connect(DB_PATH) as db:
            cursor = await db.execute('SELECT * FROM rifa_eliminacion WHERE id = 1')
            re = await cursor.fetchone()
            if re and re[1]:
                global rifa_eliminacion_activa, rifa_eliminacion_total, rifa_eliminacion_premio, rifa_eliminacion_valor, rifa_eliminacion_numeros
                rifa_eliminacion_activa = re[1] == 1
                rifa_eliminacion_total = re[2]
                rifa_eliminacion_premio = re[3]
                rifa_eliminacion_valor = re[4]
                rifa_eliminacion_numeros = json.loads(re[5]) if re[5] else []

    async def init_sistemas_tablas(self):
        async with aiosqlite.connect(DB_PATH) as db:
            # ===== TABLAS EXISTENTES =====
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
            await db.execute('''
                CREATE TABLE IF NOT EXISTS usuarios_balance (
                    discord_id TEXT PRIMARY KEY,
                    nombre TEXT NOT NULL,
                    balance INTEGER DEFAULT 0,
                    ultima_actualizacion TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            ''')
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
            # Referidos
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
                CREATE TABLE IF NOT EXISTS referidos_comisiones (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    referidor_id TEXT NOT NULL,
                    referido_id TEXT NOT NULL,
                    compra_id INTEGER,
                    monto_compra INTEGER NOT NULL,
                    porcentaje INTEGER NOT NULL,
                    comision INTEGER NOT NULL,
                    fecha TIMESTAMP DEFAULT CURRENT_TIMESTAMP
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
            # Fidelización
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
            # Cashback
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
            # Cajas misteriosas
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
            cajas_def = [
                ('comun', '📦 Caja Común', 5000, '[1000,2000,5000,10000,15000]', '[50,25,15,7,3]', 1),
                ('rara', '🎁 Caja Rara', 25000, '[5000,10000,25000,50000,100000]', '[45,25,15,10,5]', 1),
                ('epica', '✨ Caja Épica', 100000, '[25000,50000,100000,250000,500000]', '[40,25,15,12,8]', 1),
                ('legendaria', '👑 Caja Legendaria', 500000, '[100000,250000,500000,1000000,2000000]', '[35,25,18,12,10]', 1),
                ('misteriosa', '❓ Caja Misteriosa', 10000, '[0,1000,2500,5000,10000,25000,50000,100000,500000]', '[50,15,10,8,6,4,3,2,2]', 1)
            ]
            for c in cajas_def:
                await db.execute('INSERT OR IGNORE INTO cajas (tipo, nombre, precio, premios, probabilidades, activo) VALUES (?, ?, ?, ?, ?, ?)', c)
            # Distribuidores
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
            niv_dist = [
                (1, 'Bronce', 5, 0, 5, '[]', 0, 0),
                (2, 'Plata', 7, 2, 10, '[]', 50, 100000),
                (3, 'Oro', 10, 3, 15, '[]', 200, 500000),
                (4, 'Platino', 12, 3, 20, '[]', 500, 1000000),
                (5, 'Diamante', 15, 5, 25, '[]', 1000, 2500000),
                (6, 'Elite', 20, 5, 30, '[]', 2500, 5000000)
            ]
            for nd in niv_dist:
                await db.execute('INSERT OR IGNORE INTO distribuidores_niveles VALUES (?, ?, ?, ?, ?, ?, ?, ?)', nd)
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
            prods_def = [
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
            for p in prods_def:
                await db.execute('INSERT OR IGNORE INTO productos (nombre, descripcion, precio_normal, precio_mayorista, nivel_minimo, stock, activo) VALUES (?, ?, ?, ?, ?, ?, ?)', p)
            # Misiones diarias y semanales
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
            misiones_def = [
                ('Bienvenido', 'Envía 10 mensajes', 'mensajes', 10, 100, 'diaria', 1),
                ('Social', 'Reacciona a 5 mensajes', 'reacciones', 5, 50, 'diaria', 1),
                ('Comprador', 'Compra 1 boleto', 'compra', 1, 500, 'diaria', 1),
                ('Inversor', 'Invierte en el banco', 'inversion', 1, 1000, 'diaria', 1),
                ('Cajero', 'Abre 3 cajas', 'cajas', 3, 5000, 'diaria', 1),
                ('Maratón', 'Completa 7 días seguidos', 'racha', 7, 25000, 'especial', 1)
            ]
            for m in misiones_def:
                await db.execute('INSERT OR IGNORE INTO misiones (nombre, descripcion, requisito, valor_requisito, recompensa, tipo, activo) VALUES (?, ?, ?, ?, ?, ?, ?)', m)
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
            # Banco e inversiones
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
            prods_banco = [
                ('Básico', 7, 5, 10000, 500000, 10, 1),
                ('Plus', 14, 12, 50000, 2000000, 15, 1),
                ('VIP', 30, 25, 200000, 10000000, 20, 1),
                ('Elite', 60, 40, 1000000, 50000000, 25, 1)
            ]
            for pb in prods_banco:
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
            # Tickets de cambio
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
            # Jackpot y eventos activos
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
            # Eventos programados
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
            # Config global
            await db.execute('''
                CREATE TABLE IF NOT EXISTS config_global (
                    id INTEGER PRIMARY KEY CHECK (id = 1),
                    cashback_porcentaje INTEGER DEFAULT 10,
                    comision_vendedor INTEGER DEFAULT 10,
                    referidos_porcentaje INTEGER DEFAULT 10,
                    referidos_descuento INTEGER DEFAULT 10
                )
            ''')
            await db.execute('INSERT OR IGNORE INTO config_global (id) VALUES (1)')
            # Puntos revancha
            await db.execute('''
                CREATE TABLE IF NOT EXISTS puntos_revancha (
                    usuario_id TEXT PRIMARY KEY,
                    puntos INTEGER DEFAULT 0,
                    ultima_actualizacion TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            ''')
            # Códigos promocionales
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
            # Ganadores históricos
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
            # Subastas
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
            # Logros
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
            logros_def = [
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
            for l in logros_def:
                await db.execute('INSERT OR IGNORE INTO logros (nombre, descripcion, emoji, categoria, condicion_tipo, condicion_valor, recompensa_vp, recompensa_rol_id, recompensa_titulo, orden, activo) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)', l)
            # Notificaciones
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
            # Regalos
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
            # Marketplace
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
            # Personalización de perfil
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
                ('background', 'Atardecer VP', 'Hermoso atardecer', '🌅', 10000, None, 'comun', 0, 1, 1),
                ('background', 'Casino Royale', 'Luces de neón', '🎰', 50000, None, 'rara', 0, 1, 2),
                ('background', 'Cielo Estrellado', 'Estrellas infinitas', '🌌', 100000, None, 'epica', 0, 1, 3),
                ('background', 'Infinito VP', 'El infinito', '♾️', 500000, None, 'legendaria', 0, 1, 4),
                ('marco', 'Dorado', 'Marco dorado', '✨', 25000, None, 'rara', 0, 1, 1),
                ('marco', 'Diamante', 'Marco de diamantes', '💎', 100000, None, 'epica', 0, 1, 2),
                ('marco', 'Fuego', 'Marco ardiente', '🔥', 250000, None, 'legendaria', 0, 1, 3),
                ('badge', 'Veterano', 'Por años de servicio', '🎖️', 0, None, 'especial', 1, 1, 1),
                ('badge', 'Ballena', 'Por comprar 1000 boletos', '🐳', 0, None, 'legendaria', 1, 1, 2),
                ('efecto', 'Partículas', 'Partículas brillantes', '✨', 50000, None, 'rara', 0, 1, 1),
                ('efecto', 'Arcoíris', 'Colores del arcoíris', '🌈', 150000, None, 'epica', 0, 1, 2)
            ]
            for it in items_pers:
                await db.execute('INSERT OR IGNORE INTO personalizacion_items (tipo, nombre, descripcion, emoji, precio, imagen_url, rareza, exclusivo, activo, orden) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)', it)
            # Ruleta diaria
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
            prem_def = json.dumps([100, 500, 1000, 5000, 10000, 50000, 100000, 500000])
            prob_def = json.dumps([30, 25, 20, 12, 8, 3, 1.5, 0.5])
            col_def = json.dumps(["⚪", "🟢", "🟢", "🟡", "🟡", "🟠", "🔴", "🟣"])
            desc_def = json.dumps(["😅 Casi...", "👍 No está mal", "🙂 Bien", "🎉 Buena suerte", "🎊 Excelente", "🤯 Increíble", "💎 ESPECTACULAR", "👑 JACKPOT"])
            await db.execute('INSERT OR IGNORE INTO ruleta_config (id, activo, cooldown_horas, premios, probabilidades, colores, descripciones) VALUES (1, 1, 24, ?, ?, ?, ?)', (prem_def, prob_def, col_def, desc_def))
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
            # Apuestas
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
            # Franquicias y rifa eliminación
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
        logger.info("✅ Tablas inicializadas")

    @tasks.loop(seconds=5)
    async def actualizar_jackpot_task(self):
        global jackpot_activo, jackpot_total, jackpot_canal_id
        if not jackpot_activo:
            return
        try:
            canal = self.get_channel(jackpot_canal_id)
            if not canal:
                return
            mensaje_jackpot = None
            async for msg in canal.history(limit=50):
                if msg.author == self.user and msg.pinned:
                    mensaje_jackpot = msg
                    break
            embed = discord.Embed(
                title="🎰 **JACKPOT ACTIVO** 🎰",
                description=f"**Premio acumulado:** ${jackpot_total:,} VP$",
                color=0xFFD700
            )
            embed.add_field(name="💎 Base", value=f"${jackpot_base:,}", inline=True)
            embed.add_field(name="📊 % por boleto", value=f"{jackpot_porcentaje}%", inline=True)
            embed.set_footer(text="¡Cada boleto comprado aumenta el premio!")
            if mensaje_jackpot:
                await mensaje_jackpot.edit(embed=embed)
            else:
                msg = await canal.send(embed=embed)
                await msg.pin()
        except:
            pass

    @tasks.loop(minutes=1)
    async def verificar_subastas_task(self):
        async with aiosqlite.connect(DB_PATH) as db:
            cursor = await db.execute('SELECT * FROM subastas WHERE estado = "activa" AND fecha_fin <= datetime("now")')
            subastas = await cursor.fetchall()
            for subasta in subastas:
                await self.finalizar_subasta(subasta[0])

    async def finalizar_subasta(self, subasta_id):
        async with aiosqlite.connect(DB_PATH) as db:
            db.row_factory = aiosqlite.Row
            cursor = await db.execute('SELECT * FROM subastas WHERE id = ?', (subasta_id,))
            subasta = await cursor.fetchone()
            if not subasta:
                return
            cursor = await db.execute('SELECT * FROM pujas WHERE subasta_id = ? ORDER BY monto DESC LIMIT 1', (subasta_id,))
            ganador = await cursor.fetchone()
            if ganador:
                await db.execute('UPDATE subastas SET estado = "finalizada", ganador_id = ?, ganador_nick = ?, precio_actual = ? WHERE id = ?',
                               (ganador['usuario_id'], ganador['usuario_nick'], ganador['monto'], subasta_id))
                await self.crear_ticket_subasta(subasta_id, ganador['usuario_id'], ganador['usuario_nick'], subasta['item_nombre'], ganador['monto'])
                await self.enviar_notificacion(ganador['usuario_id'], 'subasta', '🎉 Ganaste una subasta', f"Ganaste {subasta['item_nombre']} por ${ganador['monto']:,} VP$")
                await self.verificar_logros(ganador['usuario_id'], ganador['usuario_nick'], 'subasta', 1)
            else:
                await db.execute('UPDATE subastas SET estado = "cancelada" WHERE id = ?', (subasta_id,))
            await db.commit()

    async def crear_ticket_subasta(self, subasta_id, usuario_id, usuario_nick, item, monto):
        guild = self.get_guild(config.GUILD_ID)
        categoria = guild.get_channel(CATEGORIA_TICKETS)
        if not categoria:
            return
        overwrites = {
            guild.default_role: discord.PermissionOverwrite(read_messages=False),
            guild.get_member(int(usuario_id)): discord.PermissionOverwrite(read_messages=True, send_messages=True),
        }
        for rid in [ROLES['CEO'], ROLES['DIRECTOR']]:
            rol = guild.get_role(rid)
            if rol:
                overwrites[rol] = discord.PermissionOverwrite(read_messages=True, send_messages=True)
        canal = await categoria.create_text_channel(f"ticket-subasta-{subasta_id}", overwrites=overwrites)
        embed = discord.Embed(
            title="🎉 ¡FELICIDADES! 🎉",
            description=f"Has ganado la subasta #{subasta_id}",
            color=COLORS['success']
        )
        embed.add_field(name="🏆 Premio", value=item, inline=True)
        embed.add_field(name="💰 Monto pagado", value=f"${monto:,} VP$", inline=True)
        embed.set_footer(text="Un staff te entregará el premio. Usa !ticketcerrar para cerrar")
        await canal.send(f"<@{usuario_id}>", embed=embed)
        async with aiosqlite.connect(DB_PATH) as db:
            await db.execute('INSERT INTO tickets_subasta (subasta_id, usuario_id, canal_id) VALUES (?, ?, ?)', (subasta_id, usuario_id, str(canal.id)))
            await db.commit()

    @tasks.loop(minutes=5)
    async def verificar_eventos_task(self):
        async with aiosqlite.connect(DB_PATH) as db:
            cursor = await db.execute('SELECT * FROM eventos_programados WHERE activo = 1 AND fecha_inicio <= datetime("now") AND fecha_fin >= datetime("now")')
            eventos = await cursor.fetchall()
            for ev in eventos:
                if ev[2] == '2x1':
                    global evento_2x1
                    evento_2x1 = True
                elif ev[2] == 'cashback_doble':
                    global evento_cashback_doble
                    evento_cashback_doble = True
                elif ev[2] == 'oferta':
                    global evento_oferta_activa, evento_oferta_porcentaje
                    evento_oferta_activa = True
                    evento_oferta_porcentaje = ev[3]
            cursor = await db.execute('SELECT * FROM eventos_programados WHERE activo = 1 AND fecha_fin < datetime("now")')
            vencidos = await cursor.fetchall()
            for ev in vencidos:
                await db.execute('UPDATE eventos_programados SET activo = 0 WHERE id = ?', (ev[0],))
                if ev[2] == '2x1':
                    evento_2x1 = False
                elif ev[2] == 'cashback_doble':
                    evento_cashback_doble = False
                elif ev[2] == 'oferta':
                    evento_oferta_activa = False
                    evento_oferta_porcentaje = 0
            await db.commit()

    @tasks.loop(hours=24)
    async def backup_automatico_task(self):
        fecha = datetime.now().strftime("%Y%m%d_%H%M%S")
        backup_file = f"backups/backup_{fecha}.db"
        shutil.copy2(DB_PATH, backup_file)
        backups = sorted(os.listdir('backups'))
        while len(backups) > 30:
            os.remove(os.path.join('backups', backups.pop(0)))
        await self.enviar_log_sistema("💾 BACKUP AUTOMÁTICO", f"Backup creado: {backup_file}")

    @tasks.loop(hours=168)
    async def reset_misiones_semanales_task(self):
        semana = datetime.now().isocalendar()[1]
        año = datetime.now().year
        async with aiosqlite.connect(DB_PATH) as db:
            await db.execute('DELETE FROM misiones_semanales_progreso')
            await db.execute('UPDATE misiones_semanales SET semana = ?, año = ?', (semana, año))
            await db.commit()
        await self.enviar_log_sistema("📋 MISIONES SEMANALES", "Misiones semanales reiniciadas")

    async def enviar_notificacion(self, usuario_id, tipo, titulo, mensaje):
        async with aiosqlite.connect(DB_PATH) as db:
            cursor = await db.execute('SELECT * FROM notificaciones_preferencias WHERE usuario_id = ?', (usuario_id,))
            pref = await cursor.fetchone()
            if pref:
                hora = datetime.now().hour
                if hora < pref[8] or hora > pref[9]:
                    return
                if tipo == 'logro' and not pref[3]:
                    return
                if tipo == 'subasta' and not pref[2]:
                    return
                if tipo == 'cashback' and not pref[4]:
                    return
            await db.execute('INSERT INTO notificaciones_historial (usuario_id, tipo, titulo, mensaje) VALUES (?, ?, ?, ?)', (usuario_id, tipo, titulo, mensaje))
            await db.commit()
        try:
            user = await self.fetch_user(int(usuario_id))
            embed = discord.Embed(title=titulo, description=mensaje, color=COLORS['info'])
            await user.send(embed=embed)
        except:
            pass

    async def verificar_logros(self, usuario_id, nick, tipo, valor=1):
        async with aiosqlite.connect(DB_PATH) as db:
            cursor = await db.execute('SELECT * FROM usuarios_stats WHERE usuario_id = ?', (usuario_id,))
            stats = await cursor.fetchone()
            if not stats:
                await db.execute('INSERT INTO usuarios_stats (usuario_id) VALUES (?)', (usuario_id,))
                stats = (usuario_id, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, datetime.now())
            if tipo == 'compra':
                nc = stats[1] + valor
                ng = stats[2] + valor
                await db.execute('UPDATE usuarios_stats SET total_compras = ?, total_gastado = ? WHERE usuario_id = ?', (nc, ng, usuario_id))
                stats = list(stats); stats[1]=nc; stats[2]=ng
            elif tipo == 'caja':
                nc = stats[3] + valor
                await db.execute('UPDATE usuarios_stats SET total_cajas_abiertas = ? WHERE usuario_id = ?', (nc, usuario_id))
                stats[3] = nc
            elif tipo == 'referido':
                nr = stats[4] + valor
                await db.execute('UPDATE usuarios_stats SET total_referidos = ? WHERE usuario_id = ?', (nr, usuario_id))
                stats[4] = nr
            elif tipo == 'subasta':
                ns = stats[6] + valor
                await db.execute('UPDATE usuarios_stats SET total_subastas_ganadas = ? WHERE usuario_id = ?', (ns, usuario_id))
                stats[6] = ns
            cursor = await db.execute('SELECT * FROM logros WHERE activo = 1')
            logros = await cursor.fetchall()
            for l in logros:
                cur2 = await db.execute('SELECT * FROM usuarios_logros WHERE usuario_id = ? AND logro_id = ?', (usuario_id, l[0]))
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
                    cur3 = await db.execute('SELECT mejor_racha FROM rachas WHERE usuario_id = ?', (usuario_id,))
                    rr = await cur3.fetchone()
                    if rr and rr[0] >= l[7]:
                        cumplido = True
                if cumplido:
                    await db.execute('INSERT INTO usuarios_logros (usuario_id, logro_id, fecha_desbloqueo) VALUES (?, ?, ?)', (usuario_id, l[0], datetime.now()))
                    if l[8] > 0:
                        await db.execute('UPDATE usuarios_balance SET balance = balance + ? WHERE discord_id = ?', (l[8], usuario_id))
                    if l[9]:
                        guild = self.get_guild(config.GUILD_ID)
                        if guild:
                            member = guild.get_member(int(usuario_id))
                            rol = guild.get_role(int(l[9]))
                            if member and rol and rol not in member.roles:
                                await member.add_roles(rol)
                    await self.enviar_notificacion(usuario_id, 'logro', f"🏆 Logro desbloqueado: {l[1]}", f"Has desbloqueado {l[3]} **{l[1]}**\nRecompensa: ${l[8]:,} VP$")
            await db.commit()

    async def on_ready(self):
        logger.info(f"✅ Bot conectado como {self.user}")
        logger.info(f"🌐 En {len(self.guilds)} servidores")
        if self.volumen_montado:
            logger.info("💾 Volumen persistente activo")
        global reconnect_attempts
        reconnect_attempts = 0
        self.reconnecting = False
        await self.enviar_log_sistema("🟢 **BOT INICIADO**", f"VP Rifas Bot v{VERSION}\nServidores: {len(self.guilds)}\nVolumen: {'✅' if self.volumen_montado else '❌'}")

    async def on_disconnect(self):
        logger.warning("⚠️ Bot desconectado")
        self.reconnecting = True

    async def on_resumed(self):
        logger.info("🔄 Bot reconectado")
        self.reconnecting = False

    async def enviar_log_sistema(self, titulo, descripcion):
        try:
            canal = self.get_channel(self.update_channel_id)
            if not canal:
                return
            embed = discord.Embed(title=titulo, description=descripcion, color=0x0099FF, timestamp=datetime.now())
            embed.set_footer(text=f"VP Rifas v{VERSION}")
            await canal.send(embed=embed)
        except:
            pass

    @tasks.loop(seconds=30)
    async def keep_alive_task(self):
        try:
            activities = [
                f"{PREFIX}ayuda | {len(self.guilds)} servidores",
                f"Rifas VP v{VERSION}",
                f"{PREFIX}comprarrandom | Activo",
                f"{len(self.users)} usuarios"
            ]
            activity = random.choice(activities)
            await self.change_presence(activity=discord.Activity(type=discord.ActivityType.watching, name=activity))
        except:
            pass

    @tasks.loop(minutes=60)
    async def status_task(self):
        uptime = datetime.now() - start_time
        horas = uptime.total_seconds() // 3600
        minutos = (uptime.total_seconds() % 3600) // 60
        await self.enviar_log_sistema("💓 **HEARTBEAT**", f"Activo por {int(horas)}h {int(minutos)}m\nVersión: {VERSION}")

bot = VPRifasBot()

# ============================================
# FUNCIONES AUXILIARES GLOBALES (fuera de la clase)
# ============================================

async def get_db():
    conn = await aiosqlite.connect(DB_PATH)
    conn.row_factory = aiosqlite.Row
    return conn

async def enviar_log(ctx, accion, detalles):
    try:
        canal_logs = bot.get_channel(1482849207290429461)
        if not canal_logs:
            return
        embed = discord.Embed(title=f"📋 {accion}", description=detalles, color=0x0099FF, timestamp=datetime.now())
        embed.add_field(name="👤 Usuario", value=ctx.author.name, inline=True)
        embed.add_field(name="📌 Canal", value=ctx.channel.name, inline=True)
        await canal_logs.send(embed=embed)
    except:
        pass

async def enviar_dm(usuario_id, titulo, mensaje):
    try:
        usuario = await bot.fetch_user(int(usuario_id))
        embed = discord.Embed(title=titulo, description=mensaje, color=0x0099FF)
        await usuario.send(embed=embed)
    except:
        pass

async def verificar_canal(ctx, categoria_id=None):
    if not ctx.guild:
        await ctx.send(embed=embeds.crear_embed_error("❌ Solo en servidores"))
        return False
    cat = categoria_id if categoria_id else CATEGORIA_RIFAS
    if ctx.channel.category_id != cat:
        await ctx.send(embed=embeds.crear_embed_error("❌ Comando no disponible aquí"))
        return False
    return True

def tiene_rol(miembro, role_id):
    return any(role.id == role_id for role in miembro.roles)

async def check_admin(ctx):
    if not ctx.guild:
        return False
    member = ctx.guild.get_member(ctx.author.id)
    return member and (tiene_rol(member, ROLES['CEO']) or tiene_rol(member, ROLES['DIRECTOR']))

async def check_vendedor(ctx):
    if not ctx.guild:
        return False
    member = ctx.guild.get_member(ctx.author.id)
    return member and (tiene_rol(member, ROLES['CEO']) or tiene_rol(member, ROLES['DIRECTOR']) or tiene_rol(member, ROLES['RIFAS']))

async def check_ceo(ctx):
    if not ctx.guild:
        return False
    member = ctx.guild.get_member(ctx.author.id)
    return member and tiene_rol(member, ROLES['CEO'])

async def check_distribuidor(ctx, nivel_minimo=1):
    if not ctx.guild:
        return False
    member = ctx.guild.get_member(ctx.author.id)
    if not member:
        return False
    for rol_id in ROLES_DISTRIBUIDORES.values():
        if tiene_rol(member, rol_id):
            async with aiosqlite.connect(DB_PATH) as db:
                cursor = await db.execute('SELECT nivel FROM distribuidores WHERE discord_id = ?', (str(ctx.author.id),))
                result = await cursor.fetchone()
                if result and result[0] >= nivel_minimo:
                    return True
    return tiene_rol(member, ROLES['CEO']) or tiene_rol(member, ROLES['DIRECTOR'])

async def check_franquicia(ctx, nivel=None):
    if not ctx.guild:
        return False
    member = ctx.guild.get_member(ctx.author.id)
    if not member:
        return False
    if nivel:
        rol_id = ROLES_FRANQUICIA[nivel]['rol_id']
        return tiene_rol(member, rol_id) or tiene_rol(member, ROLES['CEO']) or tiene_rol(member, ROLES['DIRECTOR'])
    for nivel_data in ROLES_FRANQUICIA.values():
        if tiene_rol(member, nivel_data['rol_id']):
            return True
    return tiene_rol(member, ROLES['CEO']) or tiene_rol(member, ROLES['DIRECTOR'])

async def generar_codigo_unico(usuario_id):
    return f"VP-{hashlib.md5(usuario_id.encode()).hexdigest()[:8].upper()}"

async def obtener_o_crear_codigo(usuario_id, usuario_nombre):
    async with aiosqlite.connect(DB_PATH) as db:
        cursor = await db.execute('SELECT codigo FROM referidos_codigos WHERE usuario_id = ?', (usuario_id,))
        r = await cursor.fetchone()
        if r:
            return r[0]
        codigo = await generar_codigo_unico(usuario_id)
        await db.execute('INSERT INTO referidos_codigos (usuario_id, codigo) VALUES (?, ?)', (usuario_id, codigo))
        await db.commit()
        return codigo

async def obtener_nivel_por_gasto(gasto_total):
    async with aiosqlite.connect(DB_PATH) as db:
        cursor = await db.execute('''
            SELECT nivel FROM fidelizacion_config
            WHERE gasto_minimo <= ? AND (gasto_maximo >= ? OR gasto_maximo IS NULL)
            ORDER BY gasto_minimo DESC LIMIT 1
        ''', (gasto_total, gasto_total))
        r = await cursor.fetchone()
        return r[0] if r else 'BRONCE'

async def actualizar_fidelizacion(usuario_id, monto_compra):
    async with aiosqlite.connect(DB_PATH) as db:
        cursor = await db.execute('SELECT gasto_total FROM fidelizacion WHERE usuario_id = ?', (usuario_id,))
        r = await cursor.fetchone()
        nuevo = (r[0] + monto_compra) if r else monto_compra
        await db.execute('INSERT OR REPLACE INTO fidelizacion (usuario_id, gasto_total) VALUES (?, ?)', (usuario_id, nuevo))
        nivel = await obtener_nivel_por_gasto(nuevo)
        await db.execute('UPDATE fidelizacion SET nivel = ? WHERE usuario_id = ?', (nivel, usuario_id))
        await db.commit()
        return nivel

async def aplicar_cashback(usuario_id, monto_compra):
    global evento_cashback_doble, CASHBACK_PORCENTAJE
    porcentaje = CASHBACK_PORCENTAJE
    if evento_cashback_doble:
        porcentaje *= 2
    cb = int(monto_compra * porcentaje / 100)
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute('INSERT INTO cashback (usuario_id, cashback_acumulado) VALUES (?, ?) ON CONFLICT(usuario_id) DO UPDATE SET cashback_acumulado = cashback_acumulado + ?', (usuario_id, cb, cb))
        await db.commit()
        return cb

async def obtener_descuento_usuario(usuario_id):
    global evento_oferta_activa, evento_oferta_porcentaje
    async with aiosqlite.connect(DB_PATH) as db:
        cursor = await db.execute('SELECT nivel FROM fidelizacion WHERE usuario_id = ?', (usuario_id,))
        r = await cursor.fetchone()
        if r:
            cursor2 = await db.execute('SELECT descuento FROM fidelizacion_config WHERE nivel = ?', (r[0],))
            base = (await cursor2.fetchone() or [0])[0]
        else:
            base = 0
        if evento_oferta_activa:
            base += evento_oferta_porcentaje
        return min(base, 50)

async def procesar_comision_referido(comprador_id, monto_compra):
    global REFERIDOS_PORCENTAJE
    async with aiosqlite.connect(DB_PATH) as db:
        cursor = await db.execute('SELECT referidor_id FROM referidos_relaciones WHERE referido_id = ?', (comprador_id,))
        ref = await cursor.fetchone()
        if ref:
            comision = int(monto_compra * REFERIDOS_PORCENTAJE / 100)
            await db.execute('UPDATE usuarios_balance SET balance = balance + ? WHERE discord_id = ?', (comision, ref[0]))
            await db.execute('UPDATE referidos_relaciones SET comisiones_generadas = comisiones_generadas + ? WHERE referido_id = ?', (comision, comprador_id))
            await db.commit()

async def procesar_comision_vendedor(vendedor_id, monto_compra):
    global COMISION_VENDEDOR
    comision = int(monto_compra * COMISION_VENDEDOR / 100)
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute('INSERT INTO vendedores (discord_id, nombre, comisiones_pendientes) VALUES (?, ?, ?) ON CONFLICT(discord_id) DO UPDATE SET comisiones_pendientes = comisiones_pendientes + ?', (vendedor_id, vendedor_id, comision, comision))
        await db.commit()

async def agregar_puntos_revancha(usuario_id, boletos_perdidos):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute('INSERT INTO puntos_revancha (usuario_id, puntos) VALUES (?, ?) ON CONFLICT(usuario_id) DO UPDATE SET puntos = puntos + ?', (usuario_id, boletos_perdidos, boletos_perdidos))
        await db.commit()

async def actualizar_jackpot(monto_compra):
    global jackpot_activo, jackpot_total, jackpot_rifa_id, jackpot_porcentaje
    if not jackpot_activo:
        return
    rifa = await bot.db.get_rifa_activa()
    if not rifa or rifa['id'] != jackpot_rifa_id:
        return
    aporte = int(monto_compra * jackpot_porcentaje / 100)
    jackpot_total += aporte
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute('UPDATE jackpot SET total = ? WHERE id = 1', (jackpot_total,))
        await db.commit()

async def actualizar_ranking_rifa(rifa_id, usuario_id, cantidad):
    global ranking_rifa
    if rifa_id not in ranking_rifa:
        ranking_rifa[rifa_id] = {}
    ranking_rifa[rifa_id][usuario_id] = ranking_rifa[rifa_id].get(usuario_id, 0) + cantidad

async def reiniciar_ranking_rifa(rifa_id):
    global ranking_rifa
    if rifa_id in ranking_rifa:
        ranking_rifa[rifa_id] = {}

async def registrar_ganador_historico(usuario_id, usuario_nick, premio, rifa_nombre, numero):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute('INSERT INTO ganadores_historicos (usuario_id, usuario_nick, premio, rifa_nombre, numero) VALUES (?, ?, ?, ?, ?)', (usuario_id, usuario_nick, premio, rifa_nombre, numero))
        await db.commit()

async def es_numero_bloqueado(rifa_id, numero):
    async with aiosqlite.connect(DB_PATH) as db:
        cursor = await db.execute('SELECT numeros_bloqueados FROM rifas WHERE id = ?', (rifa_id,))
        r = await cursor.fetchone()
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

# ============================================
# VISTA PARA MENÚ DE AYUDA (SELECT MENU)
# ============================================

class AyudaView(View):
    def __init__(self, user_id, es_ceo, es_admin, es_vendedor, es_distribuidor, es_franquicia):
        super().__init__(timeout=60)
        self.user_id = user_id
        self.es_ceo = es_ceo
        self.es_admin = es_admin
        self.es_vendedor = es_vendedor
        self.es_distribuidor = es_distribuidor
        self.es_franquicia = es_franquicia

    @discord.ui.select(placeholder="📋 Selecciona una categoría...", options=[
        discord.SelectOption(label="👤 Básicos", value="basicos", emoji="👤", description="Comandos para todos"),
        discord.SelectOption(label="🤝 Referidos", value="referidos", emoji="🤝"),
        discord.SelectOption(label="🏆 Fidelización", value="fidelizacion", emoji="🏆"),
        discord.SelectOption(label="📦 Cajas", value="cajas", emoji="📦"),
        discord.SelectOption(label="🎲 Apuestas", value="apuestas", emoji="🎲"),
        discord.SelectOption(label="🎡 Ruleta", value="ruleta", emoji="🎡"),
        discord.SelectOption(label="🏦 Banco", value="banco", emoji="🏦"),
        discord.SelectOption(label="🎫 Subastas", value="subastas", emoji="🎫"),
        discord.SelectOption(label="🛒 Marketplace", value="marketplace", emoji="🛒"),
        discord.SelectOption(label="🎁 Regalos", value="regalos", emoji="🎁"),
        discord.SelectOption(label="📋 Misiones", value="misiones", emoji="📋"),
        discord.SelectOption(label="💰 Vendedores", value="vendedores", emoji="💰"),
        discord.SelectOption(label="📦 Distribuidores", value="distribuidores", emoji="📦"),
        discord.SelectOption(label="👑 Franquicias", value="franquicias", emoji="👑"),
        discord.SelectOption(label="🎯 Director", value="director", emoji="🎯"),
        discord.SelectOption(label="👑 CEO", value="ceo", emoji="👑"),
    ])
    async def select_callback(self, interaction: discord.Interaction, select: Select):
        if interaction.user.id != self.user_id:
            return await interaction.response.send_message("❌ No puedes usar este menú", ephemeral=True)
        categoria = select.values[0]
        embed = self._get_embed(categoria)
        await interaction.response.edit_message(embed=embed, view=self)

    def _get_embed(self, cat):
        e = discord.Embed(title="📚 AYUDA - " + cat.upper(), color=COLORS['primary'])
        if cat == "basicos":
            e.description = "```\n!rifa\n!comprarrandom [cantidad]\n!misboletos\n!balance [@user]\n!topvp\n!ranking\n!historial\n!celiminacion [num]\n!beliminacion\n!mispuntos\n```"
        elif cat == "referidos":
            e.description = "```\n!codigo\n!usar [codigo]\n!misreferidos\n```"
        elif cat == "fidelizacion":
            e.description = "```\n!nivel\n!topgastadores\n!cashback\n!topcashback\n!verniveles\n```"
        elif cat == "cajas":
            e.description = "```\n!cajas\n!comprarcaja [tipo] [cant]\n!miscajas\n!abrircaja [id]\n!topcajas\n```"
        elif cat == "apuestas":
            e.description = "```\n!apostar [num] [monto]\n!mis_apuestas\n```"
        elif cat == "ruleta":
            e.description = "```\n!ruleta\n!ruleta_stats\n```"
        elif cat == "banco":
            e.description = "```\n!banco\n!invertir [prod] [monto]\n!misinversiones\n!retirar [id]\n!cambiarng [monto]\n```"
        elif cat == "subastas":
            e.description = "```\n!subastas\n!pujar [id] [monto]\n!mis_pujas\n```"
        elif cat == "marketplace":
            e.description = "```\n!marketplace\n!vender_boleto [num] [precio]\n!comprar_boleto [id]\n!mis_listados\n```"
        elif cat == "regalos":
            e.description = "```\n!regalar [@user] [monto]\n!solicitudes\n!aceptar [id]\n!rechazar [id]\n```"
        elif cat == "misiones":
            e.description = "```\n!misiones\n!misiones_semanales\n!miracha\n!reclamar [id]\n```"
        elif cat == "vendedores" and (self.es_vendedor or self.es_admin or self.es_ceo):
            e.description = "```\n!vender [@user] [num]\n!venderrandom [@user] [cant]\n!misventas\n!listaboletos\n```"
        elif cat == "distribuidores" and (self.es_distribuidor or self.es_admin or self.es_ceo):
            e.description = "```\n!distribuidor\n!productos\n!comprar_producto [nombre] [cant]\n!mis_productos\n```"
        elif cat == "franquicias" and (self.es_franquicia or self.es_admin or self.es_ceo):
            e.description = "```\n!franquicia\n!franquicia_rifa [premio] [precio] [total]\n!franquicia_stats\n```"
        elif cat == "director" and (self.es_admin or self.es_ceo):
            e.description = "```\n!crearifa [premio] [precio] [total] [bloq]\n!cerrarifa\n!iniciarsorteo [num]\n!finalizarrifa [id] [num]\n!vendedoradd [@user] [%]\n!vercomisiones\n!pagarcomisiones\n!verboletos [@user]\n!subasta crear [item] [base] [horas]\n!rifaeliminacion [total] [premio] [valor]\n!rifaeliminacionr [num]\n```"
        elif cat == "ceo" and self.es_ceo:
            e.description = "```\n!acreditarvp [@user] [monto]\n!retirarvp [@user] [monto]\n!procesarvp [@user]\n!procesadovp [@user] [vp]\n!setrefcomision [%]\n!setcashback [%]\n!pagarcashback\n!evento 2x1/cashbackdoble/oferta\n!config get/set/list\n!backup\n!resetallsistema\n!version\n!caja crear/editar\n!mision crear/editar\n!ruleta_config set\n!apuestas_config set\n!ticketcerrar\n!verboletos [@user]\n!crearcodigo [codigo] [vp]\n!borrarcodigo [codigo]\n!jackpot [base] [%] [id]\n!jackpotreset\n!jackpotsortear [num]\n```"
        else:
            e.description = "❌ No tienes permiso para ver esta categoría."
        return e

# ============================================
# COMANDO DE AYUDA PRINCIPAL
# ============================================

@bot.command(name="ayuda")
async def cmd_ayuda(ctx):
    if not await verificar_canal(ctx):
        return
    member = ctx.guild.get_member(ctx.author.id)
    es_ceo = await check_ceo(ctx)
    es_admin = await check_admin(ctx)
    es_vendedor = await check_vendedor(ctx)
    es_distribuidor = await check_distribuidor(ctx)
    es_franquicia = await check_franquicia(ctx)
    embed_inicial = discord.Embed(
        title="🎟️ **SISTEMA DE RIFAS VP**",
        description=f"Prefijo: `{PREFIX}` | Versión: {VERSION}\n\n📌 **Selecciona una categoría en el menú desplegable**",
        color=COLORS['primary']
    )
    embed_inicial.set_footer(text="Los comandos de staff requieren roles específicos")
    view = AyudaView(ctx.author.id, es_ceo, es_admin, es_vendedor, es_distribuidor, es_franquicia)
    await ctx.send(embed=embed_inicial, view=view)

# ============================================
# COMANDOS BÁSICOS (se incluyen todos)
# ============================================

@bot.command(name="version")
async def cmd_version(ctx):
    uptime = datetime.now() - start_time
    horas = uptime.total_seconds() // 3600
    minutos = (uptime.total_seconds() % 3600) // 60
    embed = discord.Embed(
        title="🤖 **VP RIFAS BOT**",
        description=f"**Versión:** `{VERSION}`\n**Estado:** 🟢 Activo\n**Uptime:** {int(horas)}h {int(minutos)}m\n**Servidores:** {len(bot.guilds)}\n**Volumen:** {'✅ Activo' if bot.volumen_montado else '❌ No detectado'}",
        color=COLORS['primary']
    )
    await ctx.send(embed=embed)

@bot.command(name="rifa")
async def cmd_rifa(ctx):
    if not await verificar_canal(ctx):
        return
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        cursor = await db.execute('SELECT * FROM rifas WHERE estado = "activa" ORDER BY id DESC LIMIT 1')
        rifa = await cursor.fetchone()
    if not rifa:
        await ctx.send(embed=embeds.crear_embed_error("No hay rifa activa"))
        return
    embed = discord.Embed(
        title=f"🎟️ {rifa['nombre']}",
        description=f"**{rifa['premio']}**",
        color=COLORS['primary']
    )
    embed.add_field(name="🏆 Premio", value=f"${rifa['valor_premio']:,}", inline=True)
    embed.add_field(name="💰 Precio", value=f"${rifa['precio_boleto']:,}", inline=True)
    if rifa['numeros_bloqueados']:
        embed.add_field(name="🔒 Números VIP", value="Solo disponibles con staff", inline=False)
    embed.set_footer(text="Usa !comprarrandom para participar")
    await ctx.send(embed=embed)

@bot.command(name="comprarrandom")
async def cmd_comprar_random(ctx, cantidad: int = 1):
    if not await verificar_canal(ctx):
        return
    if cantidad < 1 or cantidad > 50:
        await ctx.send(embed=embeds.crear_embed_error("Cantidad entre 1 y 50"))
        return
    rifa_activa = await bot.db.get_rifa_activa()
    if not rifa_activa:
        await ctx.send(embed=embeds.crear_embed_error("No hay rifa activa"))
        return
    disponibles = await bot.db.get_boletos_disponibles(rifa_activa['id'])
    disponibles_filtrados = [n for n in disponibles if not await es_numero_bloqueado(rifa_activa['id'], n)]
    if len(disponibles_filtrados) < cantidad:
        await ctx.send(embed=embeds.crear_embed_error(f"Solo hay {len(disponibles_filtrados)} boletos disponibles para compra normal"))
        return
    precio_boleto = rifa_activa['precio_boleto']
    descuento = await obtener_descuento_usuario(str(ctx.author.id))
    global evento_2x1
    boletos_a_pagar = cantidad
    boletos_a_recibir = cantidad
    if evento_2x1:
        boletos_a_pagar = cantidad // 2 + (cantidad % 2)
        boletos_a_recibir = cantidad
    precio_total = precio_boleto * boletos_a_pagar
    precio_con_descuento = int(precio_total * (100 - descuento) / 100)
    async with aiosqlite.connect(DB_PATH) as db:
        cursor = await db.execute('SELECT balance FROM usuarios_balance WHERE discord_id = ?', (str(ctx.author.id),))
        result = await cursor.fetchone()
        balance = result[0] if result else 0
    if balance < precio_con_descuento:
        await ctx.send(embed=embeds.crear_embed_error(f"Necesitas {precio_con_descuento} VP$"))
        return
    seleccionados = random.sample(disponibles_filtrados, boletos_a_recibir)
    comprados = []
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute('UPDATE usuarios_balance SET balance = balance - ? WHERE discord_id = ?', (precio_con_descuento, str(ctx.author.id)))
        for num in seleccionados:
            await db.execute('INSERT INTO boletos (rifa_id, numero, comprador_id, comprador_nick, precio_pagado) VALUES (?, ?, ?, ?, ?)', (rifa_activa['id'], num, str(ctx.author.id), ctx.author.name, precio_boleto))
            comprados.append(num)
        await db.commit()
    await actualizar_fidelizacion(str(ctx.author.id), precio_con_descuento)
    cashback = await aplicar_cashback(str(ctx.author.id), precio_con_descuento)
    await procesar_comision_referido(str(ctx.author.id), precio_con_descuento)
    await actualizar_jackpot(precio_con_descuento)
    await actualizar_ranking_rifa(rifa_activa['id'], str(ctx.author.id), len(comprados))
    await bot.verificar_logros(str(ctx.author.id), ctx.author.name, 'compra', len(comprados))
    await enviar_dm(str(ctx.author.id), "✅ Compra realizada", 
                    f"Has comprado {len(comprados)} boletos: {', '.join(map(str, comprados))}\nTotal: ${precio_con_descuento}\nDescuento: {descuento}%\nCashback acumulado: ${cashback}")
    await ctx.message.delete()
    await ctx.send(embed=embeds.crear_embed_exito(f"✅ Compra realizada! Revisa tu DM."))

@bot.command(name="misboletos")
async def cmd_mis_boletos(ctx):
    if not await verificar_canal(ctx):
        return
    rifa_activa = await bot.db.get_rifa_activa()
    if not rifa_activa:
        await ctx.send(embed=embeds.crear_embed_error("No hay rifa activa"))
        return
    async with aiosqlite.connect(DB_PATH) as db:
        cursor = await db.execute('SELECT numero FROM boletos WHERE rifa_id = ? AND comprador_id = ?', (rifa_activa['id'], str(ctx.author.id)))
        boletos = await cursor.fetchall()
    if not boletos:
        await ctx.send(embed=embeds.crear_embed_error("No tienes boletos"))
        return
    numeros = [str(b[0]) for b in boletos]
    embed = discord.Embed(title="🎟️ Tus boletos", description=f"Números: {', '.join(numeros)}", color=COLORS['primary'])
    await ctx.send(embed=embed)

@bot.command(name="balance")
async def cmd_balance(ctx, usuario: discord.Member = None):
    if not await verificar_canal(ctx):
        return
    target = usuario if usuario else ctx.author
    if usuario and not await check_admin(ctx):
        await ctx.send(embed=embeds.crear_embed_error("No tienes permiso para ver el balance de otros"))
        return
    async with aiosqlite.connect(DB_PATH) as db:
        cursor = await db.execute('SELECT balance FROM usuarios_balance WHERE discord_id = ?', (str(target.id),))
        result = await cursor.fetchone()
        balance = result[0] if result else 0
    embed = discord.Embed(title=f"💰 Balance de {target.name}", description=f"**{balance:,} VP$**", color=COLORS['primary'])
    await ctx.send(embed=embed)

@bot.command(name="topvp")
async def cmd_top_vp(ctx):
    if not await verificar_canal(ctx):
        return
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        cursor = await db.execute('SELECT nombre, balance FROM usuarios_balance WHERE balance > 0 ORDER BY balance DESC LIMIT 10')
        usuarios = await cursor.fetchall()
    if not usuarios:
        await ctx.send(embed=embeds.crear_embed_info("Sin datos", "No hay usuarios con VP$"))
        return
    embed = discord.Embed(title="🏆 TOP 10 VP$", color=COLORS['primary'])
    for i, u in enumerate(usuarios, 1):
        medalla = {1: "🥇", 2: "🥈", 3: "🥉"}.get(i, f"{i}.")
        embed.add_field(name=f"{medalla} {u['nombre']}", value=f"**{u['balance']} VP$**", inline=False)
    await ctx.send(embed=embed)

@bot.command(name="ranking")
async def cmd_ranking(ctx):
    if not await verificar_canal(ctx):
        return
    rifa_activa = await bot.db.get_rifa_activa()
    if not rifa_activa:
        await ctx.send(embed=embeds.crear_embed_error("No hay rifa activa"))
        return
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        cursor = await db.execute('SELECT comprador_nick, COUNT(*) as boletos FROM boletos WHERE rifa_id = ? GROUP BY comprador_id ORDER BY boletos DESC LIMIT 10', (rifa_activa['id'],))
        ranking = await cursor.fetchall()
    if not ranking:
        await ctx.send(embed=embeds.crear_embed_info("Sin datos", "Aún no hay compras en esta rifa"))
        return
    embed = discord.Embed(title="🏆 TOP COMPRADORES", color=COLORS['primary'])
    for i, u in enumerate(ranking, 1):
        medalla = {1: "🥇", 2: "🥈", 3: "🥉"}.get(i, f"{i}.")
        embed.add_field(name=f"{medalla} {u['comprador_nick']}", value=f"{u['boletos']} boletos", inline=False)
    await ctx.send(embed=embed)

@bot.command(name="historial")
async def cmd_historial(ctx):
    if not await verificar_canal(ctx):
        return
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        cursor = await db.execute('SELECT b.numero, r.nombre as rifa, b.fecha_compra, b.precio_pagado FROM boletos b JOIN rifas r ON b.rifa_id = r.id WHERE b.comprador_id = ? ORDER BY b.fecha_compra DESC LIMIT 20', (str(ctx.author.id),))
        boletos = await cursor.fetchall()
    if not boletos:
        await ctx.send(embed=embeds.crear_embed_error("Sin historial"))
        return
    embed = discord.Embed(title="📜 Tu historial", color=COLORS['primary'])
    for b in boletos[:10]:
        embed.add_field(name=f"{b['rifa']} - #{b['numero']}", value=f"${b['precio_pagado']} - {b['fecha_compra'][:10]}", inline=False)
    await ctx.send(embed=embed)

# ============================================
# SISTEMA DE REFERIDOS
# ============================================

@bot.command(name="codigo")
async def cmd_codigo(ctx):
    if not await verificar_canal(ctx):
        return
    codigo = await obtener_o_crear_codigo(str(ctx.author.id), ctx.author.name)
    embed = discord.Embed(title="🔗 Tu código de referido", description=f"`{codigo}`", color=COLORS['primary'])
    async with aiosqlite.connect(DB_PATH) as db:
        cursor = await db.execute('SELECT COUNT(*), SUM(comisiones_generadas) FROM referidos_relaciones WHERE referidor_id = ?', (str(ctx.author.id),))
        stats = await cursor.fetchone()
        total_referidos = stats[0] if stats else 0
        total_comisiones = stats[1] if stats and stats[1] else 0
    embed.add_field(name="📊 Referidos", value=f"**{total_referidos}**", inline=True)
    embed.add_field(name="💰 Comisiones", value=f"**{total_comisiones} VP$**", inline=True)
    await ctx.send(embed=embed)

@bot.command(name="usar")
async def cmd_usar_codigo(ctx, codigo: str):
    if not await verificar_canal(ctx):
        return
    async with aiosqlite.connect(DB_PATH) as db:
        cursor = await db.execute('SELECT * FROM referidos_relaciones WHERE referido_id = ?', (str(ctx.author.id),))
        if await cursor.fetchone():
            await ctx.send(embed=embeds.crear_embed_error("Ya tienes un referidor"))
            return
        cursor = await db.execute('SELECT usuario_id FROM referidos_codigos WHERE codigo = ? AND usuario_id != ?', (codigo.upper(), str(ctx.author.id)))
        referidor = await cursor.fetchone()
        if not referidor:
            await ctx.send(embed=embeds.crear_embed_error("Código inválido"))
            return
        await db.execute('INSERT INTO referidos_relaciones (referido_id, referidor_id) VALUES (?, ?)', (str(ctx.author.id), referidor[0]))
        await db.commit()
    await bot.verificar_logros(str(referidor[0]), None, 'referido', 1)
    await ctx.send(embed=embeds.crear_embed_exito("Código aplicado correctamente"))

@bot.command(name="misreferidos")
async def cmd_mis_referidos(ctx):
    if not await verificar_canal(ctx):
        return
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        cursor = await db.execute('SELECT r.referido_id, c.nombre, r.total_compras, r.comisiones_generadas FROM referidos_relaciones r LEFT JOIN clientes c ON r.referido_id = c.discord_id WHERE r.referidor_id = ? ORDER BY r.fecha_registro DESC LIMIT 20', (str(ctx.author.id),))
        referidos = await cursor.fetchall()
    if not referidos:
        await ctx.send(embed=embeds.crear_embed_error("No tienes referidos"))
        return
    embed = discord.Embed(title="👥 Tus referidos", color=COLORS['primary'])
    for ref in referidos[:10]:
        nombre = ref['nombre'] or "Usuario"
        embed.add_field(name=f"👤 {nombre}", value=f"Compras: {ref['total_compras']} | Comisiones: ${ref['comisiones_generadas']}", inline=False)
    await ctx.send(embed=embed)

# ============================================
# COMANDOS DE FIDELIZACIÓN Y CASHBACK
# ============================================

@bot.command(name="nivel")
async def cmd_nivel(ctx):
    if not await verificar_canal(ctx):
        return
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        cursor = await db.execute('SELECT gasto_total, nivel FROM fidelizacion WHERE usuario_id = ?', (str(ctx.author.id),))
        data = await cursor.fetchone()
        if not data:
            await ctx.send(embed=embeds.crear_embed_info("Sin compras", "Aún no tienes historial"))
            return
        cursor = await db.execute('SELECT * FROM fidelizacion_config WHERE nivel = ?', (data['nivel'],))
        beneficios = await cursor.fetchone()
    embed = discord.Embed(title=f"🏆 Nivel: {data['nivel']}", description=f"Gasto total: **${data['gasto_total']:,} VP$**", color=COLORS['primary'])
    if beneficios:
        beneficio_texto = []
        if beneficios['descuento'] > 0:
            beneficio_texto.append(f"💰 {beneficios['descuento']}% descuento")
        if beneficios['boletos_gratis_por_cada'] > 0:
            beneficio_texto.append(f"🎟️ +{beneficios['cantidad_boletos_gratis']} c/{beneficios['boletos_gratis_por_cada']}")
        if beneficios['acceso_anticipado_horas'] > 0:
            beneficio_texto.append(f"⏰ {beneficios['acceso_anticipado_horas']}h anticipación")
        if beneficios['canal_vip']:
            beneficio_texto.append(f"👑 Canal VIP")
        if beneficios['rifas_exclusivas']:
            beneficio_texto.append(f"✨ Rifas exclusivas")
        if beneficio_texto:
            embed.add_field(name="✅ Beneficios", value="\n".join(beneficio_texto), inline=False)
    await ctx.send(embed=embed)

@bot.command(name="topgastadores")
async def cmd_top_gastadores(ctx):
    if not await verificar_canal(ctx):
        return
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        cursor = await db.execute('SELECT c.nombre, f.gasto_total, f.nivel FROM fidelizacion f LEFT JOIN clientes c ON f.usuario_id = c.discord_id WHERE f.gasto_total > 0 ORDER BY f.gasto_total DESC LIMIT 10')
        top = await cursor.fetchall()
    if not top:
        await ctx.send(embed=embeds.crear_embed_info("Sin datos", "No hay gastadores"))
        return
    embed = discord.Embed(title="🏆 TOP GASTADORES", color=COLORS['primary'])
    for i, u in enumerate(top, 1):
        nombre = u['nombre'] or "Usuario"
        medalla = {1: "🥇", 2: "🥈", 3: "🥉"}.get(i, f"{i}.")
        embed.add_field(name=f"{medalla} {nombre}", value=f"Gastado: ${u['gasto_total']:,} | {u['nivel']}", inline=False)
    await ctx.send(embed=embed)

@bot.command(name="cashback")
async def cmd_cashback(ctx):
    if not await verificar_canal(ctx):
        return
    async with aiosqlite.connect(DB_PATH) as db:
        cursor = await db.execute('SELECT cashback_acumulado FROM cashback WHERE usuario_id = ?', (str(ctx.author.id),))
        result = await cursor.fetchone()
        cashback = result[0] if result else 0
    embed = discord.Embed(title="💰 Cashback", description=f"Acumulado: **${cashback:,} VP$**", color=COLORS['primary'])
    await ctx.send(embed=embed)

@bot.command(name="topcashback")
async def cmd_top_cashback(ctx):
    if not await verificar_canal(ctx):
        return
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        cursor = await db.execute('SELECT c.usuario_id, cl.nombre, c.cashback_acumulado FROM cashback c LEFT JOIN clientes cl ON c.usuario_id = cl.discord_id WHERE c.cashback_acumulado > 0 ORDER BY c.cashback_acumulado DESC LIMIT 10')
        top = await cursor.fetchall()
    if not top:
        await ctx.send(embed=embeds.crear_embed_info("Sin datos", "No hay cashback"))
        return
    embed = discord.Embed(title="💰 TOP CASHBACK", color=COLORS['primary'])
    for i, u in enumerate(top, 1):
        nombre = u['nombre'] or "Usuario"
        embed.add_field(name=f"{i}. {nombre}", value=f"**${u['cashback_acumulado']}**", inline=False)
    await ctx.send(embed=embed)

@bot.command(name="verniveles")
async def cmd_verniveles(ctx):
    if not await verificar_canal(ctx):
        return
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        cursor = await db.execute('SELECT * FROM fidelizacion_config ORDER BY gasto_minimo ASC')
        niveles = await cursor.fetchall()
    embed = discord.Embed(title="📊 **CONFIGURACIÓN DE NIVELES**", color=COLORS['primary'])
    for n in niveles:
        beneficios = []
        if n['descuento'] > 0:
            beneficios.append(f"💰 {n['descuento']}% desc")
        if n['boletos_gratis_por_cada'] > 0:
            beneficios.append(f"🎟️ +{n['cantidad_boletos_gratis']} c/{n['boletos_gratis_por_cada']}")
        if n['acceso_anticipado_horas'] > 0:
            beneficios.append(f"⏰ {n['acceso_anticipado_horas']}h")
        if n['canal_vip']:
            beneficios.append(f"👑 VIP")
        if n['rifas_exclusivas']:
            beneficios.append(f"✨ Excl")
        minimo = str(n['gasto_minimo'])
        maximo = str(n['gasto_maximo']) if n['gasto_maximo'] else '∞'
        rango = f"${minimo} - ${maximo}"
        texto = f"**Rango:** {rango}\n**Beneficios:** {' | '.join(beneficios) if beneficios else 'Ninguno'}"
        embed.add_field(name=f"**{n['nivel']}**", value=texto, inline=False)
    await ctx.send(embed=embed)

# ============================================
# CAJAS MISTERIOSAS (comandos)
# ============================================

@bot.command(name="cajas")
async def cmd_cajas(ctx):
    if not await verificar_canal(ctx):
        return
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        cursor = await db.execute('SELECT * FROM cajas WHERE activo = 1')
        cajas = await cursor.fetchall()
    embed = discord.Embed(title="🎁 **CAJAS MISTERIOSAS**", color=COLORS['primary'])
    for caja in cajas:
        premios = json.loads(caja['premios'])
        probs = json.loads(caja['probabilidades'])
        texto = ""
        for p, prob in zip(premios[:3], probs[:3]):
            texto += f"• {p:,} VP$ ({prob}%)\n"
        if len(premios) > 3:
            texto += f"... y {len(premios)-3} más"
        embed.add_field(name=f"{caja['nombre']} - ${caja['precio']:,} VP$", value=texto, inline=False)
    await ctx.send(embed=embed)

@bot.command(name="comprarcaja")
async def cmd_comprar_caja(ctx, tipo: str, cantidad: int = 1):
    if not await verificar_canal(ctx):
        return
    if cantidad < 1 or cantidad > 50:
        await ctx.send(embed=embeds.crear_embed_error("Cantidad 1-50"))
        return
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        cursor = await db.execute('SELECT * FROM cajas WHERE tipo = ? AND activo = 1', (tipo.lower(),))
        caja = await cursor.fetchone()
        if not caja:
            await ctx.send(embed=embeds.crear_embed_error("Tipo inválido: comun, rara, epica, legendaria, misteriosa"))
            return
        precio_total = caja['precio'] * cantidad
        cursor = await db.execute('SELECT balance FROM usuarios_balance WHERE discord_id = ?', (str(ctx.author.id),))
        result = await cursor.fetchone()
        balance = result[0] if result else 0
        if balance < precio_total:
            await ctx.send(embed=embeds.crear_embed_error(f"Necesitas {precio_total:,} VP$"))
            return
        await db.execute('UPDATE usuarios_balance SET balance = balance - ? WHERE discord_id = ?', (precio_total, str(ctx.author.id)))
        for _ in range(cantidad):
            await db.execute('INSERT INTO cajas_compradas (usuario_id, caja_id) VALUES (?, ?)', (str(ctx.author.id), caja['id']))
        await db.commit()
    embed = discord.Embed(title="✅ Compra realizada", description=f"Compraste {cantidad}x {caja['nombre']} por ${precio_total:,} VP$", color=COLORS['success'])
    embed.add_field(name="📦 Usa", value="`!miscajas` para ver tus cajas", inline=False)
    await ctx.send(embed=embed)

@bot.command(name="miscajas")
async def cmd_mis_cajas(ctx):
    if not await verificar_canal(ctx):
        return
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        cursor = await db.execute('SELECT cc.id, c.nombre, c.tipo FROM cajas_compradas cc JOIN cajas c ON cc.caja_id = c.id WHERE cc.usuario_id = ? AND cc.abierta = 0 ORDER BY cc.fecha_compra ASC', (str(ctx.author.id),))
        cajas = await cursor.fetchall()
    if not cajas:
        await ctx.send(embed=embeds.crear_embed_info("Sin cajas", "No tienes cajas sin abrir"))
        return
    embed = discord.Embed(title="📦 Tus cajas sin abrir", color=COLORS['primary'])
    for caja in cajas[:20]:
        embed.add_field(name=f"ID: {caja['id']} - {caja['nombre']}", value=f"Usa `!abrircaja {caja['id']}` para abrir", inline=False)
    await ctx.send(embed=embed)

@bot.command(name="abrircaja")
async def cmd_abrir_caja(ctx, caja_id: int):
    if not await verificar_canal(ctx):
        return
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        cursor = await db.execute('SELECT cc.*, c.premios, c.probabilidades, c.nombre FROM cajas_compradas cc JOIN cajas c ON cc.caja_id = c.id WHERE cc.id = ? AND cc.usuario_id = ? AND cc.abierta = 0', (caja_id, str(ctx.author.id)))
        caja = await cursor.fetchone()
        if not caja:
            await ctx.send(embed=embeds.crear_embed_error("Caja no encontrada o ya abierta"))
            return
        premios = json.loads(caja['premios'])
        probabilidades = json.loads(caja['probabilidades'])
        elegido = random.choices(premios, weights=probabilidades, k=1)[0]
        await db.execute('UPDATE cajas_compradas SET abierta = 1, premio = ?, fecha_apertura = CURRENT_TIMESTAMP WHERE id = ?', (elegido, caja_id))
        if elegido > 0:
            await db.execute('UPDATE usuarios_balance SET balance = balance + ? WHERE discord_id = ?', (elegido, str(ctx.author.id)))
        await db.execute('INSERT INTO cajas_historial (usuario_id, usuario_nick, caja_nombre, premio_obtenido) VALUES (?, ?, ?, ?)', (str(ctx.author.id), ctx.author.name, caja['nombre'], elegido))
        await db.commit()
    await bot.verificar_logros(str(ctx.author.id), ctx.author.name, 'caja', 1)
    if elegido > 0:
        embed = discord.Embed(title="🎉 CAJA ABIERTA", description=f"Has obtenido **${elegido:,} VP$**", color=COLORS['success'])
    else:
        embed = discord.Embed(title="😢 CAJA ABIERTA", description="No has ganado nada", color=COLORS['error'])
    await ctx.send(embed=embed)

@bot.command(name="topcajas")
async def cmd_top_cajas(ctx):
    if not await verificar_canal(ctx):
        return
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        cursor = await db.execute('SELECT usuario_nick, SUM(premio_obtenido) as total FROM cajas_historial GROUP BY usuario_id ORDER BY total DESC LIMIT 10')
        top = await cursor.fetchall()
    if not top:
        await ctx.send(embed=embeds.crear_embed_info("Sin datos", "No hay ganadores de cajas"))
        return
    embed = discord.Embed(title="🏆 TOP GANADORES DE CAJAS", color=COLORS['primary'])
    for i, u in enumerate(top, 1):
        medalla = {1: "🥇", 2: "🥈", 3: "🥉"}.get(i, f"{i}.")
        embed.add_field(name=f"{medalla} {u['usuario_nick']}", value=f"**${u['total']:,} VP$**", inline=False)
    await ctx.send(embed=embed)

# ============================================
# BANCO VP E INVERSIONES (comandos)
# ============================================

@bot.command(name="banco")
async def cmd_banco(ctx):
    if not await verificar_canal(ctx):
        return
    async with aiosqlite.connect(DB_PATH) as db:
        cursor = await db.execute('SELECT tasa_compra, tasa_venta FROM banco_config WHERE id = 1')
        config_data = await cursor.fetchone()
        tasa_compra = config_data[0] if config_data else 0.9
        tasa_venta = config_data[1] if config_data else 1.1
        db.row_factory = aiosqlite.Row
        cursor = await db.execute('SELECT * FROM banco_productos WHERE activo = 1')
        productos = await cursor.fetchall()
    embed = discord.Embed(title="🏦 **BANCO VP**", description="Invierte tus VP$ y gana intereses", color=COLORS['primary'])
    for prod in productos:
        embed.add_field(name=f"📈 {prod['nombre']}", value=f"Plazo: {prod['duracion_dias']} días\nInterés: {prod['interes_porcentaje']}%\nMonto: {prod['monto_minimo']:,} - {prod['monto_maximo']:,} VP$", inline=False)
    embed.add_field(name="💱 Mercado de Cambio", value=f"• 1,000,000 NG$ → {int(1000000 * tasa_compra):,} VP$\n• 100,000 VP$ → {int(100000 * tasa_venta):,} NG$", inline=False)
    await ctx.send(embed=embed)

@bot.command(name="invertir")
async def cmd_invertir(ctx, producto: str, monto: int):
    if not await verificar_canal(ctx):
        return
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        cursor = await db.execute('SELECT * FROM banco_productos WHERE nombre LIKE ? AND activo = 1', (f'%{producto}%',))
        prod = await cursor.fetchone()
        if not prod:
            await ctx.send(embed=embeds.crear_embed_error("Producto inválido. Usa: basico, plus, vip, elite"))
            return
        if monto < prod['monto_minimo'] or monto > prod['monto_maximo']:
            await ctx.send(embed=embeds.crear_embed_error(f"Monto entre {prod['monto_minimo']:,} y {prod['monto_maximo']:,} VP$"))
            return
        cursor = await db.execute('SELECT balance FROM usuarios_balance WHERE discord_id = ?', (str(ctx.author.id),))
        result = await cursor.fetchone()
        balance = result[0] if result else 0
        if balance < monto:
            await ctx.send(embed=embeds.crear_embed_error(f"Necesitas {monto:,} VP$"))
            return
        fecha_fin = datetime.now() + timedelta(days=prod['duracion_dias'])
        await db.execute('UPDATE usuarios_balance SET balance = balance - ? WHERE discord_id = ?', (monto, str(ctx.author.id)))
        await db.execute('INSERT INTO inversiones (usuario_id, producto, monto, interes, fecha_fin) VALUES (?, ?, ?, ?, ?)', (str(ctx.author.id), prod['nombre'], monto, prod['interes_porcentaje'], fecha_fin))
        await db.commit()
    await bot.verificar_logros(str(ctx.author.id), ctx.author.name, 'inversion', monto)
    embed = discord.Embed(title="✅ Inversión realizada", description=f"Has invertido **{monto:,} VP$** en **{prod['nombre']}**", color=COLORS['success'])
    embed.add_field(name="📅 Plazo", value=f"{prod['duracion_dias']} días", inline=True)
    embed.add_field(name="💰 Interés", value=f"{prod['interes_porcentaje']}%", inline=True)
    embed.add_field(name="💵 Retiro", value=f"≈ {int(monto * (1 + prod['interes_porcentaje']/100)):,} VP$", inline=True)
    await ctx.send(embed=embed)

@bot.command(name="misinversiones")
async def cmd_mis_inversiones(ctx):
    if not await verificar_canal(ctx):
        return
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        cursor = await db.execute('SELECT id, producto, monto, interes, fecha_inicio, fecha_fin FROM inversiones WHERE usuario_id = ? AND estado = "activa"', (str(ctx.author.id),))
        inversiones = await cursor.fetchall()
    if not inversiones:
        await ctx.send(embed=embeds.crear_embed_info("Sin inversiones", "No tienes inversiones activas"))
        return
    embed = discord.Embed(title="📊 Tus inversiones", color=COLORS['primary'])
    for inv in inversiones:
        fecha_fin = datetime.fromisoformat(inv['fecha_fin'])
        dias_restantes = (fecha_fin - datetime.now()).days
        embed.add_field(name=f"#{inv['id']} - {inv['producto']}", value=f"Monto: ${inv['monto']:,}\nInterés: {inv['interes']}%\nRetiro: {dias_restantes} días", inline=False)
    await ctx.send(embed=embed)

@bot.command(name="retirar")
async def cmd_retirar(ctx, inversion_id: int):
    if not await verificar_canal(ctx):
        return
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        cursor = await db.execute('SELECT * FROM inversiones WHERE id = ? AND usuario_id = ? AND estado = "activa"', (inversion_id, str(ctx.author.id)))
        inv = await cursor.fetchone()
        if not inv:
            await ctx.send(embed=embeds.crear_embed_error("Inversión no encontrada"))
            return
        fecha_fin = datetime.fromisoformat(inv['fecha_fin'])
        if datetime.now() < fecha_fin:
            dias_restantes = (fecha_fin - datetime.now()).days
            await ctx.send(embed=embeds.crear_embed_error(f"Faltan {dias_restantes} días para retirar"))
            return
        ganancia_bruta = int(inv['monto'] * inv['interes'] / 100)
        penalizacion = 0
        comision = int(ganancia_bruta * 5 / 100)
        ganancia_neta = ganancia_bruta - comision - penalizacion
        total = inv['monto'] + ganancia_neta
        await db.execute('UPDATE usuarios_balance SET balance = balance + ? WHERE discord_id = ?', (total, str(ctx.author.id)))
        await db.execute('UPDATE inversiones SET estado = "completada" WHERE id = ?', (inversion_id,))
        await db.commit()
    embed = discord.Embed(title="✅ Inversión retirada", description=f"Has retirado **{total:,} VP$**", color=COLORS['success'])
    embed.add_field(name="💰 Inversión inicial", value=f"${inv['monto']:,}", inline=True)
    embed.add_field(name="📈 Interés", value=f"+${ganancia_bruta:,}", inline=True)
    embed.add_field(name="💸 Comisión", value=f"-${comision:,}", inline=True)
    embed.add_field(name="💵 Total", value=f"**${total:,}**", inline=False)
    await ctx.send(embed=embed)

@bot.command(name="cambiarng")
async def cmd_cambiar_ng(ctx, cantidad: int):
    if not await verificar_canal(ctx):
        return
    if cantidad <= 0:
        await ctx.send(embed=embeds.crear_embed_error("Cantidad positiva"))
        return
    async with aiosqlite.connect(DB_PATH) as db:
        cursor = await db.execute('SELECT value FROM config_global WHERE key = "cambio_ng_minimo"')
        min_result = await cursor.fetchone()
        min_cambio = int(min_result[0]) if min_result else 100000
        cursor = await db.execute('SELECT value FROM config_global WHERE key = "cambio_ng_maximo"')
        max_result = await cursor.fetchone()
        max_cambio = int(max_result[0]) if max_result else 10000000
        cursor = await db.execute('SELECT value FROM config_global WHERE key = "tasa_compra"')
        tasa_result = await cursor.fetchone()
        tasa_compra = float(tasa_result[0]) if tasa_result else 0.9
    if cantidad < min_cambio or cantidad > max_cambio:
        await ctx.send(embed=embeds.crear_embed_error(f"Monto entre {min_cambio:,} y {max_cambio:,} NG$"))
        return
    cantidad_vp = int(cantidad * tasa_compra)
    guild = ctx.guild
    categoria = guild.get_channel(CATEGORIA_TICKETS)
    if not categoria:
        await ctx.send(embed=embeds.crear_embed_error("Error: Categoría de tickets no encontrada"))
        return
    overwrites = {
        guild.default_role: discord.PermissionOverwrite(read_messages=False),
        ctx.author: discord.PermissionOverwrite(read_messages=True, send_messages=True),
    }
    for rol_id in [ROLES['CEO'], ROLES['DIRECTOR']]:
        rol = guild.get_role(rol_id)
        if rol:
            overwrites[rol] = discord.PermissionOverwrite(read_messages=True, send_messages=True)
    ticket_num = 1
    async with aiosqlite.connect(DB_PATH) as db:
        cursor = await db.execute('SELECT COUNT(*) FROM tickets_cambio')
        count = await cursor.fetchone()
        ticket_num = count[0] + 1
    ticket_channel = await categoria.create_text_channel(f"ticket-ng-{ticket_num}", overwrites=overwrites)
    embed = discord.Embed(
        title="🎫 **SOLICITUD DE CAMBIO NG$ → VP$**",
        description=f"**Usuario:** {ctx.author.mention}\n"
                    f"**Cantidad NG$:** {cantidad:,} NG$\n"
                    f"**Tasa actual:** {tasa_compra}\n"
                    f"**VP$ a recibir:** {cantidad_vp:,} VP$\n\n"
                    f"**Estado:** ⏰ PENDIENTE",
        color=COLORS['info']
    )
    embed.set_footer(text=f"Ticket ID: {ticket_num} | Usa !procesarvp @user para gestionar")
    await ticket_channel.send(f"{ctx.author.mention}", embed=embed)
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute('INSERT INTO tickets_cambio (usuario_id, usuario_nick, canal_id, cantidad_ng, tasa_compra, cantidad_vp) VALUES (?, ?, ?, ?, ?, ?)', (str(ctx.author.id), ctx.author.name, str(ticket_channel.id), cantidad, tasa_compra, cantidad_vp))
        await db.commit()
    await ctx.send(embed=embeds.crear_embed_exito(f"✅ Ticket creado! Revisa tu ticket privado: {ticket_channel.mention}"))

# ============================================
# COMANDOS DE VENDEDORES
# ============================================

@bot.command(name="vender")
async def cmd_vender(ctx, usuario: discord.Member, numero: int):
    if not await verificar_canal(ctx) or not await check_vendedor(ctx):
        return
    rifa_activa = await bot.db.get_rifa_activa()
    if not rifa_activa:
        await ctx.send(embed=embeds.crear_embed_error("No hay rifa"))
        return
    if numero < 1 or numero > rifa_activa['total_boletos']:
        await ctx.send(embed=embeds.crear_embed_error(f"Número entre 1-{rifa_activa['total_boletos']}"))
        return
    disponibles = await bot.db.get_boletos_disponibles(rifa_activa['id'])
    if numero not in disponibles:
        await ctx.send(embed=embeds.crear_embed_error(f"Número {numero} no disponible"))
        return
    async with aiosqlite.connect(DB_PATH) as db:
        cursor = await db.execute('SELECT balance FROM usuarios_balance WHERE discord_id = ?', (str(usuario.id),))
        result = await cursor.fetchone()
        balance = result[0] if result else 0
    precio_boleto = rifa_activa['precio_boleto']
    descuento = await obtener_descuento_usuario(str(usuario.id))
    precio_final = int(precio_boleto * (100 - descuento) / 100)
    if balance < precio_final:
        await ctx.send(embed=embeds.crear_embed_error(f"Usuario necesita {precio_final:,} VP$"))
        return
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute('UPDATE usuarios_balance SET balance = ? WHERE discord_id = ?', (balance - precio_final, str(usuario.id)))
        await db.execute('INSERT INTO boletos (rifa_id, numero, comprador_id, comprador_nick, vendedor_id, precio_pagado) VALUES (?, ?, ?, ?, ?, ?)', (rifa_activa['id'], numero, str(usuario.id), usuario.name, str(ctx.author.id), precio_boleto))
        await db.commit()
    await actualizar_fidelizacion(str(usuario.id), precio_final)
    await aplicar_cashback(str(usuario.id), precio_final)
    await procesar_comision_referido(str(usuario.id), precio_final)
    await procesar_comision_vendedor(str(ctx.author.id), precio_final)
    await actualizar_jackpot(precio_final)
    await actualizar_ranking_rifa(rifa_activa['id'], str(usuario.id), 1)
    await bot.verificar_logros(str(usuario.id), usuario.name, 'compra', 1)
    await enviar_dm(str(usuario.id), "🎟️ Boleto comprado", f"Has comprado el boleto #{numero} por ${precio_final:,} VP$")
    await enviar_dm(str(ctx.author.id), "💰 Venta realizada", f"Has vendido el boleto #{numero} a {usuario.name} por ${precio_final:,} VP$")
    await ctx.message.delete()
    await ctx.send(embed=embeds.crear_embed_exito(f"✅ Venta realizada. Revisa tu DM."))

@bot.command(name="venderrandom")
async def cmd_vender_random(ctx, usuario: discord.Member, cantidad: int = 1):
    if not await verificar_canal(ctx) or not await check_vendedor(ctx):
        return
    if cantidad < 1 or cantidad > 50:
        await ctx.send(embed=embeds.crear_embed_error("1-50 boletos"))
        return
    rifa_activa = await bot.db.get_rifa_activa()
    if not rifa_activa:
        await ctx.send(embed=embeds.crear_embed_error("No hay rifa"))
        return
    disponibles = await bot.db.get_boletos_disponibles(rifa_activa['id'])
    disponibles_filtrados = [n for n in disponibles if not await es_numero_bloqueado(rifa_activa['id'], n)]
    if len(disponibles_filtrados) < cantidad:
        await ctx.send(embed=embeds.crear_embed_error(f"Solo {len(disponibles_filtrados)} disponibles para venta normal"))
        return
    async with aiosqlite.connect(DB_PATH) as db:
        cursor = await db.execute('SELECT balance FROM usuarios_balance WHERE discord_id = ?', (str(usuario.id),))
        result = await cursor.fetchone()
        balance = result[0] if result else 0
    precio_boleto = rifa_activa['precio_boleto']
    descuento = await obtener_descuento_usuario(str(usuario.id))
    global evento_2x1
    boletos_a_pagar = cantidad
    boletos_a_recibir = cantidad
    if evento_2x1:
        boletos_a_pagar = cantidad // 2 + (cantidad % 2)
        boletos_a_recibir = cantidad
    precio_total = precio_boleto * boletos_a_pagar
    precio_final = int(precio_total * (100 - descuento) / 100)
    if balance < precio_final:
        await ctx.send(embed=embeds.crear_embed_error(f"Usuario necesita {precio_final:,} VP$"))
        return
    seleccionados = random.sample(disponibles_filtrados, boletos_a_recibir)
    comprados = []
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute('UPDATE usuarios_balance SET balance = ? WHERE discord_id = ?', (balance - precio_final, str(usuario.id)))
        for num in seleccionados:
            await db.execute('INSERT INTO boletos (rifa_id, numero, comprador_id, comprador_nick, vendedor_id, precio_pagado) VALUES (?, ?, ?, ?, ?, ?)', (rifa_activa['id'], num, str(usuario.id), usuario.name, str(ctx.author.id), precio_boleto))
            comprados.append(num)
        await db.commit()
    await actualizar_fidelizacion(str(usuario.id), precio_final)
    cashback = await aplicar_cashback(str(usuario.id), precio_final)
    await procesar_comision_referido(str(usuario.id), precio_final)
    await procesar_comision_vendedor(str(ctx.author.id), precio_final)
    await actualizar_jackpot(precio_final)
    await actualizar_ranking_rifa(rifa_activa['id'], str(usuario.id), len(comprados))
    await bot.verificar_logros(str(usuario.id), usuario.name, 'compra', len(comprados))
    await enviar_dm(str(usuario.id), "🎟️ Compra realizada", f"Has comprado {len(comprados)} boletos por ${precio_final:,} VP$")
    await enviar_dm(str(ctx.author.id), "💰 Venta realizada", f"Has vendido {len(comprados)} boletos a {usuario.name} por ${precio_final:,} VP$")
    await ctx.message.delete()
    await ctx.send(embed=embeds.crear_embed_exito(f"✅ Venta realizada. Revisa tu DM."))

@bot.command(name="misventas")
async def cmd_mis_ventas(ctx):
    if not await verificar_canal(ctx) or not await check_vendedor(ctx):
        return
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        cursor = await db.execute('SELECT b.numero, r.nombre as rifa, b.comprador_nick, b.precio_pagado, b.fecha_compra FROM boletos b JOIN rifas r ON b.rifa_id = r.id WHERE b.vendedor_id = ? ORDER BY b.fecha_compra DESC LIMIT 20', (str(ctx.author.id),))
        ventas = await cursor.fetchall()
        cursor = await db.execute('SELECT comisiones_pendientes FROM vendedores WHERE discord_id = ?', (str(ctx.author.id),))
        vendedor = await cursor.fetchone()
    embed = discord.Embed(title="💰 Tus ventas", color=COLORS['primary'])
    if vendedor and vendedor[0] > 0:
        embed.add_field(name="Comisiones pendientes", value=f"**${vendedor[0]:,}**", inline=False)
    if ventas:
        for v in ventas[:5]:
            embed.add_field(name=f"#{v['numero']} - {v['rifa']}", value=f"{v['comprador_nick']} | ${v['precio_pagado']:,} | {v['fecha_compra'][:10]}", inline=False)
    else:
        embed.description = "No tienes ventas"
    await ctx.send(embed=embed)

@bot.command(name="listaboletos")
async def cmd_lista_boletos(ctx):
    if not await verificar_canal(ctx) or not await check_vendedor(ctx):
        return
    rifa_activa = await bot.db.get_rifa_activa()
    if not rifa_activa:
        await ctx.send(embed=embeds.crear_embed_error("No hay rifa"))
        return
    disponibles = await bot.db.get_boletos_disponibles(rifa_activa['id'])
    vendidos = await bot.db.get_boletos_vendidos(rifa_activa['id'])
    embed = discord.Embed(title=f"📋 Boletos - {rifa_activa['nombre']}", description=f"**Total:** {rifa_activa['total_boletos']}\n**Vendidos:** {vendidos}\n**Disponibles:** {len(disponibles)}", color=COLORS['info'])
    await ctx.send(embed=embed)

# ============================================
# COMANDOS DE DIRECTORES Y ADMINISTRACIÓN
# ============================================

@bot.command(name="crearifa")
async def cmd_crear_rifa(ctx, premio: str, precio: int, total: int, bloqueados: str = None):
    if not await check_admin(ctx):
        return
    nombre = f"Rifa {datetime.now().strftime('%d/%m')}"
    async with aiosqlite.connect(DB_PATH) as db:
        cursor = await db.execute('INSERT INTO rifas (nombre, premio, valor_premio, precio_boleto, total_boletos, numeros_bloqueados, estado) VALUES (?, ?, ?, ?, ?, ?, "activa")', (nombre, premio, precio, precio, total, bloqueados))
        rifa_id = cursor.lastrowid
        await db.commit()
    embed = embeds.crear_embed_exito(f"✅ Rifa creada ID: {rifa_id}\nPremio: {premio}\nPrecio: ${precio}\nTotal: {total} boletos")
    if bloqueados:
        embed.add_field(name="🔒 Números VIP", value=f"{bloqueados} (solo venta staff)", inline=False)
    await ctx.send(embed=embed)

@bot.command(name="cerrarifa")
async def cmd_cerrar_rifa(ctx):
    if not await check_admin(ctx):
        return
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute('UPDATE rifas SET estado = "cerrada" WHERE estado = "activa"')
        await db.commit()
    await ctx.message.delete()
    await ctx.send(embed=embeds.crear_embed_exito("✅ Rifa cerrada"))

@bot.command(name="iniciarsorteo")
async def cmd_iniciar_sorteo(ctx, ganadores: int = 1):
    if not await check_admin(ctx):
        return
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        cursor = await db.execute('SELECT * FROM rifas WHERE estado = "activa" ORDER BY id DESC LIMIT 1')
        rifa = await cursor.fetchone()
        if not rifa:
            await ctx.send(embed=embeds.crear_embed_error("No hay rifa activa"))
            return
        cursor = await db.execute('SELECT numero, comprador_id, comprador_nick FROM boletos WHERE rifa_id = ? AND estado = "pagado"', (rifa['id'],))
        boletos = await cursor.fetchall()
    if len(boletos) < ganadores:
        await ctx.send(embed=embeds.crear_embed_error(f"Solo {len(boletos)} boletos vendidos"))
        return
    await ctx.send("🎲 Sorteo en 10 segundos...")
    await asyncio.sleep(10)
    await ctx.send("**3...**")
    await asyncio.sleep(1)
    await ctx.send("**2...**")
    await asyncio.sleep(1)
    await ctx.send("**1...**")
    await asyncio.sleep(1)
    if len(boletos) <= ganadores:
        ganadores_sel = boletos
    else:
        ganadores_sel = random.sample(boletos, ganadores)
    embed = discord.Embed(title="🎉 GANADORES", color=COLORS['success'])
    for i, b in enumerate(ganadores_sel, 1):
        embed.add_field(name=f"#{i}", value=f"#{b['numero']} - {b['comprador_nick']}", inline=False)
        await enviar_dm(b['comprador_id'], "🎉 GANASTE", f"Ganaste la rifa {rifa['nombre']} con #{b['numero']}")
        await registrar_ganador_historico(b['comprador_id'], b['comprador_nick'], rifa['premio'], rifa['nombre'], b['numero'])
    await ctx.send(embed=embed)
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute('UPDATE rifas SET estado = "finalizada" WHERE id = ?', (rifa['id'],))
        await db.commit()

@bot.command(name="finalizarrifa")
async def cmd_finalizar_rifa(ctx, id_rifa: int = None, ganadores: int = 1):
    if not await check_admin(ctx):
        return
    if id_rifa is None:
        rifa = await bot.db.get_rifa_activa()
        if not rifa:
            await ctx.send(embed=embeds.crear_embed_error("Especifica ID"))
            return
        id_rifa = rifa['id']
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        cursor = await db.execute('SELECT * FROM rifas WHERE id = ?', (id_rifa,))
        rifa = await cursor.fetchone()
        if not rifa:
            await ctx.send(embed=embeds.crear_embed_error("ID inválido"))
            return
        cursor = await db.execute('SELECT numero, comprador_id, comprador_nick FROM boletos WHERE rifa_id = ? AND estado = "pagado"', (id_rifa,))
        boletos = await cursor.fetchall()
    if not boletos:
        await ctx.send(embed=embeds.crear_embed_error("Sin boletos"))
        return
    if len(boletos) < ganadores:
        await ctx.send(embed=embeds.crear_embed_error(f"Solo {len(boletos)} boletos"))
        return
    ganadores_sel = random.sample(boletos, ganadores)
    embed = discord.Embed(title=f"🎉 Rifa #{id_rifa} finalizada", description=f"Premio: {rifa['premio']}", color=COLORS['success'])
    for i, b in enumerate(ganadores_sel, 1):
        embed.add_field(name=f"Ganador {i}", value=f"#{b['numero']} - {b['comprador_nick']}", inline=False)
        await enviar_dm(b['comprador_id'], "🎉 GANASTE", f"Ganaste la rifa {rifa['nombre']} (ID: {id_rifa}) con #{b['numero']}")
        await registrar_ganador_historico(b['comprador_id'], b['comprador_nick'], rifa['premio'], rifa['nombre'], b['numero'])
    await ctx.send(embed=embed)

@bot.command(name="vendedoradd")
async def cmd_vendedor_add(ctx, usuario: discord.Member, comision: int = 10):
    if not await check_admin(ctx):
        return
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute('INSERT INTO vendedores (discord_id, nombre, comision) VALUES (?, ?, ?) ON CONFLICT(discord_id) DO UPDATE SET comision = ?, nombre = ?', (str(usuario.id), usuario.name, comision, comision, usuario.name))
        await db.commit()
    try:
        rol = ctx.guild.get_role(ROLES['RIFAS'])
        if rol and rol not in usuario.roles:
            await usuario.add_roles(rol)
    except:
        pass
    await ctx.message.delete()
    await ctx.send(embed=embeds.crear_embed_exito(f"{usuario.name} es vendedor ({comision}%)"))

@bot.command(name="vercomisiones")
async def cmd_ver_comisiones(ctx):
    if not await check_admin(ctx):
        return
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        cursor = await db.execute('SELECT nombre, comisiones_pendientes FROM vendedores WHERE comisiones_pendientes > 0')
        vendedores = await cursor.fetchall()
    if not vendedores:
        await ctx.send(embed=embeds.crear_embed_info("Info", "No hay comisiones pendientes"))
        return
    embed = discord.Embed(title="💰 Comisiones pendientes", color=COLORS['primary'])
    for v in vendedores:
        embed.add_field(name=v['nombre'], value=f"${v['comisiones_pendientes']}", inline=True)
    await ctx.send(embed=embed)

@bot.command(name="pagarcomisiones")
async def cmd_pagar_comisiones(ctx):
    if not await check_admin(ctx):
        return
    await ctx.send("💰 Procesando pagos de comisiones...")
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        cursor = await db.execute('SELECT discord_id, nombre, comisiones_pendientes FROM vendedores WHERE comisiones_pendientes > 0')
        vendedores = await cursor.fetchall()
        if not vendedores:
            await ctx.send(embed=embeds.crear_embed_error("No hay comisiones pendientes"))
            return
        total_pagado = 0
        for v in vendedores:
            monto = v['comisiones_pendientes']
            await db.execute('UPDATE usuarios_balance SET balance = balance + ? WHERE discord_id = ?', (monto, v['discord_id']))
            await db.execute('INSERT INTO transacciones (tipo, monto, destino_id, descripcion) VALUES ("comision", ?, ?, ?)', (monto, v['discord_id'], "Pago de comisiones"))
            await db.execute('UPDATE vendedores SET comisiones_pendientes = 0, comisiones_pagadas = comisiones_pagadas + ? WHERE discord_id = ?', (monto, v['discord_id']))
            total_pagado += monto
            await enviar_dm(v['discord_id'], "💰 Comisiones pagadas", f"Has recibido ${monto} VP$ por tus ventas")
        await db.commit()
    await ctx.message.delete()
    await ctx.send(embed=embeds.crear_embed_exito(f"Pagadas ${total_pagado} VP$ en comisiones a {len(vendedores)} vendedores"))

@bot.command(name="verboletos")
async def cmd_ver_boletos(ctx, usuario: discord.Member):
    if not await check_admin(ctx):
        return
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        cursor = await db.execute('SELECT b.numero, r.nombre as rifa, b.precio_pagado, b.fecha_compra FROM boletos b JOIN rifas r ON b.rifa_id = r.id WHERE b.comprador_id = ? ORDER BY b.fecha_compra DESC', (str(usuario.id),))
        boletos = await cursor.fetchall()
    if not boletos:
        await ctx.send(embed=embeds.crear_embed_info("Sin boletos", f"{usuario.name} no tiene boletos"))
        return
    embed = discord.Embed(title=f"🎟️ Boletos de {usuario.name}", description=f"Total: {len(boletos)} boletos", color=COLORS['primary'])
    for b in boletos[:15]:
        embed.add_field(name=f"#{b['numero']} - {b['rifa']}", value=f"${b['precio_pagado']:,} - {b['fecha_compra'][:10]}", inline=False)
    if len(boletos) > 15:
        embed.set_footer(text=f"Mostrando 15 de {len(boletos)} boletos")
    await ctx.send(embed=embed)

# ============================================
# SUBASTAS (comandos)
# ============================================

@bot.command(name="subasta crear")
async def cmd_subasta_crear(ctx, item: str, precio_base: int, horas: int):
    if not await check_admin(ctx):
        return
    fecha_fin = datetime.now() + timedelta(hours=horas)
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute('INSERT INTO subastas (item_nombre, precio_base, precio_actual, canal_id, fecha_fin, creada_por) VALUES (?, ?, ?, ?, ?, ?)', (item, precio_base, precio_base, str(ctx.channel.id), fecha_fin, str(ctx.author.id)))
        sid = cur.lastrowid
        await db.commit()
    embed = discord.Embed(title=f"🎫 SUBASTA: {item}", description=f"Precio base: ${precio_base:,} VP$\nDuración: {horas} horas", color=COLORS['primary'])
    embed.add_field(name="💰 Puja actual", value=f"${precio_base:,} VP$", inline=False)
    embed.add_field(name="📝 Cómo pujar", value=f"Usa `!pujar {sid} [monto]`", inline=False)
    embed.set_footer(text=f"ID: {sid} | Finaliza: {fecha_fin.strftime('%H:%M')}")
    msg = await ctx.send(embed=embed)
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute('UPDATE subastas SET mensaje_id = ? WHERE id = ?', (str(msg.id), sid))
        await db.commit()

@bot.command(name="pujar")
async def cmd_pujar(ctx, subasta_id: int, monto: int):
    if not await verificar_canal(ctx):
        return
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        cursor = await db.execute('SELECT * FROM subastas WHERE id = ? AND estado = "activa"', (subasta_id,))
        subasta = await cursor.fetchone()
        if not subasta:
            await ctx.send(embed=embeds.crear_embed_error("Subasta no encontrada o finalizada"))
            return
        if monto <= subasta['precio_actual']:
            await ctx.send(embed=embeds.crear_embed_error(f"La puja debe ser mayor a ${subasta['precio_actual']:,} VP$"))
            return
        cursor = await db.execute('SELECT balance FROM usuarios_balance WHERE discord_id = ?', (str(ctx.author.id),))
        result = await cursor.fetchone()
        balance = result[0] if result else 0
        if balance < monto:
            await ctx.send(embed=embeds.crear_embed_error(f"Necesitas ${monto:,} VP$ para pujar"))
            return
        await db.execute('UPDATE subastas SET precio_actual = ? WHERE id = ?', (monto, subasta_id))
        await db.execute('INSERT INTO pujas (subasta_id, usuario_id, usuario_nick, monto) VALUES (?, ?, ?, ?)', (subasta_id, str(ctx.author.id), ctx.author.name, monto))
        await db.commit()
        # Actualizar embed del mensaje de la subasta
        try:
            canal = ctx.channel
            msg = await canal.fetch_message(int(subasta['mensaje_id']))
            if msg.embeds:
                emb = msg.embeds[0]
                for i, field in enumerate(emb.fields):
                    if field.name == "💰 Puja actual":
                        emb.set_field_at(i, name="💰 Puja actual", value=f"${monto:,} VP$ por {ctx.author.name}", inline=False)
                        await msg.edit(embed=emb)
                        break
        except:
            pass
        await ctx.message.delete()
        await ctx.send(embed=embeds.crear_embed_exito(f"✅ Puja realizada! ${monto:,} VP$ en subasta #{subasta_id}"))

@bot.command(name="subastas")
async def cmd_subastas(ctx):
    if not await verificar_canal(ctx):
        return
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        cursor = await db.execute('SELECT * FROM subastas WHERE estado = "activa" ORDER BY fecha_fin ASC')
        subastas = await cursor.fetchall()
    if not subastas:
        await ctx.send(embed=embeds.crear_embed_info("Sin subastas", "No hay subastas activas en este momento"))
        return
    embed = discord.Embed(title="🎫 **SUBASTAS ACTIVAS**", color=COLORS['primary'])
    for s in subastas:
        tiempo_restante = datetime.fromisoformat(s['fecha_fin']) - datetime.now()
        horas = tiempo_restante.total_seconds() // 3600
        minutos = (tiempo_restante.total_seconds() % 3600) // 60
        embed.add_field(name=f"#{s['id']} - {s['item_nombre']}", value=f"💰 Precio actual: ${s['precio_actual']:,} VP$\n🏷️ Precio base: ${s['precio_base']:,} VP$\n⏰ Tiempo restante: {int(horas)}h {int(minutos)}m\n📝 Usa `!pujar {s['id']} [monto]` para pujar", inline=False)
    await ctx.send(embed=embed)

@bot.command(name="mis_pujas")
async def cmd_mis_pujas(ctx):
    if not await verificar_canal(ctx):
        return
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        cursor = await db.execute('SELECT p.*, s.item_nombre, s.fecha_fin, s.estado FROM pujas p JOIN subastas s ON p.subasta_id = s.id WHERE p.usuario_id = ? ORDER BY p.fecha DESC LIMIT 20', (str(ctx.author.id),))
        pujas = await cursor.fetchall()
    if not pujas:
        await ctx.send(embed=embeds.crear_embed_info("Sin pujas", "No has participado en subastas"))
        return
    embed = discord.Embed(title="📊 Tus pujas", color=COLORS['primary'])
    for p in pujas[:10]:
        estado_subasta = "✅ Activa" if p['estado'] == 'activa' else "🏆 Finalizada"
        embed.add_field(name=f"#{p['subasta_id']} - {p['item_nombre']}", value=f"Monto: ${p['monto']:,} VP$\nEstado: {estado_subasta}", inline=False)
    await ctx.send(embed=embed)

# ============================================
# RULETA DIARIA (comandos)
# ============================================

@bot.command(name="ruleta")
async def cmd_ruleta(ctx):
    if not await verificar_canal(ctx):
        return
    async with aiosqlite.connect(DB_PATH) as db:
        cursor = await db.execute('SELECT activo, cooldown_horas, premios, probabilidades, colores, descripciones FROM ruleta_config WHERE id = 1')
        config = await cursor.fetchone()
        if not config or not config[0]:
            await ctx.send(embed=embeds.crear_embed_error("La ruleta está desactivada temporalmente"))
            return
        cursor = await db.execute('SELECT ultimo_giro, proximo_giro FROM ruleta_usuarios WHERE usuario_id = ?', (str(ctx.author.id),))
        usuario = await cursor.fetchone()
        if usuario and usuario[1]:
            proximo = datetime.fromisoformat(usuario[1])
            if datetime.now() < proximo:
                resto = proximo - datetime.now()
                horas = resto.total_seconds() // 3600
                minutos = (resto.total_seconds() % 3600) // 60
                await ctx.send(embed=embeds.crear_embed_error(f"Ya giraste la ruleta hoy. Próximo giro en {int(horas)}h {int(minutos)}m"))
                return
        premios = json.loads(config[2])
        probabilidades = json.loads(config[3])
        colores = json.loads(config[4]) if config[4] else ["⚪"] * len(premios)
        descripciones = json.loads(config[5]) if config[5] else [""] * len(premios)
        elegido = random.choices(premios, weights=probabilidades, k=1)[0]
        indice = premios.index(elegido)
        proximo_giro = datetime.now() + timedelta(hours=config[1])
        if usuario:
            await db.execute('UPDATE ruleta_usuarios SET ultimo_giro = ?, proximo_giro = ? WHERE usuario_id = ?', (datetime.now(), proximo_giro, str(ctx.author.id)))
        else:
            await db.execute('INSERT INTO ruleta_usuarios (usuario_id, ultimo_giro, proximo_giro) VALUES (?, ?, ?)', (str(ctx.author.id), datetime.now(), proximo_giro))
        if elegido > 0:
            await db.execute('UPDATE usuarios_balance SET balance = balance + ? WHERE discord_id = ?', (elegido, str(ctx.author.id)))
        await db.execute('INSERT INTO ruleta_historial (usuario_id, usuario_nick, premio) VALUES (?, ?, ?)', (str(ctx.author.id), ctx.author.name, elegido))
        cursor = await db.execute('SELECT total_giros, total_premiado, mayor_premio FROM ruleta_stats WHERE id = 1')
        stats = await cursor.fetchone()
        total_giros = stats[0] + 1 if stats else 1
        total_premiado = stats[1] + elegido if stats else elegido
        if elegido > (stats[2] if stats else 0):
            await db.execute('UPDATE ruleta_stats SET mayor_premio = ?, ganador_mayor_premio = ? WHERE id = 1', (elegido, ctx.author.name))
        await db.execute('UPDATE ruleta_stats SET total_giros = ?, total_premiado = ?, ultimo_gran_ganador = ?, ultimo_gran_premio = ?, fecha_actualizacion = CURRENT_TIMESTAMP WHERE id = 1', (total_giros, total_premiado, ctx.author.name if elegido >= 50000 else None, elegido if elegido >= 50000 else None))
        await db.commit()
    embed = discord.Embed(title="🎡 **RULETA DE LA FORTUNA** 🎡", description=f"{ctx.author.mention} ha girado la ruleta...\n\n{colores[indice]} **{descripciones[indice]}** {colores[indice]}\n\n✨ **Premio: ${elegido:,} VP$** ✨", color=COLORS['primary'] if elegido > 0 else COLORS['error'])
    if elegido >= 100000:
        embed.add_field(name="🎉 ¡JACKPOT!", value="¡Felicidades! Has ganado el gran premio de la ruleta.", inline=False)
    horas = config[1]
    embed.set_footer(text=f"Próximo giro disponible en {horas} horas")
    await ctx.send(embed=embed)

@bot.command(name="ruleta_stats")
async def cmd_ruleta_stats(ctx, usuario: discord.Member = None):
    if not await verificar_canal(ctx):
        return
    target = usuario if usuario else ctx.author
    async with aiosqlite.connect(DB_PATH) as db:
        cursor = await db.execute('SELECT COUNT(*), SUM(premio) FROM ruleta_historial WHERE usuario_id = ?', (str(target.id),))
        stats = await cursor.fetchone()
        total_giros = stats[0] if stats else 0
        total_ganado = stats[1] if stats and stats[1] else 0
        cursor = await db.execute('SELECT premio FROM ruleta_historial WHERE usuario_id = ? ORDER BY premio DESC LIMIT 1', (str(target.id),))
        mayor = await cursor.fetchone()
        mayor_premio = mayor[0] if mayor else 0
        cursor = await db.execute('SELECT total_giros, total_premiado, mayor_premio, ganador_mayor_premio FROM ruleta_stats WHERE id = 1')
        global_stats = await cursor.fetchone()
    embed = discord.Embed(title=f"🎡 Estadísticas de ruleta - {target.name}", color=COLORS['primary'])
    embed.add_field(name="🎲 Total de giros", value=str(total_giros), inline=True)
    embed.add_field(name="💰 Total ganado", value=f"${total_ganado:,} VP$", inline=True)
    embed.add_field(name="🏆 Mayor premio", value=f"${mayor_premio:,} VP$", inline=True)
    embed.add_field(name="🌍 Giros globales", value=str(global_stats[0] if global_stats else 0), inline=True)
    embed.add_field(name="🌍 Total premiado", value=f"${global_stats[1] if global_stats else 0:,} VP$", inline=True)
    embed.add_field(name="👑 Récord global", value=f"${global_stats[2] if global_stats else 0:,} VP$ - {global_stats[3] if global_stats else 'Nadie'}", inline=True)
    await ctx.send(embed=embed)

# ============================================
# APUESTAS (comandos)
# ============================================

@bot.command(name="apostar")
async def cmd_apostar(ctx, numero: int, cantidad: int):
    if not await verificar_canal(ctx):
        return
    rifa_activa = await bot.db.get_rifa_activa()
    if not rifa_activa:
        await ctx.send(embed=embeds.crear_embed_error("No hay rifa activa"))
        return
    if numero < 1 or numero > rifa_activa['total_boletos']:
        await ctx.send(embed=embeds.crear_embed_error(f"Número entre 1 y {rifa_activa['total_boletos']}"))
        return
    async with aiosqlite.connect(DB_PATH) as db:
        cursor = await db.execute('SELECT activo, apuesta_minima, apuesta_maxima, multiplicador_base, multiplicador_especial, numero_especial FROM apuestas_config WHERE id = 1')
        config = await cursor.fetchone()
        if not config or not config[0]:
            await ctx.send(embed=embeds.crear_embed_error("Las apuestas están desactivadas temporalmente"))
            return
        if cantidad < config[1] or cantidad > config[2]:
            await ctx.send(embed=embeds.crear_embed_error(f"Apuesta mínima: ${config[1]:,} VP$\nApuesta máxima: ${config[2]:,} VP$"))
            return
        cursor = await db.execute('SELECT balance FROM usuarios_balance WHERE discord_id = ?', (str(ctx.author.id),))
        result = await cursor.fetchone()
        balance = result[0] if result else 0
        if balance < cantidad:
            await ctx.send(embed=embeds.crear_embed_error(f"Necesitas ${cantidad:,} VP$"))
            return
        multiplicador = config[4] if numero == config[5] else config[3]
        ganancia_potencial = cantidad * multiplicador
        await db.execute('UPDATE usuarios_balance SET balance = balance - ? WHERE discord_id = ?', (cantidad, str(ctx.author.id)))
        await db.execute('INSERT INTO apuestas (rifa_id, usuario_id, usuario_nick, numero_apostado, monto, ganancia_potencial) VALUES (?, ?, ?, ?, ?, ?)', (rifa_activa['id'], str(ctx.author.id), ctx.author.name, numero, cantidad, ganancia_potencial))
        cursor = await db.execute('SELECT total_apuestas, total_apostado FROM apuestas_stats WHERE usuario_id = ?', (str(ctx.author.id),))
        stats = await cursor.fetchone()
        if stats:
            await db.execute('UPDATE apuestas_stats SET total_apuestas = ?, total_apostado = ? WHERE usuario_id = ?', (stats[0] + 1, stats[1] + cantidad, str(ctx.author.id)))
        else:
            await db.execute('INSERT INTO apuestas_stats (usuario_id, total_apuestas, total_apostado) VALUES (?, ?, ?)', (str(ctx.author.id), 1, cantidad))
        await db.commit()
    embed = discord.Embed(title="🎲 Apuesta registrada", description=f"Has apostado **${cantidad:,} VP$** al número **{numero}**", color=COLORS['info'])
    embed.add_field(name="🎯 Multiplicador", value=f"{multiplicador}x", inline=True)
    embed.add_field(name="💰 Ganancia potencial", value=f"${ganancia_potencial:,} VP$", inline=True)
    await ctx.send(embed=embed)

@bot.command(name="mis_apuestas")
async def cmd_mis_apuestas(ctx):
    if not await verificar_canal(ctx):
        return
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        cursor = await db.execute('SELECT * FROM apuestas WHERE usuario_id = ? AND estado = "activa" ORDER BY fecha_apuesta DESC', (str(ctx.author.id),))
        apuestas = await cursor.fetchall()
        cursor = await db.execute('SELECT * FROM apuestas_historial WHERE usuario_id = ? ORDER BY fecha_resolucion DESC LIMIT 10', (str(ctx.author.id),))
        historial = await cursor.fetchall()
    embed = discord.Embed(title="🎲 Mis apuestas", color=COLORS['primary'])
    if apuestas:
        texto = ""
        for a in apuestas:
            texto += f"#{a['numero_apostado']} - ${a['monto']:,} VP$ (potencial: ${a['ganancia_potencial']:,})\n"
        embed.add_field(name="⏳ Apuestas activas", value=texto, inline=False)
    else:
        embed.add_field(name="⏳ Apuestas activas", value="No tienes apuestas activas", inline=False)
    if historial:
        texto = ""
        for h in historial:
            resultado_emoji = "✅" if h['resultado'] == 'ganada' else "❌"
            texto += f"{resultado_emoji} #{h['numero_apostado']} - ${h['monto']:,} → ${h['ganancia']:,}\n"
        embed.add_field(name="📜 Historial", value=texto, inline=False)
    else:
        embed.add_field(name="📜 Historial", value="No tienes historial de apuestas", inline=False)
    await ctx.send(embed=embed)

@bot.command(name="apuestas_stats")
async def cmd_apuestas_stats(ctx, usuario: discord.Member = None):
    if not await verificar_canal(ctx):
        return
    target = usuario if usuario else ctx.author
    async with aiosqlite.connect(DB_PATH) as db:
        cursor = await db.execute('SELECT total_apuestas, apuestas_ganadas, total_apostado, total_ganado, mejor_ganancia FROM apuestas_stats WHERE usuario_id = ?', (str(target.id),))
        stats = await cursor.fetchone()
    if not stats or stats[0] == 0:
        await ctx.send(embed=embeds.crear_embed_info("Sin estadísticas", f"{target.name} no ha realizado apuestas"))
        return
    porcentaje_ganadas = (stats[1] / stats[0] * 100) if stats[0] > 0 else 0
    balance = stats[4] - stats[2] if stats[4] else -stats[2]
    embed = discord.Embed(title=f"🎲 Estadísticas de apuestas - {target.name}", color=COLORS['primary'])
    embed.add_field(name="🎲 Total apuestas", value=str(stats[0]), inline=True)
    embed.add_field(name="✅ Apuestas ganadas", value=str(stats[1]), inline=True)
    embed.add_field(name="📊 Porcentaje de acierto", value=f"{porcentaje_ganadas:.1f}%", inline=True)
    embed.add_field(name="💰 Total apostado", value=f"${stats[2]:,} VP$", inline=True)
    embed.add_field(name="💵 Total ganado", value=f"${stats[3] if stats[3] else 0:,} VP$", inline=True)
    embed.add_field(name="⚖️ Balance neto", value=f"${balance:,} VP$", inline=True)
    embed.add_field(name="🏆 Mejor ganancia", value=f"${stats[4] if stats[4] else 0:,} VP$", inline=True)
    await ctx.send(embed=embed)

# ============================================
# RIFA ELIMINACIÓN (comandos)
# ============================================

@bot.command(name="celiminacion")
async def cmd_compra_eliminacion(ctx, numero: int):
    if not await verificar_canal(ctx):
        return
    global rifa_eliminacion_activa, rifa_eliminacion_numeros, rifa_eliminacion_valor
    if not rifa_eliminacion_activa:
        await ctx.send(embed=embeds.crear_embed_error("No hay rifa eliminación activa"))
        return
    if numero not in rifa_eliminacion_numeros:
        await ctx.send(embed=embeds.crear_embed_error(f"Número {numero} no disponible"))
        return
    async with aiosqlite.connect(DB_PATH) as db:
        cursor = await db.execute('SELECT balance FROM usuarios_balance WHERE discord_id = ?', (str(ctx.author.id),))
        result = await cursor.fetchone()
        balance = result[0] if result else 0
        if balance < rifa_eliminacion_valor:
            await ctx.send(embed=embeds.crear_embed_error(f"Necesitas ${rifa_eliminacion_valor} VP$"))
            return
        await db.execute('UPDATE usuarios_balance SET balance = balance - ? WHERE discord_id = ?', (rifa_eliminacion_valor, str(ctx.author.id)))
        await db.execute('INSERT INTO transacciones (tipo, monto, origen_id, descripcion) VALUES ("eliminacion", ?, ?, ?)', (rifa_eliminacion_valor, str(ctx.author.id), f"Compra #{numero} en rifa eliminación"))
        await db.commit()
    rifa_eliminacion_numeros.remove(numero)
    if len(rifa_eliminacion_numeros) == 1:
        ganador_numero = rifa_eliminacion_numeros[0]
        embed = discord.Embed(title="🏆 RIFA ELIMINACIÓN FINALIZADA", description=f"¡El número **{ganador_numero}** sobrevivió!\n**Ganador:** <@{ctx.author.id}>\n**Premio:** {rifa_eliminacion_premio}", color=COLORS['success'])
        await ctx.send(embed=embed)
        await enviar_dm(str(ctx.author.id), "🏆 GANASTE", f"Ganaste la rifa eliminación con #{ganador_numero}\nPremio: {rifa_eliminacion_premio}")
        rifa_eliminacion_activa = False
    else:
        await ctx.message.delete()
        await ctx.send(embed=embeds.crear_embed_exito(f"✅ Has comprado el número #{numero} por ${rifa_eliminacion_valor}\nQuedan {len(rifa_eliminacion_numeros)} números"))
        await enviar_dm(str(ctx.author.id), "🎟️ Compra en Rifa Eliminación", f"Has comprado #{numero} por ${rifa_eliminacion_valor}")

@bot.command(name="beliminacion")
async def cmd_ver_eliminacion(ctx):
    if not await verificar_canal(ctx):
        return
    global rifa_eliminacion_activa, rifa_eliminacion_numeros, rifa_eliminacion_total
    if not rifa_eliminacion_activa:
        await ctx.send(embed=embeds.crear_embed_error("No hay rifa eliminación activa"))
        return
    embed = discord.Embed(title="🔪 RIFA ELIMINACIÓN", description=f"**Boletos disponibles:** {len(rifa_eliminacion_numeros)}/{rifa_eliminacion_total}\n**Precio:** ${rifa_eliminacion_valor}", color=COLORS['info'])
    await ctx.send(embed=embed)

@bot.command(name="rifaeliminacion")
async def cmd_rifa_eliminacion(ctx, total: int, premio: str, valor: int):
    if not await check_admin(ctx):
        return
    global rifa_eliminacion_activa, rifa_eliminacion_total, rifa_eliminacion_premio, rifa_eliminacion_valor, rifa_eliminacion_numeros
    if total <= 0 or valor <= 0:
        await ctx.send(embed=embeds.crear_embed_error("Valores inválidos"))
        return
    rifa_eliminacion_activa = True
    rifa_eliminacion_total = total
    rifa_eliminacion_premio = premio
    rifa_eliminacion_valor = valor
    rifa_eliminacion_numeros = list(range(1, total + 1))
    embed = discord.Embed(title="🔪 RIFA ELIMINACIÓN INICIADA", description=f"Premio: {premio}\nValor: ${valor}\nTotal: {total} boletos\n¡El último número que quede GANA!", color=COLORS['primary'])
    await ctx.send(embed=embed)

@bot.command(name="rifaeliminacionr")
async def cmd_rifa_eliminacion_eliminar(ctx, numero: int):
    if not await check_admin(ctx):
        return
    global rifa_eliminacion_activa, rifa_eliminacion_numeros
    if not rifa_eliminacion_activa:
        await ctx.send(embed=embeds.crear_embed_error("No hay rifa eliminación activa"))
        return
    if numero not in rifa_eliminacion_numeros:
        await ctx.send(embed=embeds.crear_embed_error(f"El número {numero} ya fue eliminado"))
        return
    rifa_eliminacion_numeros.remove(numero)
    await ctx.message.delete()
    await ctx.send(embed=embeds.crear_embed_exito(f"Número {numero} eliminado. Quedan {len(rifa_eliminacion_numeros)} números"))

# ============================================
# PUNTOS REVANCHA
# ============================================

@bot.command(name="mispuntos")
async def cmd_mis_puntos(ctx):
    if not await verificar_canal(ctx):
        return
    async with aiosqlite.connect(DB_PATH) as db:
        cursor = await db.execute('SELECT puntos FROM puntos_revancha WHERE usuario_id = ?', (str(ctx.author.id),))
        result = await cursor.fetchone()
        puntos = result[0] if result else 0
    embed = discord.Embed(title="🔄 Puntos de Revancha", description=f"Tienes **{puntos}** puntos", color=COLORS['primary'])
    embed.set_footer(text="Se acumulan por boletos perdidos")
    await ctx.send(embed=embed)

# ============================================
# MISIONES DIARIAS Y SEMANALES (comandos)
# ============================================

@bot.command(name="misiones")
async def cmd_misiones(ctx):
    if not await verificar_canal(ctx):
        return
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        cursor = await db.execute('SELECT * FROM misiones WHERE activo = 1')
        misiones = await cursor.fetchall()
        progreso = {}
        for m in misiones:
            cursor = await db.execute('SELECT progreso, completada, reclamada FROM progreso_misiones WHERE usuario_id = ? AND mision_id = ?', (str(ctx.author.id), m['id']))
            result = await cursor.fetchone()
            if result:
                progreso[m['id']] = {'progreso': result[0], 'completada': result[1], 'reclamada': result[2]}
            else:
                progreso[m['id']] = {'progreso': 0, 'completada': False, 'reclamada': False}
        cursor = await db.execute('SELECT racha FROM rachas WHERE usuario_id = ?', (str(ctx.author.id),))
        racha_result = await cursor.fetchone()
        racha = racha_result[0] if racha_result else 0
    embed = discord.Embed(title="📋 MISIONES DIARIAS", description=f"Racha actual: **{racha}** días 🔥", color=COLORS['primary'])
    for m in misiones:
        if progreso[m['id']]['reclamada']:
            estado = "✅ RECLAMADA"
        elif progreso[m['id']]['completada']:
            estado = "🎁 LISTA PARA RECLAMAR"
        else:
            estado = f"⏳ Progreso: {progreso[m['id']]['progreso']}/{m['valor_requisito']}"
        texto = f"{m['descripcion']}\nRecompensa: ${m['recompensa']:,} VP$\n{estado}"
        embed.add_field(name=m['nombre'], value=texto, inline=False)
    await ctx.send(embed=embed)

@bot.command(name="misiones_semanales")
async def cmd_misiones_semanales(ctx):
    if not await verificar_canal(ctx):
        return
    semana_actual = datetime.now().isocalendar()[1]
    año_actual = datetime.now().year
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        cursor = await db.execute('SELECT * FROM misiones_semanales WHERE activo = 1 AND semana = ? AND año = ?', (semana_actual, año_actual))
        misiones = await cursor.fetchall()
        if not misiones:
            await ctx.send(embed=embeds.crear_embed_info("Sin misiones", "No hay misiones semanales disponibles"))
            return
        progreso = {}
        for m in misiones:
            cursor = await db.execute('SELECT progreso, completada, reclamada FROM misiones_semanales_progreso WHERE usuario_id = ? AND mision_id = ?', (str(ctx.author.id), m['id']))
            result = await cursor.fetchone()
            if result:
                progreso[m['id']] = {'progreso': result[0], 'completada': result[1], 'reclamada': result[2]}
            else:
                progreso[m['id']] = {'progreso': 0, 'completada': False, 'reclamada': False}
    embed = discord.Embed(title="📋 MISIONES SEMANALES", description=f"Semana {semana_actual} del {año_actual}", color=COLORS['primary'])
    for m in misiones:
        if progreso[m['id']]['reclamada']:
            estado = "✅ RECLAMADA"
        elif progreso[m['id']]['completada']:
            estado = "🎁 LISTA PARA RECLAMAR"
        else:
            estado = f"⏳ Progreso: {progreso[m['id']]['progreso']}/{m['requisito_valor']}"
        texto = f"{m['descripcion']}\nRecompensa: ${m['recompensa_vp']:,} VP$"
        if m['recompensa_caja_tipo']:
            texto += f" + Caja {m['recompensa_caja_tipo'].capitalize()}"
        if m['recompensa_titulo']:
            texto += f" + Título '{m['recompensa_titulo']}'"
        texto += f"\n{estado}"
        embed.add_field(name=f"{m['emoji']} {m['nombre']}", value=texto, inline=False)
    await ctx.send(embed=embed)

@bot.command(name="miracha")
async def cmd_miracha(ctx):
    if not await verificar_canal(ctx):
        return
    async with aiosqlite.connect(DB_PATH) as db:
        cursor = await db.execute('SELECT racha, mejor_racha FROM rachas WHERE usuario_id = ?', (str(ctx.author.id),))
        result = await cursor.fetchone()
        racha = result[0] if result else 0
        mejor_racha = result[1] if result else 0
    embed = discord.Embed(title="🔥 Tu racha", description=f"Racha actual: **{racha}** días\nMejor racha: **{mejor_racha}** días", color=COLORS['primary'])
    bonos = [(3, "+500 VP$"), (7, "+2,000 VP$"), (14, "+10,000 VP$"), (30, "+50,000 VP$ + Caja Épica")]
    texto_bonos = ""
    for dias, bono in bonos:
        if racha < dias:
            texto_bonos += f"• En {dias} días: {bono}\n"
    if texto_bonos:
        embed.add_field(name="🎁 Próximos bonos", value=texto_bonos, inline=False)
    await ctx.send(embed=embed)

@bot.command(name="reclamar")
async def cmd_reclamar(ctx, mision_id: int = None):
    if not await verificar_canal(ctx):
        return
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        cursor = await db.execute('SELECT * FROM misiones WHERE id = ? AND activo = 1', (mision_id,))
        mision = await cursor.fetchone()
        if not mision:
            await ctx.send(embed=embeds.crear_embed_error("Misión no encontrada"))
            return
        cursor = await db.execute('SELECT completada, reclamada FROM progreso_misiones WHERE usuario_id = ? AND mision_id = ?', (str(ctx.author.id), mision_id))
        progreso = await cursor.fetchone()
        if not progreso or not progreso['completada']:
            await ctx.send(embed=embeds.crear_embed_error("No has completado esta misión aún"))
            return
        if progreso['reclamada']:
            await ctx.send(embed=embeds.crear_embed_error("Ya reclamaste esta recompensa"))
            return
        await db.execute('UPDATE usuarios_balance SET balance = balance + ? WHERE discord_id = ?', (mision['recompensa'], str(ctx.author.id)))
        await db.execute('UPDATE progreso_misiones SET reclamada = 1 WHERE usuario_id = ? AND mision_id = ?', (str(ctx.author.id), mision_id))
        if mision['tipo'] == 'diaria':
            cursor = await db.execute('SELECT racha FROM rachas WHERE usuario_id = ?', (str(ctx.author.id),))
            racha_data = await cursor.fetchone()
            nueva_racha = (racha_data[0] + 1) if racha_data else 1
            await db.execute('INSERT INTO rachas (usuario_id, racha, ultima_completada, mejor_racha) VALUES (?, ?, CURRENT_TIMESTAMP, ?) ON CONFLICT(usuario_id) DO UPDATE SET racha = CASE WHEN julianday(CURRENT_TIMESTAMP) - julianday(ultima_completada) <= 1 THEN racha + 1 ELSE 1 END, ultima_completada = CURRENT_TIMESTAMP, mejor_racha = MAX(mejor_racha, racha + 1)', (str(ctx.author.id), nueva_racha, nueva_racha))
            await bot.verificar_logros(str(ctx.author.id), ctx.author.name, 'racha', nueva_racha)
        await db.commit()
    await ctx.send(embed=embeds.crear_embed_exito(f"✅ Reclamaste ${mision['recompensa']:,} VP$ de la misión {mision['nombre']}"))

# ============================================
# COMANDOS DE CEO (adicionales)
# ============================================

@bot.command(name="acreditarvp")
async def cmd_acreditarvp(ctx, usuario: discord.Member, cantidad: int):
    if not await check_ceo(ctx):
        return
    if cantidad <= 0:
        await ctx.send(embed=embeds.crear_embed_error("Cantidad positiva"))
        return
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute('INSERT INTO usuarios_balance (discord_id, nombre, balance) VALUES (?, ?, ?) ON CONFLICT(discord_id) DO UPDATE SET balance = balance + ?', (str(usuario.id), usuario.name, cantidad, cantidad))
        await db.execute('INSERT INTO transacciones (tipo, monto, destino_id, descripcion) VALUES ("acreditar", ?, ?, ?)', (cantidad, str(usuario.id), f"Acreditación por {ctx.author.name}"))
        await db.commit()
    await enviar_dm(str(usuario.id), "💰 Acreditación de VP$", f"Se te han acreditado ${cantidad} VP$ en tu balance")
    await ctx.message.delete()
    await ctx.send(embed=embeds.crear_embed_exito(f"Acreditados ${cantidad} VP$ a {usuario.name}"))

@bot.command(name="retirarvp")
async def cmd_retirarvp(ctx, usuario: discord.Member, cantidad: int):
    if not await check_ceo(ctx):
        return
    if cantidad <= 0:
        await ctx.send(embed=embeds.crear_embed_error("Cantidad positiva"))
        return
    async with aiosqlite.connect(DB_PATH) as db:
        cursor = await db.execute('SELECT balance FROM usuarios_balance WHERE discord_id = ?', (str(usuario.id),))
        result = await cursor.fetchone()
        if not result or result[0] < cantidad:
            await ctx.send(embed=embeds.crear_embed_error("Saldo insuficiente"))
            return
        await db.execute('UPDATE usuarios_balance SET balance = balance - ? WHERE discord_id = ?', (cantidad, str(usuario.id)))
        await db.execute('INSERT INTO transacciones (tipo, monto, origen_id, descripcion) VALUES ("retirar", ?, ?, ?)', (cantidad, str(usuario.id), f"Retiro por {ctx.author.name}"))
        await db.commit()
    await enviar_dm(str(usuario.id), "💰 Retiro de VP$", f"Se te han retirado ${cantidad} VP$ de tu balance")
    await ctx.message.delete()
    await ctx.send(embed=embeds.crear_embed_exito(f"Retirados ${cantidad} VP$ de {usuario.name}"))

@bot.command(name="procesarvp")
async def cmd_procesar_vp(ctx, usuario: discord.Member):
    if not await check_ceo(ctx):
        return
    async with aiosqlite.connect(DB_PATH) as db:
        cursor = await db.execute('SELECT id, canal_id, cantidad_ng, cantidad_vp FROM tickets_cambio WHERE usuario_id = ? AND estado = "pendiente" ORDER BY fecha_creacion DESC LIMIT 1', (str(usuario.id),))
        ticket = await cursor.fetchone()
        if not ticket:
            await ctx.send(embed=embeds.crear_embed_error(f"{usuario.name} no tiene tickets pendientes"))
            return
        await db.execute('UPDATE tickets_cambio SET estado = "procesando", fecha_procesado = CURRENT_TIMESTAMP, procesado_por = ? WHERE id = ?', (str(ctx.author.id), ticket[0]))
        await db.commit()
        canal = bot.get_channel(int(ticket[1]))
        if canal:
            embed = discord.Embed(title="🔄 **ESTADO ACTUALIZADO**", description=f"⏳ Su pago de **{ticket[2]:,} NG$** está siendo **PROCESADO**\n✅ Pronto recibirá sus VP$", color=COLORS['info'])
            await canal.send(embed=embed)
    await ctx.message.delete()
    await ctx.send(embed=embeds.crear_embed_exito(f"✅ Ticket de {usuario.name} marcado como en proceso"))

@bot.command(name="procesadovp")
async def cmd_procesado_vp(ctx, usuario: discord.Member, cantidad_vp: int):
    if not await check_ceo(ctx):
        return
    async with aiosqlite.connect(DB_PATH) as db:
        cursor = await db.execute('SELECT id, canal_id, cantidad_ng, cantidad_vp FROM tickets_cambio WHERE usuario_id = ? AND estado = "procesando" ORDER BY fecha_creacion DESC LIMIT 1', (str(usuario.id),))
        ticket = await cursor.fetchone()
        if not ticket:
            await ctx.send(embed=embeds.crear_embed_error(f"{usuario.name} no tiene tickets en proceso"))
            return
        await db.execute('INSERT INTO usuarios_balance (discord_id, nombre, balance) VALUES (?, ?, ?) ON CONFLICT(discord_id) DO UPDATE SET balance = balance + ?', (str(usuario.id), usuario.name, cantidad_vp, cantidad_vp))
        await db.execute('UPDATE tickets_cambio SET estado = "completado", cantidad_vp = ? WHERE id = ?', (cantidad_vp, ticket[0]))
        tiempo_procesado = int((datetime.now() - datetime.fromisoformat(str(await db.execute('SELECT fecha_creacion FROM tickets_cambio WHERE id = ?', (ticket[0],)).fetchone()[0]))).total_seconds())
        await db.execute('INSERT INTO tickets_historial (ticket_id, usuario_id, cantidad_ng, cantidad_vp, tasa_utilizada, tiempo_procesado, procesado_por) VALUES (?, ?, ?, ?, ?, ?, ?)', (ticket[0], str(usuario.id), ticket[2], cantidad_vp, ticket[3] / ticket[2] if ticket[2] > 0 else 0, tiempo_procesado, str(ctx.author.id)))
        await db.commit()
        canal = bot.get_channel(int(ticket[1]))
        if canal:
            embed = discord.Embed(title="✅ **PAGO CONFIRMADO**", description=f"💰 Se le han acreditado **{cantidad_vp:,} VP$**\n📊 Nuevo balance: {cantidad_vp:,} VP$\n🎫 Este ticket se cerrará en 10 segundos...", color=COLORS['success'])
            await canal.send(embed=embed)
            await asyncio.sleep(10)
            await canal.delete()
    await ctx.message.delete()
    await ctx.send(embed=embeds.crear_embed_exito(f"✅ Acreditados ${cantidad_vp:,} VP$ a {usuario.name}"))

@bot.command(name="ticketcerrar")
async def cmd_ticket_cerrar(ctx):
    if not await check_admin(ctx):
        return
    if not ctx.channel.name.startswith("ticket"):
        await ctx.send(embed=embeds.crear_embed_error("Este comando solo funciona en canales de ticket"))
        return
    embed = discord.Embed(title="🔒 Cerrando ticket", description="Este ticket se cerrará en 5 segundos...", color=COLORS['info'])
    await ctx.send(embed=embed)
    await asyncio.sleep(5)
    await ctx.channel.delete()

@bot.command(name="setrefcomision")
async def cmd_set_ref_comision(ctx, porcentaje: int):
    if not await check_ceo(ctx):
        return
    if porcentaje < 0 or porcentaje > 50:
        await ctx.send(embed=embeds.crear_embed_error("0-50%"))
        return
    global REFERIDOS_PORCENTAJE
    REFERIDOS_PORCENTAJE = porcentaje
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute('UPDATE referidos_config SET porcentaje_comision = ? WHERE id = 1', (porcentaje,))
        await db.commit()
    await ctx.message.delete()
    await ctx.send(embed=embeds.crear_embed_exito(f"Comisión referidos: {porcentaje}%"))

@bot.command(name="setcashback")
async def cmd_set_cashback(ctx, porcentaje: int):
    if not await check_ceo(ctx):
        return
    if porcentaje < 0 or porcentaje > 50:
        await ctx.send(embed=embeds.crear_embed_error("0-50%"))
        return
    global CASHBACK_PORCENTAJE
    CASHBACK_PORCENTAJE = porcentaje
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute('UPDATE cashback_config SET porcentaje = ? WHERE id = 1', (porcentaje,))
        await db.commit()
    await ctx.message.delete()
    await ctx.send(embed=embeds.crear_embed_exito(f"Cashback: {porcentaje}%"))

@bot.command(name="pagarcashback")
async def cmd_pagar_cashback(ctx):
    if not await check_ceo(ctx):
        return
    await ctx.send("💰 Procesando pagos de cashback...")
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        cursor = await db.execute('SELECT usuario_id, cashback_acumulado FROM cashback WHERE cashback_acumulado > 0')
        usuarios = await cursor.fetchall()
        if not usuarios:
            await ctx.send(embed=embeds.crear_embed_error("No hay cashback para pagar"))
            return
        total_pagado = 0
        for u in usuarios:
            monto = u['cashback_acumulado']
            await db.execute('UPDATE usuarios_balance SET balance = balance + ? WHERE discord_id = ?', (monto, u['usuario_id']))
            await db.execute('INSERT INTO transacciones (tipo, monto, destino_id, descripcion) VALUES ("cashback", ?, ?, ?)', (monto, u['usuario_id'], "Pago de cashback"))
            await db.execute('UPDATE cashback SET cashback_acumulado = 0, cashback_recibido = cashback_recibido + ? WHERE usuario_id = ?', (monto, u['usuario_id']))
            total_pagado += monto
            await enviar_dm(u['usuario_id'], "💰 Cashback pagado", f"Has recibido ${monto} VP$ por cashback")
        await db.commit()
    await ctx.message.delete()
    await ctx.send(embed=embeds.crear_embed_exito(f"Pagados ${total_pagado} VP$ de cashback a {len(usuarios)} usuarios"))

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
        await ctx.send(embed=embeds.crear_embed_error("Uso: !evento 2x1 on/off\n!evento cashbackdoble on/off\n!evento oferta on/off [%]"))
        return
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute('UPDATE eventos_activos SET evento_2x1 = ?, cashback_doble = ?, oferta_activa = ?, oferta_porcentaje = ? WHERE id = 1', (evento_2x1, evento_cashback_doble, evento_oferta_activa, evento_oferta_porcentaje))
        await db.commit()
    await ctx.message.delete()
    await ctx.send(embed=embeds.crear_embed_exito(f"🎉 Evento {tipo.upper()} {estado}"))

@bot.command(name="config")
async def cmd_config(ctx, accion: str, key: str = None, valor: str = None):
    if not await check_ceo(ctx):
        return
    if accion == "get" and key:
        async with aiosqlite.connect(DB_PATH) as db:
            cursor = await db.execute('SELECT value, descripcion FROM config_global WHERE key = ?', (key,))
            result = await cursor.fetchone()
            if result:
                embed = discord.Embed(title="⚙️ Configuración", description=f"**{key}** = `{result[0]}`\n📝 {result[1]}", color=COLORS['info'])
                await ctx.send(embed=embed)
            else:
                await ctx.send(embed=embeds.crear_embed_error("Clave no encontrada"))
    elif accion == "set" and key and valor:
        async with aiosqlite.connect(DB_PATH) as db:
            await db.execute('UPDATE config_global SET value = ?, actualizado_por = ?, fecha_actualizacion = CURRENT_TIMESTAMP WHERE key = ?', (valor, str(ctx.author.id), key))
            await db.commit()
        await ctx.message.delete()
        await ctx.send(embed=embeds.crear_embed_exito(f"Configuración actualizada: {key} = {valor}"))
    elif accion == "list":
        async with aiosqlite.connect(DB_PATH) as db:
            db.row_factory = aiosqlite.Row
            cursor = await db.execute('SELECT key, value, descripcion FROM config_global ORDER BY key')
            configs = await cursor.fetchall()
        embed = discord.Embed(title="📋 Configuración global", color=COLORS['primary'])
        for c in configs:
            embed.add_field(name=c['key'], value=f"`{c['value']}`\n{c['descripcion']}", inline=False)
        await ctx.send(embed=embed)
    else:
        await ctx.send(embed=embeds.crear_embed_error("Uso: `!config get [key]`, `!config set [key] [valor]`, `!config list`"))

@bot.command(name="backup")
async def cmd_backup(ctx):
    if not await check_ceo(ctx):
        return
    fecha = datetime.now().strftime("%Y%m%d_%H%M%S")
    backup_file = f"backups/backup_{fecha}.db"
    shutil.copy2(DB_PATH, backup_file)
    await ctx.author.send(file=discord.File(backup_file))
    await ctx.message.delete()
    await ctx.send(embed=embeds.crear_embed_exito("✅ Backup creado. Revisa tu DM."))

@bot.command(name="resetallsistema")
async def cmd_reset_all_sistema(ctx):
    global reset_pending
    if not await check_ceo(ctx):
        return
    embed = discord.Embed(title="⚠️ REINICIO TOTAL", description="Esto borrará TODOS los datos.\nEscribe `!confirmarreset` en 30s", color=COLORS['error'])
    await ctx.send(embed=embed)
    reset_pending = {'usuario_id': ctx.author.id, 'timestamp': datetime.now().timestamp()}

@bot.command(name="confirmarreset")
async def cmd_confirmar_reset(ctx):
    global reset_pending
    if not await check_ceo(ctx):
        return
    if not reset_pending or reset_pending.get('usuario_id') != ctx.author.id:
        await ctx.send(embed=embeds.crear_embed_error("Sin solicitud"))
        return
    if datetime.now().timestamp() - reset_pending['timestamp'] > 30:
        reset_pending = None
        await ctx.send(embed=embeds.crear_embed_error("Tiempo expirado"))
        return
    await ctx.send("🔄 REINICIANDO SISTEMA...")
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
        await db.execute("DELETE FROM codigos_promocionales")
        await db.execute("DELETE FROM codigos_canjeados")
        await db.execute("DELETE FROM puntos_revancha")
        await db.execute("DELETE FROM cajas")
        await db.execute("DELETE FROM cajas_compradas")
        await db.execute("DELETE FROM distribuidores")
        await db.execute("DELETE FROM productos")
        await db.execute("DELETE FROM inventario_distribuidor")
        await db.execute("DELETE FROM misiones")
        await db.execute("DELETE FROM progreso_misiones")
        await db.execute("DELETE FROM rachas")
        await db.execute("DELETE FROM inversiones")
        await db.execute("DELETE FROM prestamos")
        await db.execute("DELETE FROM transacciones_cambio")
        await db.execute("DELETE FROM pagos_pendientes")
        await db.execute("DELETE FROM subastas")
        await db.execute("DELETE FROM pujas")
        await db.execute("DELETE FROM tickets_subasta")
        await db.execute("DELETE FROM logros")
        await db.execute("DELETE FROM usuarios_logros")
        await db.execute("DELETE FROM usuarios_stats")
        await db.execute("DELETE FROM notificaciones_preferencias")
        await db.execute("DELETE FROM notificaciones_historial")
        await db.execute("DELETE FROM regalos")
        await db.execute("DELETE FROM marketplace_listings")
        await db.execute("DELETE FROM marketplace_ofertas")
        await db.execute("DELETE FROM personalizacion_items")
        await db.execute("DELETE FROM personalizacion_usuarios")
        await db.execute("DELETE FROM perfiles_cache")
        await db.execute("DELETE FROM ruleta_config")
        await db.execute("DELETE FROM ruleta_historial")
        await db.execute("DELETE FROM ruleta_usuarios")
        await db.execute("DELETE FROM apuestas")
        await db.execute("DELETE FROM apuestas_historial")
        await db.execute("DELETE FROM apuestas_stats")
        await db.execute("DELETE FROM franquicias")
        await db.execute("DELETE FROM franquicias_rifas")
        await db.execute("DELETE FROM sqlite_sequence")
        await db.commit()
    reset_pending = None
    await ctx.message.delete()
    await ctx.send(embed=embeds.crear_embed_exito("✅ Sistema reiniciado"))

# ============================================
# COMANDOS DE DISTRIBUIDORES
# ============================================

@bot.command(name="distribuidor")
async def cmd_distribuidor(ctx):
    if not await verificar_canal(ctx) or not await check_distribuidor(ctx):
        await ctx.send(embed=embeds.crear_embed_error("No tienes permiso o no eres distribuidor"))
        return
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        cursor = await db.execute('SELECT * FROM distribuidores WHERE discord_id = ?', (str(ctx.author.id),))
        dist = await cursor.fetchone()
        if not dist:
            await ctx.send(embed=embeds.crear_embed_error("No eres distribuidor. Contacta al CEO"))
            return
        nivel_letra = {1: 'A', 2: 'B', 3: 'C', 4: 'D', 5: 'E', 6: 'F'}.get(dist['nivel'], 'A')
        embed = discord.Embed(title="📦 Tu perfil de distribuidor", color=COLORS['primary'])
        embed.add_field(name="📊 Nivel", value=f"{nivel_letra} (Nivel {dist['nivel']})", inline=True)
        embed.add_field(name="💰 Comisión", value=f"{dist['comision']}%", inline=True)
        embed.add_field(name="📈 Ventas totales", value=f"{dist['ventas_totales']}", inline=True)
        embed.add_field(name="⏳ Comisiones pendientes", value=f"${dist['comisiones_pendientes']:,} VP$", inline=True)
        embed.add_field(name="✅ Comisiones pagadas", value=f"${dist['comisiones_pagadas']:,} VP$", inline=True)
        await ctx.send(embed=embed)

@bot.command(name="productos")
async def cmd_productos(ctx):
    if not await verificar_canal(ctx) or not await check_distribuidor(ctx):
        await ctx.send(embed=embeds.crear_embed_error("No tienes permiso"))
        return
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        cursor = await db.execute('SELECT * FROM productos WHERE activo = 1 ORDER BY nivel_minimo ASC')
        productos = await cursor.fetchall()
    embed = discord.Embed(title="📦 Catálogo de productos", color=COLORS['primary'])
    for prod in productos:
        nivel_letra = {1: 'A', 2: 'B', 3: 'C', 4: 'D', 5: 'E', 6: 'F'}.get(prod['nivel_minimo'], 'A')
        embed.add_field(name=f"{prod['nombre']} (Nivel {nivel_letra})", value=f"{prod['descripcion']}\n💰 Normal: ${prod['precio_normal']:,}\n📦 Mayorista: ${prod['precio_mayorista']:,}\n📊 Stock: {'Ilimitado' if prod['stock'] == -1 else prod['stock']}", inline=False)
    await ctx.send(embed=embed)

@bot.command(name="comprar_producto")
async def cmd_comprar_producto(ctx, nombre: str, cantidad: int = 1):
    if not await verificar_canal(ctx) or not await check_distribuidor(ctx):
        await ctx.send(embed=embeds.crear_embed_error("No tienes permiso"))
        return
    if cantidad < 1 or cantidad > 100:
        await ctx.send(embed=embeds.crear_embed_error("Cantidad 1-100"))
        return
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        cursor = await db.execute('SELECT * FROM productos WHERE nombre LIKE ? AND activo = 1', (f'%{nombre}%',))
        producto = await cursor.fetchone()
        if not producto:
            await ctx.send(embed=embeds.crear_embed_error("Producto no encontrado"))
            return
        cursor = await db.execute('SELECT nivel FROM distribuidores WHERE discord_id = ?', (str(ctx.author.id),))
        dist = await cursor.fetchone()
        nivel_dist = dist['nivel'] if dist else 1
        if nivel_dist < producto['nivel_minimo']:
            nivel_letra = {1: 'A', 2: 'B', 3: 'C', 4: 'D', 5: 'E', 6: 'F'}.get(producto['nivel_minimo'], 'A')
            await ctx.send(embed=embeds.crear_embed_error(f"Necesitas nivel {nivel_letra}"))
            return
        precio_total = producto['precio_mayorista'] * cantidad
        cursor = await db.execute('SELECT balance FROM usuarios_balance WHERE discord_id = ?', (str(ctx.author.id),))
        result = await cursor.fetchone()
        balance = result[0] if result else 0
        if balance < precio_total:
            await ctx.send(embed=embeds.crear_embed_error(f"Necesitas {precio_total:,} VP$"))
            return
        await db.execute('UPDATE usuarios_balance SET balance = balance - ? WHERE discord_id = ?', (precio_total, str(ctx.author.id)))
        cursor = await db.execute('SELECT cantidad FROM inventario_distribuidor WHERE distribuidor_id = ? AND producto_id = ?', (str(ctx.author.id), producto['id']))
        inv = await cursor.fetchone()
        if inv:
            await db.execute('UPDATE inventario_distribuidor SET cantidad = cantidad + ? WHERE distribuidor_id = ? AND producto_id = ?', (cantidad, str(ctx.author.id), producto['id']))
        else:
            await db.execute('INSERT INTO inventario_distribuidor (distribuidor_id, producto_id, cantidad) VALUES (?, ?, ?)', (str(ctx.author.id), producto['id'], cantidad))
        await db.commit()
    embed = discord.Embed(title="✅ Compra realizada", description=f"Has comprado {cantidad}x {producto['nombre']} por ${precio_total:,} VP$", color=COLORS['success'])
    await ctx.send(embed=embed)

@bot.command(name="mis_productos")
async def cmd_mis_productos(ctx):
    if not await verificar_canal(ctx) or not await check_distribuidor(ctx):
        await ctx.send(embed=embeds.crear_embed_error("No tienes permiso"))
        return
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        cursor = await db.execute('SELECT i.cantidad, p.nombre, p.precio_normal FROM inventario_distribuidor i JOIN productos p ON i.producto_id = p.id WHERE i.distribuidor_id = ? AND i.cantidad > 0', (str(ctx.author.id),))
        inventario = await cursor.fetchall()
    if not inventario:
        await ctx.send(embed=embeds.crear_embed_info("Sin inventario", "No tienes productos en inventario"))
        return
    embed = discord.Embed(title="📦 Tu inventario", color=COLORS['primary'])
    total_valor = 0
    for item in inventario:
        valor = item['cantidad'] * item['precio_normal']
        total_valor += valor
        embed.add_field(name=f"{item['nombre']}", value=f"Cantidad: {item['cantidad']}\nValor de reventa: ${valor:,} VP$", inline=False)
    embed.add_field(name="💰 Valor total del inventario", value=f"**${total_valor:,} VP$**", inline=False)
    await ctx.send(embed=embed)

# ============================================
# COMANDOS DE FRANQUICIAS
# ============================================

@bot.command(name="franquicia")
async def cmd_franquicia(ctx):
    if not await verificar_canal(ctx, CATEGORIA_FRANQUICIAS) or not await check_franquicia(ctx):
        await ctx.send(embed=embeds.crear_embed_error("No tienes una franquicia activa o no estás en el canal correcto"))
        return
    nivel = 0
    for n, datos in ROLES_FRANQUICIA.items():
        if tiene_rol(ctx.author, datos['rol_id']):
            nivel = n
            break
    embed = discord.Embed(title="👑 Tu Franquicia VP", description=f"Nivel: **{nivel}**\nCanal exclusivo: <#{ROLES_FRANQUICIA[nivel]['canal_id']}>", color=COLORS['primary'])
    beneficios = {1: "• 5% de comisión por ventas\n• Acceso a productos básicos", 2: "• 7% de comisión por ventas\n• Pack de 10 boletos disponible", 3: "• 10% de comisión por ventas\n• Pack de 50 boletos disponible", 4: "• 12% de comisión por ventas\n• Productos VIP", 5: "• 15% de comisión por ventas\n• TODO el catálogo"}
    embed.add_field(name="🎁 Beneficios", value=beneficios.get(nivel, "• Comisión base"), inline=False)
    embed.add_field(name="📈 Próximo nivel", value=f"Faltan {1000 - nivel * 200} ventas para nivel {nivel + 1}" if nivel < 5 else "Máximo nivel alcanzado", inline=False)
    await ctx.send(embed=embed)

@bot.command(name="franquicia_rifa")
async def cmd_franquicia_rifa(ctx, premio: str, precio: int, total: int):
    if not await verificar_canal(ctx, CATEGORIA_FRANQUICIAS) or not await check_franquicia(ctx):
        await ctx.send(embed=embeds.crear_embed_error("No tienes una franquicia activa"))
        return
    nombre = f"Rifa {ctx.author.name} - {datetime.now().strftime('%d/%m')}"
    async with aiosqlite.connect(DB_PATH) as db:
        cursor = await db.execute('INSERT INTO rifas (nombre, premio, valor_premio, precio_boleto, total_boletos, estado) VALUES (?, ?, ?, ?, ?, "activa")', (nombre, premio, precio, precio, total))
        rifa_id = cursor.lastrowid
        await db.commit()
    embed = embeds.crear_embed_exito(f"✅ Rifa de franquicia creada!\nID: {rifa_id}\nPremio: {premio}\nPrecio: ${precio}\nTotal: {total} boletos")
    await ctx.send(embed=embed)
    for nivel, datos in ROLES_FRANQUICIA.items():
        if tiene_rol(ctx.author, datos['rol_id']):
            canal = bot.get_channel(datos['canal_id'])
            if canal:
                await canal.send(f"🎉 **Nueva rifa creada por {ctx.author.mention}**\nPremio: {premio}\nPrecio: ${precio}")

@bot.command(name="franquicia_stats")
async def cmd_franquicia_stats(ctx):
    if not await verificar_canal(ctx, CATEGORIA_FRANQUICIAS) or not await check_franquicia(ctx):
        await ctx.send(embed=embeds.crear_embed_error("No tienes una franquicia activa"))
        return
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        cursor = await db.execute('SELECT ventas_totales, comisiones_pendientes, comisiones_pagadas FROM distribuidores WHERE discord_id = ?', (str(ctx.author.id),))
        stats = await cursor.fetchone()
        if not stats:
            await ctx.send(embed=embeds.crear_embed_error("No hay estadísticas disponibles"))
            return
        ventas_totales, comisiones_pendientes, comisiones_pagadas = stats
    embed = discord.Embed(title="📊 Estadísticas de Franquicia", color=COLORS['primary'])
    embed.add_field(name="📈 Ventas totales", value=f"{ventas_totales}", inline=True)
    embed.add_field(name="⏳ Comisiones pendientes", value=f"${comisiones_pendientes:,} VP$", inline=True)
    embed.add_field(name="✅ Comisiones pagadas", value=f"${comisiones_pagadas:,} VP$", inline=True)
    await ctx.send(embed=embed)

# ============================================
# OTROS COMANDOS ÚTILES
# ============================================

@bot.command(name="canjear")
async def cmd_canjear(ctx, codigo: str):
    if not await verificar_canal(ctx):
        return
    codigo = codigo.lower()
    async with aiosqlite.connect(DB_PATH) as db:
        cursor = await db.execute('SELECT recompensa FROM codigos_promocionales WHERE codigo = ? AND activo = 1', (codigo,))
        codigo_data = await cursor.fetchone()
        if not codigo_data:
            await ctx.send(embed=embeds.crear_embed_error("Código inválido o expirado"))
            return
        cursor = await db.execute('SELECT * FROM codigos_canjeados WHERE codigo = ? AND usuario_id = ?', (codigo, str(ctx.author.id)))
        if await cursor.fetchone():
            await ctx.send(embed=embeds.crear_embed_error("Ya has canjeado este código"))
            return
        recompensa = codigo_data[0]
        await db.execute('INSERT INTO codigos_canjeados (codigo, usuario_id) VALUES (?, ?)', (codigo, str(ctx.author.id)))
        await db.execute('INSERT INTO usuarios_balance (discord_id, nombre, balance) VALUES (?, ?, ?) ON CONFLICT(discord_id) DO UPDATE SET balance = balance + ?', (str(ctx.author.id), ctx.author.name, recompensa, recompensa))
        await db.commit()
    await enviar_dm(str(ctx.author.id), "🎁 Código canjeado", f"Has canjeado el código `{codigo}` y recibido ${recompensa} VP$")
    await ctx.message.delete()
    await ctx.send(embed=embeds.crear_embed_exito(f"✅ Código canjeado. Revisa tu DM."))

@bot.command(name="crearcodigo")
async def cmd_crear_codigo(ctx, codigo: str, recompensa: int):
    if not await check_ceo(ctx):
        return
    codigo = codigo.lower()
    async with aiosqlite.connect(DB_PATH) as db:
        cursor = await db.execute('SELECT * FROM codigos_promocionales WHERE codigo = ?', (codigo,))
        if await cursor.fetchone():
            await ctx.send(embed=embeds.crear_embed_error("El código ya existe"))
            return
        await db.execute('INSERT INTO codigos_promocionales (codigo, recompensa, creador_id) VALUES (?, ?, ?)', (codigo, recompensa, str(ctx.author.id)))
        await db.commit()
    await ctx.message.delete()
    await ctx.send(embed=embeds.crear_embed_exito(f"Código `{codigo}` creado con {recompensa} VP$"))

@bot.command(name="borrarcodigo")
async def cmd_borrar_codigo(ctx, codigo: str):
    if not await check_ceo(ctx):
        return
    codigo = codigo.lower()
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute('DELETE FROM codigos_promocionales WHERE codigo = ?', (codigo,))
        await db.commit()
    await ctx.message.delete()
    await ctx.send(embed=embeds.crear_embed_exito(f"Código `{codigo}` eliminado"))

@bot.command(name="jackpot")
async def cmd_jackpot(ctx, base: int, porcentaje: int, id_rifa: int):
    if not await check_ceo(ctx):
        return
    global jackpot_activo, jackpot_base, jackpot_porcentaje, jackpot_rifa_id, jackpot_total
    if base <= 0 or porcentaje <= 0:
        await ctx.send(embed=embeds.crear_embed_error("Valores positivos"))
        return
    rifa = await bot.db.get_rifa_activa()
    if not rifa or rifa['id'] != id_rifa:
        await ctx.send(embed=embeds.crear_embed_error(f"Rifa ID {id_rifa} no activa"))
        return
    jackpot_activo = True
    jackpot_base = base
    jackpot_porcentaje = porcentaje
    jackpot_rifa_id = id_rifa
    jackpot_total = base
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute('INSERT OR REPLACE INTO jackpot (id, rifa_id, base, porcentaje, total, activo) VALUES (1, ?, ?, ?, ?, 1)', (id_rifa, base, porcentaje, base))
        await db.commit()
    await ctx.message.delete()
    await ctx.send(embed=embeds.crear_embed_exito(f"🎰 Jackpot activado!\nBase: ${base}\n{porcentaje}% de cada compra"))

@bot.command(name="jackpotreset")
async def cmd_jackpot_reset(ctx):
    if not await check_ceo(ctx):
        return
    global jackpot_activo, jackpot_total, jackpot_base, jackpot_porcentaje, jackpot_rifa_id
    jackpot_activo = False
    jackpot_total = 0
    jackpot_base = 0
    jackpot_porcentaje = 0
    jackpot_rifa_id = 0
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute('UPDATE jackpot SET activo = 0 WHERE id = 1')
        await db.commit()
    await ctx.message.delete()
    await ctx.send(embed=embeds.crear_embed_exito("🎰 Jackpot resetado"))

@bot.command(name="jackpotsortear")
async def cmd_jackpot_sortear(ctx, ganadores: int = 1):
    if not await check_ceo(ctx):
        return
    global jackpot_activo, jackpot_total
    if not jackpot_activo or jackpot_total == 0:
        await ctx.send(embed=embeds.crear_embed_error("No hay jackpot activo"))
        return
    if ganadores < 1 or ganadores > 10:
        await ctx.send(embed=embeds.crear_embed_error("1-10 ganadores"))
        return
    rifa_activa = await bot.db.get_rifa_activa()
    if not rifa_activa or rifa_activa['id'] != jackpot_rifa_id:
        await ctx.send(embed=embeds.crear_embed_error("La rifa del jackpot ya no está activa"))
        return
    async with aiosqlite.connect(DB_PATH) as db:
        cursor = await db.execute('SELECT comprador_id, comprador_nick FROM boletos WHERE rifa_id = ? AND estado = "pagado"', (jackpot_rifa_id,))
        boletos = await cursor.fetchall()
    if len(boletos) == 0:
        await ctx.send(embed=embeds.crear_embed_error("No hay boletos vendidos"))
        return
    premio_por_ganador = jackpot_total // ganadores
    ganadores_sel = random.sample(boletos, min(ganadores, len(boletos)))
    embed = discord.Embed(title="🎰 JACKPOT SORTEADO", description=f"Premio total: **${jackpot_total}**", color=COLORS['success'])
    for i, ganador in enumerate(ganadores_sel, 1):
        embed.add_field(name=f"🏆 Ganador #{i}", value=f"{ganador[1]} | ${premio_por_ganador}", inline=False)
        async with aiosqlite.connect(DB_PATH) as db2:
            await db2.execute('UPDATE usuarios_balance SET balance = balance + ? WHERE discord_id = ?', (premio_por_ganador, ganador[0]))
        await enviar_dm(ganador[0], "🎰 GANASTE EL JACKPOT!", f"Has ganado ${premio_por_ganador} VP$ del jackpot")
    await ctx.send(embed=embed)
    jackpot_activo = False
    jackpot_total = 0

@bot.command(name="puntosreset")
async def cmd_puntos_reset(ctx, usuario: discord.Member = None):
    if not await check_ceo(ctx):
        return
    async with aiosqlite.connect(DB_PATH) as db:
        if usuario:
            await db.execute('DELETE FROM puntos_revancha WHERE usuario_id = ?', (str(usuario.id),))
            await ctx.send(embed=embeds.crear_embed_exito(f"Puntos de {usuario.name} reseteados"))
        else:
            await db.execute('DELETE FROM puntos_revancha')
            await ctx.send(embed=embeds.crear_embed_exito("Todos los puntos reseteados"))
        await db.commit()
    await ctx.message.delete()

@bot.command(name="estadisticas")
async def cmd_estadisticas(ctx):
    if not await check_ceo(ctx):
        return
    async with aiosqlite.connect(DB_PATH) as db:
        cursor = await db.execute('SELECT COUNT(*) FROM rifas')
        total_rifas = (await cursor.fetchone())[0]
        cursor = await db.execute('SELECT COUNT(*) FROM boletos')
        total_boletos = (await cursor.fetchone())[0]
        cursor = await db.execute('SELECT SUM(precio_pagado) FROM boletos')
        total_recaudado = (await cursor.fetchone())[0] or 0
        cursor = await db.execute('SELECT COUNT(*) FROM clientes')
        total_clientes = (await cursor.fetchone())[0]
        cursor = await db.execute('SELECT SUM(balance) FROM usuarios_balance')
        total_vp = (await cursor.fetchone())[0] or 0
    embed = discord.Embed(title="📊 ESTADÍSTICAS GLOBALES", color=COLORS['primary'])
    embed.add_field(name="🎟️ Rifas", value=f"**{total_rifas}**", inline=True)
    embed.add_field(name="🎲 Boletos", value=f"**{total_boletos}**", inline=True)
    embed.add_field(name="💰 Recaudado", value=f"**${total_recaudado:,}**", inline=True)
    embed.add_field(name="👥 Clientes", value=f"**{total_clientes}**", inline=True)
    embed.add_field(name="💵 VP$ en circulación", value=f"**${total_vp:,}**", inline=True)
    await ctx.send(embed=embed)

@bot.command(name="auditoria")
async def cmd_auditoria(ctx):
    if not await check_ceo(ctx):
        return
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        cursor = await db.execute('SELECT tipo, monto, descripcion, fecha FROM transacciones ORDER BY fecha DESC LIMIT 20')
        transacciones = await cursor.fetchall()
    if not transacciones:
        await ctx.send(embed=embeds.crear_embed_info("Sin datos", "No hay transacciones registradas"))
        return
    embed = discord.Embed(title="📋 AUDITORÍA", color=COLORS['primary'])
    for t in transacciones[:15]:
        embed.add_field(name=f"{t['fecha'][:10]} - {t['tipo']}", value=f"${t['monto']} - {t['descripcion']}", inline=False)
    await ctx.send(embed=embed)

@bot.command(name="exportar")
async def cmd_exportar(ctx):
    if not await check_ceo(ctx):
        return
    fecha = datetime.now().strftime("%Y%m%d_%H%M%S")
    archivo = f"/tmp/vp_rifas_{fecha}.csv"
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        cursor = await db.execute('SELECT b.numero, r.nombre as rifa, b.comprador_nick, b.precio_pagado, b.fecha_compra FROM boletos b JOIN rifas r ON b.rifa_id = r.id ORDER BY b.fecha_compra DESC')
        boletos = await cursor.fetchall()
    with open(archivo, 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(['Número', 'Rifa', 'Comprador', 'Precio', 'Fecha'])
        for b in boletos:
            writer.writerow([b['numero'], b['rifa'], b['comprador_nick'], b['precio_pagado'], b['fecha_compra']])
    await ctx.author.send(file=discord.File(archivo))
    await ctx.message.delete()
    await ctx.send(embed=embeds.crear_embed_exito("✅ Datos exportados. Revisa tu DM."))

@bot.command(name="setcomision")
async def cmd_set_comision(ctx, porcentaje: int):
    if not await check_admin(ctx):
        return
    if porcentaje < 0 or porcentaje > 30:
        await ctx.send(embed=embeds.crear_embed_error("0-30%"))
        return
    global COMISION_VENDEDOR
    COMISION_VENDEDOR = porcentaje
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute('UPDATE config_global SET comision_vendedor = ? WHERE id = 1', (porcentaje,))
        await db.commit()
    await ctx.message.delete()
    await ctx.send(embed=embeds.crear_embed_exito(f"Comisión vendedores: {porcentaje}%"))

@bot.command(name="setnivel")
async def cmd_set_nivel(ctx, nivel: str = None, campo: str = None, valor: str = None):
    if not await check_ceo(ctx):
        return
    niveles_validos = ['BRONCE', 'PLATA', 'ORO', 'PLATINO', 'DIAMANTE', 'MASTER']
    if not nivel:
        await ctx.send("Uso: `!setnivel [nivel] [campo] [valor]`\nNiveles: BRONCE, PLATA, ORO, PLATINO, DIAMANTE, MASTER\nCampos: descuento, gratis_cada, gratis_cantidad, anticipo_horas, canal_vip, rifas_exclusivas")
        return
    nivel = nivel.upper()
    if nivel not in niveles_validos:
        await ctx.send(embed=embeds.crear_embed_error(f"Nivel inválido. Niveles: {', '.join(niveles_validos)}"))
        return
    campos_validos = {
        'descuento': 'descuento',
        'gratis_cada': 'boletos_gratis_por_cada',
        'gratis_cantidad': 'cantidad_boletos_gratis',
        'anticipo_horas': 'acceso_anticipado_horas',
        'canal_vip': 'canal_vip',
        'rifas_exclusivas': 'rifas_exclusivas'
    }
    if campo not in campos_validos:
        await ctx.send(embed=embeds.crear_embed_error(f"Campo inválido. Campos: {', '.join(campos_validos.keys())}"))
        return
    columna = campos_validos[campo]
    try:
        valor_int = int(valor)
    except:
        if campo in ['canal_vip', 'rifas_exclusivas']:
            valor_int = 1 if valor.lower() in ['si', 'true', '1', 'activo'] else 0
        else:
            await ctx.send(embed=embeds.crear_embed_error("Valor debe ser número"))
            return
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(f'UPDATE fidelizacion_config SET {columna} = ? WHERE nivel = ?', (valor_int, nivel))
        await db.commit()
    await ctx.message.delete()
    await ctx.send(embed=embeds.crear_embed_exito(f"Nivel {nivel}: {campo} = {valor_int}"))

@bot.command(name="reporte")
async def cmd_reporte(ctx):
    if not await check_admin(ctx):
        return
    rifa = await bot.db.get_rifa_activa()
    if not rifa:
        await ctx.send(embed=embeds.crear_embed_error("No hay rifa activa"))
        return
    vendidos = await bot.db.get_boletos_vendidos(rifa['id'])
    recaudado = vendidos * rifa['precio_boleto']
    embed = discord.Embed(title=f"📊 Reporte - {rifa['nombre']}", description=f"**Vendidos:** {vendidos}/{rifa['total_boletos']}\n**Recaudado:** ${recaudado}", color=COLORS['primary'])
    await ctx.send(embed=embed)

@bot.command(name="alertar")
async def cmd_alertar(ctx, *, mensaje: str):
    if not await check_admin(ctx):
        return
    embed = discord.Embed(title="📢 ALERTA DE RIFAS", description=mensaje, color=COLORS['primary'])
    embed.set_footer(text="VP Rifas • Responde en el canal de rifas")
    enviados = 0
    for member in ctx.guild.members:
        if not member.bot:
            try:
                await member.send(embed=embed)
                enviados += 1
                await asyncio.sleep(0.5)
            except:
                pass
    await ctx.message.delete()
    await ctx.send(embed=embeds.crear_embed_exito(f"✅ Mensaje enviado a {enviados} miembros"))

@bot.command(name="topgastadoresreset")
async def cmd_top_gastadores_reset(ctx):
    if not await check_ceo(ctx):
        return
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute('UPDATE fidelizacion SET gasto_total = 0, nivel = "BRONCE"')
        await db.commit()
    await ctx.message.delete()
    await ctx.send(embed=embeds.crear_embed_exito("Ranking de gastadores reseteado"))

@bot.command(name="resetcashback")
async def cmd_reset_cashback(ctx):
    if not await check_ceo(ctx):
        return
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute('UPDATE cashback SET cashback_acumulado = 0')
        await db.commit()
    await ctx.message.delete()
    await ctx.send(embed=embeds.crear_embed_exito("Cashback reseteados"))

@bot.command(name="rankingreset")
async def cmd_ranking_reset(ctx):
    if not await check_admin(ctx):
        return
    rifa_activa = await bot.db.get_rifa_activa()
    if not rifa_activa:
        await ctx.send(embed=embeds.crear_embed_error("No hay rifa activa"))
        return
    await reiniciar_ranking_rifa(rifa_activa['id'])
    await ctx.message.delete()
    await ctx.send(embed=embeds.crear_embed_exito(f"Ranking de la rifa #{rifa_activa['id']} reseteado"))

@bot.command(name="topcomprador")
async def cmd_top_comprador(ctx, id_rifa: int):
    if not await check_admin(ctx):
        return
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        cursor = await db.execute('SELECT comprador_nick, COUNT(*) as boletos FROM boletos WHERE rifa_id = ? GROUP BY comprador_id ORDER BY boletos DESC LIMIT 10', (id_rifa,))
        ranking = await cursor.fetchall()
        cursor = await db.execute('SELECT * FROM rifas WHERE id = ?', (id_rifa,))
        rifa = await cursor.fetchone()
    if not rifa:
        await ctx.send(embed=embeds.crear_embed_error(f"No existe rifa con ID {id_rifa}"))
        return
    if not ranking:
        await ctx.send(embed=embeds.crear_embed_info("Sin datos", f"No hay compras en la rifa #{id_rifa}"))
        return
    embed = discord.Embed(title=f"🏆 TOP COMPRADORES - Rifa #{id_rifa}", description=f"**Premio:** {rifa['premio']}", color=COLORS['primary'])
    for i, u in enumerate(ranking, 1):
        medalla = {1: "🥇", 2: "🥈", 3: "🥉"}.get(i, f"{i}.")
        embed.add_field(name=f"{medalla} {u['comprador_nick']}", value=f"[{u['boletos']} boletos]", inline=False)
    await ctx.send(embed=embed)

@bot.command(name="aumentarnumeros")
async def cmd_aumentar_numeros(ctx, cantidad: int):
    if not await check_admin(ctx):
        return
    rifa = await bot.db.get_rifa_activa()
    if not rifa:
        await ctx.send(embed=embeds.crear_embed_error("No hay rifa activa"))
        return
    if cantidad <= 0:
        await ctx.send(embed=embeds.crear_embed_error("Cantidad positiva"))
        return
    async with aiosqlite.connect(DB_PATH) as db:
        nuevo_total = rifa['total_boletos'] + cantidad
        await db.execute('UPDATE rifas SET total_boletos = ? WHERE id = ?', (nuevo_total, rifa['id']))
        await db.commit()
    await ctx.message.delete()
    await ctx.send(embed=embeds.crear_embed_exito(f"✅ Total: {nuevo_total} boletos"))

@bot.command(name="cancelarsorteo")
async def cmd_cancelar_sorteo(ctx):
    if not await check_admin(ctx):
        return
    global sorteo_en_curso, sorteo_cancelado
    sorteo_cancelado = True
    sorteo_en_curso = False
    await ctx.message.delete()
    await ctx.send(embed=embeds.crear_embed_exito("Sorteo cancelado"))

# ============================================
# PERSONALIZACIÓN DE PERFIL (comandos básicos)
# ============================================

@bot.command(name="perfil")
async def cmd_perfil(ctx, usuario: discord.Member = None):
    if not await verificar_canal(ctx):
        return
    target = usuario if usuario else ctx.author
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        cursor = await db.execute('SELECT * FROM perfiles_cache WHERE usuario_id = ?', (str(target.id),))
        perfil = await cursor.fetchone()
        cursor = await db.execute('SELECT balance FROM usuarios_balance WHERE discord_id = ?', (str(target.id),))
        balance_result = await cursor.fetchone()
        balance = balance_result[0] if balance_result else 0
        cursor = await db.execute('SELECT nivel, gasto_total FROM fidelizacion WHERE usuario_id = ?', (str(target.id),))
        fidelizacion = await cursor.fetchone()
        nivel = fidelizacion['nivel'] if fidelizacion else 'BRONCE'
        gasto_total = fidelizacion['gasto_total'] if fidelizacion else 0
        cursor = await db.execute('SELECT COUNT(*) FROM boletos WHERE comprador_id = ?', (str(target.id),))
        boletos_count = (await cursor.fetchone())[0]
    background_nombre = "Predeterminado"
    marco_nombre = "Predeterminado"
    badge_nombre = ""
    efecto_nombre = ""
    async with aiosqlite.connect(DB_PATH) as db:
        cursor = await db.execute('SELECT pu.item_id, pi.tipo, pi.nombre, pi.emoji FROM personalizacion_usuarios pu JOIN personalizacion_items pi ON pu.item_id = pi.id WHERE pu.usuario_id = ? AND pu.equipado = 1', (str(target.id),))
        equipados = await cursor.fetchall()
        for eq in equipados:
            if eq[1] == 'background':
                background_nombre = f"{eq[3]} {eq[2]}"
            elif eq[1] == 'marco':
                marco_nombre = f"{eq[3]} {eq[2]}"
            elif eq[1] == 'badge':
                badge_nombre = f"{eq[3]} {eq[2]}"
            elif eq[1] == 'efecto':
                efecto_nombre = f"{eq[3]} {eq[2]}"
    titulo = perfil['titulo_personalizado'] if perfil and perfil['titulo_personalizado'] else ""
    biografia = perfil['biografia'] if perfil and perfil['biografia'] else "No hay biografía"
    color_embed = perfil['color_embed'] if perfil and perfil['color_embed'] else COLORS['primary']
    embed = discord.Embed(title=f"🎨 Perfil de {target.name}", description=f"**{titulo}**" if titulo else "", color=color_embed)
    if badge_nombre:
        embed.set_thumbnail(url=target.avatar.url if target.avatar else target.default_avatar.url)
        embed.add_field(name="🏅 Insignia", value=badge_nombre, inline=False)
    embed.add_field(name="🎭 Marco", value=marco_nombre, inline=True)
    embed.add_field(name="🖼️ Fondo", value=background_nombre, inline=True)
    if efecto_nombre:
        embed.add_field(name="✨ Efecto", value=efecto_nombre, inline=True)
    embed.add_field(name="🏆 Nivel", value=nivel, inline=True)
    embed.add_field(name="💰 Balance", value=f"${balance:,} VP$", inline=True)
    embed.add_field(name="🎟️ Boletos comprados", value=str(boletos_count), inline=True)
    embed.add_field(name="💵 Gasto total", value=f"${gasto_total:,} VP$", inline=True)
    embed.add_field(name="📝 Biografía", value=biografia, inline=False)
    await ctx.send(embed=embed)

@bot.command(name="tienda_perfil")
async def cmd_tienda_perfil(ctx, categoria: str = None):
    if not await verificar_canal(ctx):
        return
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        if categoria:
            cursor = await db.execute('SELECT * FROM personalizacion_items WHERE tipo = ? AND activo = 1 ORDER BY orden ASC', (categoria,))
        else:
            cursor = await db.execute('SELECT * FROM personalizacion_items WHERE activo = 1 ORDER BY tipo, orden ASC')
        items = await cursor.fetchall()
    if not items:
        await ctx.send(embed=embeds.crear_embed_info("Tienda vacía", "No hay items disponibles"))
        return
    embed = discord.Embed(title="🛍️ TIENDA DE PERSONALIZACIÓN", color=COLORS['primary'])
    for item in items:
        rareza_emoji = {"comun": "⚪", "rara": "🟢", "epica": "🟣", "legendaria": "🔴", "especial": "✨"}.get(item['rareza'], "⚪")
        precio_texto = "GRATIS" if item['precio'] == 0 else f"${item['precio']:,} VP$"
        embed.add_field(name=f"{rareza_emoji} {item['nombre']} ({item['tipo']})", value=f"{item['descripcion']}\n💰 {precio_texto}\n📝 Usa `!comprar_perfil {item['id']}`", inline=False)
    await ctx.send(embed=embed)

@bot.command(name="comprar_perfil")
async def cmd_comprar_perfil(ctx, item_id: int):
    if not await verificar_canal(ctx):
        return
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        cursor = await db.execute('SELECT * FROM personalizacion_items WHERE id = ? AND activo = 1', (item_id,))
        item = await cursor.fetchone()
        if not item:
            await ctx.send(embed=embeds.crear_embed_error("Item no encontrado"))
            return
        cursor = await db.execute('SELECT * FROM personalizacion_usuarios WHERE usuario_id = ? AND item_id = ?', (str(ctx.author.id), item_id))
        ya_tiene = await cursor.fetchone()
        if ya_tiene:
            await ctx.send(embed=embeds.crear_embed_error("Ya tienes este item"))
            return
        if item['precio'] > 0:
            cursor = await db.execute('SELECT balance FROM usuarios_balance WHERE discord_id = ?', (str(ctx.author.id),))
            result = await cursor.fetchone()
            balance = result[0] if result else 0
            if balance < item['precio']:
                await ctx.send(embed=embeds.crear_embed_error(f"Necesitas ${item['precio']:,} VP$"))
                return
            await db.execute('UPDATE usuarios_balance SET balance = balance - ? WHERE discord_id = ?', (item['precio'], str(ctx.author.id)))
        await db.execute('INSERT INTO personalizacion_usuarios (usuario_id, item_id) VALUES (?, ?)', (str(ctx.author.id), item_id))
        await db.commit()
    embed = discord.Embed(title="✅ Compra realizada", description=f"Has comprado **{item['nombre']}** por {f'${item['precio']:,} VP$' if item['precio'] > 0 else 'GRATIS'}", color=COLORS['success'])
    embed.add_field(name="🎨 Equipar", value=f"Usa `!equipar {item_id}` para equipar este item", inline=False)
    await ctx.send(embed=embed)

@bot.command(name="equipar")
async def cmd_equipar(ctx, item_id: int):
    if not await verificar_canal(ctx):
        return
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        cursor = await db.execute('SELECT * FROM personalizacion_items WHERE id = ?', (item_id,))
        item = await cursor.fetchone()
        if not item:
            await ctx.send(embed=embeds.crear_embed_error("Item no encontrado"))
            return
        cursor = await db.execute('SELECT * FROM personalizacion_usuarios WHERE usuario_id = ? AND item_id = ?', (str(ctx.author.id), item_id))
        tiene = await cursor.fetchone()
        if not tiene:
            await ctx.send(embed=embeds.crear_embed_error("No tienes este item"))
            return
        cursor = await db.execute('SELECT * FROM personalizacion_usuarios WHERE usuario_id = ? AND equipado = 1 AND item_id IN (SELECT id FROM personalizacion_items WHERE tipo = ?)', (str(ctx.author.id), item['tipo']))
        equipado_actual = await cursor.fetchone()
        if equipado_actual:
            await db.execute('UPDATE personalizacion_usuarios SET equipado = 0 WHERE id = ?', (equipado_actual['id'],))
        await db.execute('UPDATE personalizacion_usuarios SET equipado = 1 WHERE id = ?', (tiene['id'],))
        await db.execute('INSERT INTO perfiles_cache (usuario_id, background_id, marco_id, badge_id, efecto_id, ultima_actualizacion) VALUES (?, (SELECT id FROM personalizacion_usuarios pu JOIN personalizacion_items pi ON pu.item_id = pi.id WHERE pu.usuario_id = ? AND pi.tipo = "background" AND pu.equipado = 1), (SELECT id FROM personalizacion_usuarios pu JOIN personalizacion_items pi ON pu.item_id = pi.id WHERE pu.usuario_id = ? AND pi.tipo = "marco" AND pu.equipado = 1), (SELECT id FROM personalizacion_usuarios pu JOIN personalizacion_items pi ON pu.item_id = pi.id WHERE pu.usuario_id = ? AND pi.tipo = "badge" AND pu.equipado = 1), (SELECT id FROM personalizacion_usuarios pu JOIN personalizacion_items pi ON pu.item_id = pi.id WHERE pu.usuario_id = ? AND pi.tipo = "efecto" AND pu.equipado = 1), CURRENT_TIMESTAMP) ON CONFLICT(usuario_id) DO UPDATE SET background_id = excluded.background_id, marco_id = excluded.marco_id, badge_id = excluded.badge_id, efecto_id = excluded.efecto_id, ultima_actualizacion = CURRENT_TIMESTAMP', (str(ctx.author.id), str(ctx.author.id), str(ctx.author.id), str(ctx.author.id), str(ctx.author.id)))
        await db.commit()
    embed = discord.Embed(title="✅ Item equipado", description=f"Has equipado **{item['nombre']}**", color=COLORS['success'])
    await ctx.send(embed=embed)

@bot.command(name="perfil_set")
async def cmd_perfil_set(ctx, campo: str, *, valor: str):
    if not await verificar_canal(ctx):
        return
    if campo not in ['titulo', 'bio', 'color']:
        await ctx.send(embed=embeds.crear_embed_error("Campos válidos: titulo, bio, color"))
        return
    async with aiosqlite.connect(DB_PATH) as db:
        cursor = await db.execute('SELECT * FROM perfiles_cache WHERE usuario_id = ?', (str(ctx.author.id),))
        perfil = await cursor.fetchone()
        if campo == 'titulo':
            if len(valor) > 50:
                await ctx.send(embed=embeds.crear_embed_error("El título no puede superar los 50 caracteres"))
                return
            if perfil:
                await db.execute('UPDATE perfiles_cache SET titulo_personalizado = ?, ultima_actualizacion = CURRENT_TIMESTAMP WHERE usuario_id = ?', (valor, str(ctx.author.id)))
            else:
                await db.execute('INSERT INTO perfiles_cache (usuario_id, titulo_personalizado) VALUES (?, ?)', (str(ctx.author.id), valor))
            mensaje = f"Título actualizado a: **{valor}**"
        elif campo == 'bio':
            if len(valor) > 200:
                await ctx.send(embed=embeds.crear_embed_error("La biografía no puede superar los 200 caracteres"))
                return
            if perfil:
                await db.execute('UPDATE perfiles_cache SET biografia = ?, ultima_actualizacion = CURRENT_TIMESTAMP WHERE usuario_id = ?', (valor, str(ctx.author.id)))
            else:
                await db.execute('INSERT INTO perfiles_cache (usuario_id, biografia) VALUES (?, ?)', (str(ctx.author.id), valor))
            mensaje = f"Biografía actualizada"
        elif campo == 'color':
            try:
                color_val = int(valor.replace('#', ''), 16)
            except:
                await ctx.send(embed=embeds.crear_embed_error("Color inválido. Usa formato HEX (ej: #FFD700)"))
                return
            if perfil:
                await db.execute('UPDATE perfiles_cache SET color_embed = ?, ultima_actualizacion = CURRENT_TIMESTAMP WHERE usuario_id = ?', (color_val, str(ctx.author.id)))
            else:
                await db.execute('INSERT INTO perfiles_cache (usuario_id, color_embed) VALUES (?, ?)', (str(ctx.author.id), color_val))
            mensaje = f"Color actualizado a {valor}"
        await db.commit()
    embed = discord.Embed(title="✅ Perfil actualizado", description=mensaje, color=COLORS['success'])
    await ctx.send(embed=embed)

# ============================================
# EJECUCIÓN
# ============================================

if __name__ == "__main__":
    try:
        if not config.BOT_TOKEN:
            print("❌ ERROR: No se encontró BOT_TOKEN en config.py")
            sys.exit(1)
        bot.run(config.BOT_TOKEN)
    except Exception as e:
        logger.error(f"Error fatal: {e}")
        traceback.print_exc()
