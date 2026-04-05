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
import time
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
evento_oferta_activa = False
evento_oferta_porcentaje = 0

# Configuración de sistemas
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
                name=f"{PREFIX}ayuda | VP Rifas v{VERSION}"
            )
        )
        self.db = Database()
        self.update_channel_id = 1483378335831560202
        self.reconnecting = False
        self.ultimo_heartbeat = datetime.now()
        self.volumen_montado = VOLUME_MOUNTED
        
    async def setup_hook(self):
        logger.info(f"🚀 Iniciando VP Rifas Bot v{VERSION}...")
        
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
        async with aiosqlite.connect(DB_PATH) as db:
            cursor = await db.execute('SELECT key, value FROM config_global')
            configs = await cursor.fetchall()
            for key, value in configs:
                if key == 'tasa_compra':
                    global tasa_compra
                    tasa_compra = float(value)
                elif key == 'tasa_venta':
                    global tasa_venta
                    tasa_venta = float(value)
    
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
            
            # ===== SISTEMA DE REFERIDOS =====
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
            
            await db.execute('''
                INSERT OR IGNORE INTO referidos_config (id, porcentaje_comision, porcentaje_descuento)
                VALUES (1, 10, 10)
            ''')
            
            # ===== SISTEMA DE FIDELIZACIÓN =====
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
            
            niveles_config = [
                ('BRONCE', 0, 499999, 0, 0, 0, 0, 0, 0),
                ('PLATA', 500000, 999999, 5, 0, 0, 0, 0, 0),
                ('ORO', 1000000, 2499999, 10, 10, 2, 0, 0, 0),
                ('PLATINO', 2500000, 4999999, 15, 10, 2, 24, 0, 0),
                ('DIAMANTE', 5000000, 9999999, 20, 10, 3, 24, 1, 0),
                ('MASTER', 10000000, None, 25, 10, 4, 48, 1, 1)
            ]
            
            for nivel in niveles_config:
                await db.execute('''
                    INSERT OR REPLACE INTO fidelizacion_config 
                    (nivel, gasto_minimo, gasto_maximo, descuento, boletos_gratis_por_cada, 
                     cantidad_boletos_gratis, acceso_anticipado_horas, canal_vip, rifas_exclusivas)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''', nivel)
            
            # ===== SISTEMA DE CASHBACK =====
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
            
            await db.execute('''
                INSERT OR IGNORE INTO cashback_config (id, porcentaje, dia_pago)
                VALUES (1, 10, 'LUNES')
            ''')
            
            # ===== SISTEMA DE CAJAS MISTERIOSAS =====
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
            
            for caja in cajas_default:
                await db.execute('''
                    INSERT OR IGNORE INTO cajas (tipo, nombre, precio, premios, probabilidades, activo)
                    VALUES (?, ?, ?, ?, ?, ?)
                ''', caja)
            
            # ===== SISTEMA DE DISTRIBUIDORES =====
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
            
            for nivel in niveles_dist:
                await db.execute('''
                    INSERT OR IGNORE INTO distribuidores_niveles 
                    (nivel, nombre, comision_base, comision_extra, descuento_mayorista, productos_acceso, ventas_requeridas, inversion_requerida)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                ''', nivel)
            
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
            
            for prod in productos_default:
                await db.execute('''
                    INSERT OR IGNORE INTO productos (nombre, descripcion, precio_normal, precio_mayorista, nivel_minimo, stock, activo)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                ''', prod)
            
            # ===== SISTEMA DE MISIONES DIARIAS =====
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
            
            for mision in misiones_default:
                await db.execute('''
                    INSERT OR IGNORE INTO misiones (nombre, descripcion, requisito, valor_requisito, recompensa, tipo, activo)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                ''', mision)
            
            # ===== MISIONES SEMANALES =====
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
            
            misiones_semanales_default = [
                ('Comprador Semanal', 'Compra 50 boletos', '🎲', 'compras', 'cantidad', 50, 25000, 'epica', None, 0, 0, 1),
                ('Cajero Semanal', 'Abre 30 cajas', '📦', 'cajas', 'cantidad', 30, 15000, 'epica', None, 0, 0, 1),
                ('Referidor Semanal', '5 referidos nuevos', '👥', 'referidos', 'cantidad', 5, 50000, 'legendaria', None, 0, 0, 1),
                ('Subastador', 'Gana 3 subastas', '🎫', 'subastas', 'cantidad', 3, 30000, 'epica', 'Pujador', 0, 0, 1),
                ('Comerciante', 'Vende 10 boletos en marketplace', '🛒', 'marketplace', 'cantidad', 10, 20000, 'rara', None, 0, 0, 1),
                ('Generoso', 'Envía 5 regalos', '🎁', 'regalos', 'cantidad', 5, 10000, 'comun', None, 0, 0, 1)
            ]
            
            for mision in misiones_semanales_default:
                await db.execute('''
                    INSERT OR IGNORE INTO misiones_semanales 
                    (nombre, descripcion, emoji, categoria, requisito_tipo, requisito_valor, 
                     recompensa_vp, recompensa_caja_tipo, recompensa_titulo, semana, año, activo)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''', mision)
            
            # ===== BANCO VP =====
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
            
            for prod in productos_banco:
                await db.execute('''
                    INSERT OR IGNORE INTO banco_productos (nombre, duracion_dias, interes_porcentaje, monto_minimo, monto_maximo, penalizacion_retiro, activo)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                ''', prod)
            
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
            
            await db.execute('''
                INSERT OR IGNORE INTO banco_config (id, tasa_compra, tasa_venta)
                VALUES (1, 0.9, 1.1)
            ''')
            
            await db.execute('''
                CREATE TABLE IF NOT EXISTS banco_tasas_historial (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    tasa_compra REAL,
                    tasa_venta REAL,
                    fecha TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    modificado_por TEXT
                )
            ''')
            
            # ===== PAGOS PENDIENTES =====
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
            
            # ===== TICKETS =====
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
            
            # ===== JACKPOT =====
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
            
            # ===== EVENTOS ACTIVOS (PERSISTENCIA) =====
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
            
            await db.execute('''
                INSERT OR IGNORE INTO eventos_activos (id) VALUES (1)
            ''')
            
            # ===== EVENTOS PROGRAMADOS =====
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
            
            # ===== CONFIGURACIÓN GLOBAL =====
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
                ('comision_vendedor', '10', '% comisión para vendedores', None),
                ('comision_distribuidor_base', '5', '% base distribuidores', None),
                ('cashback_porcentaje', '10', '% cashback normal', None),
                ('referidos_porcentaje', '10', '% comisión referidos', None),
                ('referidos_descuento', '10', '% descuento por referido', None),
                ('precio_boleto_default', '25000', 'Precio por defecto', None),
                ('max_boletos_por_compra', '50', 'Límite por transacción', None),
                ('min_balance_retiro', '10000', 'Mínimo para retirar', None),
                ('jackpot_porcentaje_default', '5', '% por compra al jackpot', None),
                ('mantenimiento', 'false', 'Modo mantenimiento', None),
                ('tasa_compra', '0.9', 'Tasa de cambio NG$ → VP$', None),
                ('tasa_venta', '1.1', 'Tasa de cambio VP$ → NG$', None),
                ('cambio_ng_minimo', '100000', 'Monto mínimo para cambiar NG$', None),
                ('cambio_ng_maximo', '10000000', 'Monto máximo para cambiar NG$', None),
                ('ticket_auto_cerrar_segundos', '10', 'Segundos antes de cerrar ticket', None),
                ('categoria_tickets', '0', 'ID de la categoría de tickets', None)
            ]
            
            for key, value, desc, _ in configs_default:
                await db.execute('''
                    INSERT OR IGNORE INTO config_global (key, value, descripcion)
                    VALUES (?, ?, ?)
                ''', (key, value, desc))
            
            # ===== PUNTOS REVANCHA =====
            await db.execute('''
                CREATE TABLE IF NOT EXISTS puntos_revancha (
                    usuario_id TEXT PRIMARY KEY,
                    puntos INTEGER DEFAULT 0,
                    ultima_actualizacion TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            ''')
            
            # ===== CÓDIGOS PROMOCIONALES =====
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
            
            # ===== GANADORES HISTÓRICOS =====
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
            
            # ===== SISTEMA DE SUBASTAS =====
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
            
            # ===== SISTEMA DE LOGROS =====
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
            
            for logro in logros_default:
                await db.execute('''
                    INSERT OR IGNORE INTO logros 
                    (nombre, descripcion, emoji, categoria, condicion_tipo, condicion_valor, 
                     recompensa_vp, recompensa_rol_id, recompensa_titulo, orden, activo)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''', logro)
            
            # ===== SISTEMA DE NOTIFICACIONES =====
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
            
            # ===== SISTEMA DE REGALOS =====
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
            
            await db.execute('''
                INSERT OR IGNORE INTO regalos_config (id) VALUES (1)
            ''')
            
            # ===== SISTEMA DE MARKETPLACE =====
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
            
            await db.execute('''
                INSERT OR IGNORE INTO marketplace_config (id) VALUES (1)
            ''')
            
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
            
            # ===== SISTEMA DE PERSONALIZACIÓN DE PERFIL =====
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
            
            items_personalizacion = [
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
            
            for item in items_personalizacion:
                await db.execute('''
                    INSERT OR IGNORE INTO personalizacion_items 
                    (tipo, nombre, descripcion, emoji, precio, imagen_url, rareza, exclusivo, activo, orden)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''', item)
            
            # ===== SISTEMA DE RULETA DIARIA =====
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
            
            premios_default = json.dumps([100, 500, 1000, 5000, 10000, 50000, 100000, 500000])
            probs_default = json.dumps([30, 25, 20, 12, 8, 3, 1.5, 0.5])
            colores_default = json.dumps(["⚪", "🟢", "🟢", "🟡", "🟡", "🟠", "🔴", "🟣"])
            descs_default = json.dumps([
                "😅 Casi...", "👍 No está mal", "🙂 Bien", "🎉 Buena suerte",
                "🎊 Excelente", "🤯 Increíble", "💎 ESPECTACULAR", "👑 JACKPOT"
            ])
            
            await db.execute('''
                INSERT OR IGNORE INTO ruleta_config 
                (id, activo, cooldown_horas, premios, probabilidades, colores, descripciones)
                VALUES (1, 1, 24, ?, ?, ?, ?)
            ''', (premios_default, probs_default, colores_default, descs_default))
            
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
            
            await db.execute('''
                INSERT OR IGNORE INTO ruleta_stats (id) VALUES (1)
            ''')
            
            # ===== SISTEMA DE APUESTAS/PREDICCIONES =====
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
            
            await db.execute('''
                INSERT OR IGNORE INTO apuestas_config (id) VALUES (1)
            ''')
            
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
            
            await db.commit()
        
        logger.info("✅ Todas las tablas inicializadas correctamente")
    
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
    
    @tasks.loop(minutes=5)
    async def verificar_eventos_task(self):
        async with aiosqlite.connect(DB_PATH) as db:
            cursor = await db.execute('SELECT * FROM eventos_programados WHERE activo = 1 AND fecha_inicio <= datetime("now") AND fecha_fin >= datetime("now")')
            eventos = await cursor.fetchall()
            for evento in eventos:
                if evento[2] == '2x1':
                    global evento_2x1
                    evento_2x1 = True
                elif evento[2] == 'cashback_doble':
                    global evento_cashback_doble
                    evento_cashback_doble = True
                elif evento[2] == 'oferta':
                    global evento_oferta_activa, evento_oferta_porcentaje
                    evento_oferta_activa = True
                    evento_oferta_porcentaje = evento[3]
            
            cursor = await db.execute('SELECT * FROM eventos_programados WHERE activo = 1 AND fecha_fin < datetime("now")')
            eventos_vencidos = await cursor.fetchall()
            for evento in eventos_vencidos:
                await db.execute('UPDATE eventos_programados SET activo = 0 WHERE id = ?', (evento[0],))
                if evento[2] == '2x1':
                    evento_2x1 = False
                elif evento[2] == 'cashback_doble':
                    evento_cashback_doble = False
                elif evento[2] == 'oferta':
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
        
        await self.enviar_log_sistema("💾 BACKUP AUTOMÁTICO", f"Backup creado: {backup_file}\nBackups totales: {len(backups)}")
    
    @tasks.loop(hours=168)
    async def reset_misiones_semanales_task(self):
        semana_actual = datetime.now().isocalendar()[1]
        año_actual = datetime.now().year
        async with aiosqlite.connect(DB_PATH) as db:
            await db.execute('DELETE FROM misiones_semanales_progreso')
            await db.execute('UPDATE misiones_semanales SET semana = ?, año = ?', (semana_actual, año_actual))
            await db.commit()
        await self.enviar_log_sistema("📋 MISIONES SEMANALES", "Las misiones semanales se han reiniciado")
    
    async def finalizar_subasta(self, subasta_id):
        async with aiosqlite.connect(DB_PATH) as db:
            db.row_factory = aiosqlite.Row
            cursor = await db.execute('SELECT * FROM subastas WHERE id = ?', (subasta_id,))
            subasta = await cursor.fetchone()
            if not subasta:
                return
            
            cursor = await db.execute('SELECT * FROM pujas WHERE subasta_id = ? ORDER BY monto DESC LIMIT 1', (subasta_id,))
            puja_ganadora = await cursor.fetchone()
            
            if puja_ganadora:
                await db.execute('UPDATE subastas SET estado = "finalizada", ganador_id = ?, ganador_nick = ?, precio_actual = ? WHERE id = ?',
                               (puja_ganadora['usuario_id'], puja_ganadora['usuario_nick'], puja_ganadora['monto'], subasta_id))
                
                await self.crear_ticket_subasta(subasta_id, puja_ganadora['usuario_id'], puja_ganadora['usuario_nick'], subasta['item_nombre'], puja_ganadora['monto'])
                
                canal = self.get_channel(subasta['canal_id'])
                if canal:
                    try:
                        msg = await canal.fetch_message(subasta['mensaje_id'])
                        embed = msg.embeds[0] if msg.embeds else None
                        if embed:
                            embed.color = COLORS['success']
                            embed.add_field(name="🏆 GANADOR", value=f"{puja_ganadora['usuario_nick']} por ${puja_ganadora['monto']:,} VP$", inline=False)
                            await msg.edit(embed=embed)
                    except:
                        pass
                
                await self.enviar_notificacion(puja_ganadora['usuario_id'], 'subasta_ganada', '🎉 Ganaste una subasta', 
                                              f'Ganaste la subasta de {subasta["item_nombre"]} por ${puja_ganadora["monto"]:,} VP$')
            else:
                await db.execute('UPDATE subastas SET estado = "cancelada" WHERE id = ?', (subasta_id,))
            
            await db.commit()
    
    async def crear_ticket_subasta(self, subasta_id, usuario_id, usuario_nick, item_nombre, monto):
        guild = self.get_guild(config.GUILD_ID)
        if not guild:
            return
        
        categoria = guild.get_channel(CATEGORIA_TICKETS)
        if not categoria:
            return
        
        overwrites = {
            guild.default_role: discord.PermissionOverwrite(read_messages=False),
            guild.get_member(int(usuario_id)): discord.PermissionOverwrite(read_messages=True, send_messages=True),
        }
        
        for rol_id in [ROLES['CEO'], ROLES['DIRECTOR']]:
            rol = guild.get_role(rol_id)
            if rol:
                overwrites[rol] = discord.PermissionOverwrite(read_messages=True, send_messages=True)
        
        ticket_channel = await categoria.create_text_channel(f"ticket-subasta-{subasta_id}", overwrites=overwrites)
        
        embed = discord.Embed(
            title="🎉 ¡FELICIDADES! 🎉",
            description=f"Has ganado la subasta #{subasta_id}",
            color=COLORS['success']
        )
        embed.add_field(name="🏆 Premio", value=item_nombre, inline=True)
        embed.add_field(name="💰 Monto pagado", value=f"${monto:,} VP$", inline=True)
        embed.add_field(name="📦 Entrega", value="Un staff te entregará tu premio en breve.", inline=False)
        embed.set_footer(text="Para cerrar este ticket, un staff debe usar: !ticketcerrar")
        
        await ticket_channel.send(f"{guild.get_member(int(usuario_id)).mention}", embed=embed)
        
        async with aiosqlite.connect(DB_PATH) as db:
            await db.execute('INSERT INTO tickets_subasta (subasta_id, usuario_id, canal_id) VALUES (?, ?, ?)',
                           (subasta_id, usuario_id, str(ticket_channel.id)))
            await db.commit()
    
    async def enviar_notificacion(self, usuario_id, tipo, titulo, mensaje):
        async with aiosqlite.connect(DB_PATH) as db:
            cursor = await db.execute('SELECT * FROM notificaciones_preferencias WHERE usuario_id = ?', (usuario_id,))
            pref = await cursor.fetchone()
            
            if pref:
                hora_actual = datetime.now().hour
                if hora_actual < pref[8] or hora_actual > pref[9]:
                    return
                
                if tipo == 'nueva_rifa' and not pref[1]:
                    return
                elif tipo == 'subasta' and not pref[2]:
                    return
                elif tipo == 'logro' and not pref[3]:
                    return
                elif tipo == 'cashback' and not pref[4]:
                    return
                elif tipo == 'inversion' and not pref[5]:
                    return
                elif tipo == 'ticket' and not pref[6]:
                    return
                elif tipo == 'promocion' and not pref[7]:
                    return
            
            await db.execute('INSERT INTO notificaciones_historial (usuario_id, tipo, titulo, mensaje) VALUES (?, ?, ?, ?)',
                           (usuario_id, tipo, titulo, mensaje))
            await db.commit()
        
        try:
            usuario = await self.fetch_user(int(usuario_id))
            embed = discord.Embed(title=titulo, description=mensaje, color=COLORS['info'])
            await usuario.send(embed=embed)
        except:
            pass
    
    async def verificar_logros(self, usuario_id, usuario_nick, tipo, valor=1):
        async with aiosqlite.connect(DB_PATH) as db:
            db.row_factory = aiosqlite.Row
            
            cursor = await db.execute('SELECT * FROM usuarios_stats WHERE usuario_id = ?', (usuario_id,))
            stats = await cursor.fetchone()
            
            if not stats:
                await db.execute('INSERT INTO usuarios_stats (usuario_id) VALUES (?)', (usuario_id,))
                stats = await db.execute('SELECT * FROM usuarios_stats WHERE usuario_id = ?', (usuario_id,))
                stats = await cursor.fetchone()
            
            if tipo == 'compra':
                nuevas_compras = stats['total_compras'] + valor
                nuevo_gastado = stats['total_gastado'] + valor
                await db.execute('UPDATE usuarios_stats SET total_compras = ?, total_gastado = ?, fecha_actualizacion = CURRENT_TIMESTAMP WHERE usuario_id = ?',
                               (nuevas_compras, nuevo_gastado, usuario_id))
            elif tipo == 'caja':
                nuevas_cajas = stats['total_cajas_abiertas'] + valor
                await db.execute('UPDATE usuarios_stats SET total_cajas_abiertas = ?, fecha_actualizacion = CURRENT_TIMESTAMP WHERE usuario_id = ?',
                               (nuevas_cajas, usuario_id))
            elif tipo == 'referido':
                nuevos_referidos = stats['total_referidos'] + valor
                await db.execute('UPDATE usuarios_stats SET total_referidos = ?, fecha_actualizacion = CURRENT_TIMESTAMP WHERE usuario_id = ?',
                               (nuevos_referidos, usuario_id))
            elif tipo == 'subasta':
                nuevas_subastas = stats['total_subastas_ganadas'] + valor
                await db.execute('UPDATE usuarios_stats SET total_subastas_ganadas = ?, fecha_actualizacion = CURRENT_TIMESTAMP WHERE usuario_id = ?',
                               (nuevas_subastas, usuario_id))
            
            cursor = await db.execute('SELECT * FROM logros WHERE activo = 1')
            logros = await cursor.fetchall()
            
            for logro in logros:
                cursor = await db.execute('SELECT * FROM usuarios_logros WHERE usuario_id = ? AND logro_id = ?', (usuario_id, logro['id']))
                ya_tiene = await cursor.fetchone()
                
                if ya_tiene:
                    continue
                
                cumplido = False
                if logro['condicion_tipo'] == 'cantidad':
                    if logro['categoria'] == 'compras':
                        cumplido = stats['total_compras'] >= logro['condicion_valor']
                    elif logro['categoria'] == 'cajas':
                        cumplido = stats['total_cajas_abiertas'] >= logro['condicion_valor']
                    elif logro['categoria'] == 'referidos':
                        cumplido = stats['total_referidos'] >= logro['condicion_valor']
                    elif logro['categoria'] == 'subastas':
                        cumplido = stats['total_subastas_ganadas'] >= logro['condicion_valor']
                elif logro['condicion_tipo'] == 'total_gastado':
                    cumplido = stats['total_gastado'] >= logro['condicion_valor']
                elif logro['condicion_tipo'] == 'racha':
                    if logro['categoria'] == 'racha':
                        cursor_racha = await db.execute('SELECT mejor_racha FROM rachas WHERE usuario_id = ?', (usuario_id,))
                        racha_data = await cursor_racha.fetchone()
                        if racha_data:
                            cumplido = racha_data[0] >= logro['condicion_valor']
                
                if cumplido:
                    await db.execute('INSERT INTO usuarios_logros (usuario_id, logro_id, fecha_desbloqueo, notificado) VALUES (?, ?, CURRENT_TIMESTAMP, 0)',
                                   (usuario_id, logro['id']))
                    
                    if logro['recompensa_vp'] > 0:
                        await db.execute('UPDATE usuarios_balance SET balance = balance + ? WHERE discord_id = ?',
                                       (logro['recompensa_vp'], usuario_id))
                    
                    if logro['recompensa_rol_id']:
                        guild = self.get_guild(config.GUILD_ID)
                        if guild:
                            member = guild.get_member(int(usuario_id))
                            rol = guild.get_role(int(logro['recompensa_rol_id']))
                            if member and rol and rol not in member.roles:
                                await member.add_roles(rol)
                    
                    await self.enviar_notificacion(usuario_id, 'logro', f"🏆 Logro desbloqueado: {logro['nombre']}",
                                                  f"Has desbloqueado el logro {logro['emoji']} **{logro['nombre']}**\nRecompensa: ${logro['recompensa_vp']:,} VP$")
            
            await db.commit()
    
    async def on_ready(self):
        logger.info(f"✅ Bot conectado como {self.user}")
        logger.info(f"🌐 En {len(self.guilds)} servidores")
        logger.info(f"📦 VP Rifas Bot v{VERSION}")
        
        if self.volumen_montado:
            logger.info("💾 Volumen persistente activo")
        
        global reconnect_attempts
        reconnect_attempts = 0
        self.reconnecting = False
        
        await self.enviar_log_sistema(
            "🟢 **BOT INICIADO**", 
            f"VP Rifas Bot v{VERSION}\nServidores: {len(self.guilds)}\nVolumen: {'✅' if self.volumen_montado else '❌'}"
        )
    
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
            embed = discord.Embed(
                title=titulo,
                description=descripcion,
                color=0x0099FF,
                timestamp=datetime.now()
            )
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
        await self.enviar_log_sistema(
            "💓 **HEARTBEAT**", 
            f"Activo por {int(horas)}h {int(minutos)}m\nVersión: {VERSION}\nVolumen: {'✅' if self.volumen_montado else '❌'}"
        )

bot = VPRifasBot()

# ============================================
# FUNCIONES AUXILIARES GLOBALES
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
        embed = discord.Embed(
            title=f"📋 {accion}",
            description=detalles,
            color=0x0099FF,
            timestamp=datetime.now()
        )
        embed.add_field(name="👤 Usuario", value=ctx.author.name, inline=True)
        embed.add_field(name="📌 Canal", value=ctx.channel.name, inline=True)
        await canal_logs.send(embed=embed)
    except:
        pass

async def enviar_dm(usuario_id, titulo, mensaje):
    try:
        usuario = await bot.fetch_user(int(usuario_id))
        embed = discord.Embed(
            title=titulo,
            description=mensaje,
            color=0x0099FF
        )
        await usuario.send(embed=embed)
    except:
        pass

async def verificar_canal(ctx, categoria_id=None):
    if not ctx.guild:
        await ctx.send("❌ Solo en servidores")
        return False
    
    cat_id = categoria_id if categoria_id else CATEGORIA_RIFAS
    
    if ctx.channel.category_id != cat_id:
        await ctx.send(f"❌ Comando no disponible aquí")
        return False
    return True

def tiene_rol(miembro, role_id):
    return any(role.id == role_id for role in miembro.roles)

async def check_admin(ctx):
    if not ctx.guild:
        return False
    member = ctx.guild.get_member(ctx.author.id)
    if not member:
        return False
    return (tiene_rol(member, ROLES['CEO']) or tiene_rol(member, ROLES['DIRECTOR']))

async def check_vendedor(ctx):
    if not ctx.guild:
        return False
    member = ctx.guild.get_member(ctx.author.id)
    if not member:
        return False
    return (tiene_rol(member, ROLES['CEO']) or tiene_rol(member, ROLES['DIRECTOR']) or tiene_rol(member, ROLES['RIFAS']))

async def check_ceo(ctx):
    if not ctx.guild:
        return False
    member = ctx.guild.get_member(ctx.author.id)
    if not member:
        return False
    return tiene_rol(member, ROLES['CEO'])

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
    hash_obj = hashlib.md5(usuario_id.encode())
    hash_hex = hash_obj.hexdigest()[:8].upper()
    return f"VP-{hash_hex}"

async def obtener_o_crear_codigo(usuario_id, usuario_nombre):
    async with aiosqlite.connect(DB_PATH) as db:
        cursor = await db.execute('SELECT codigo FROM referidos_codigos WHERE usuario_id = ?', (usuario_id,))
        result = await cursor.fetchone()
        if result:
            return result[0]
        else:
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
        result = await cursor.fetchone()
        if result:
            return result[0]
        return 'BRONCE'

async def actualizar_fidelizacion(usuario_id, monto_compra):
    async with aiosqlite.connect(DB_PATH) as db:
        cursor = await db.execute('SELECT gasto_total FROM fidelizacion WHERE usuario_id = ?', (usuario_id,))
        result = await cursor.fetchone()
        if result:
            nuevo_gasto = result[0] + monto_compra
            await db.execute('UPDATE fidelizacion SET gasto_total = ?, fecha_actualizacion = CURRENT_TIMESTAMP WHERE usuario_id = ?', (nuevo_gasto, usuario_id))
        else:
            nuevo_gasto = monto_compra
            await db.execute('INSERT INTO fidelizacion (usuario_id, gasto_total) VALUES (?, ?)', (usuario_id, nuevo_gasto))
        nuevo_nivel = await obtener_nivel_por_gasto(nuevo_gasto)
        await db.execute('UPDATE fidelizacion SET nivel = ? WHERE usuario_id = ?', (nuevo_nivel, usuario_id))
        await db.commit()
        return nuevo_nivel

async def aplicar_cashback(usuario_id, monto_compra):
    global evento_cashback_doble
    async with aiosqlite.connect(DB_PATH) as db:
        cursor = await db.execute('SELECT porcentaje FROM cashback_config WHERE id = 1')
        config_cb = await cursor.fetchone()
        porcentaje = config_cb[0] if config_cb else 10
        if evento_cashback_doble:
            porcentaje = porcentaje * 2
        cashback = int(monto_compra * porcentaje / 100)
        await db.execute('''
            INSERT INTO cashback (usuario_id, cashback_acumulado) VALUES (?, ?)
            ON CONFLICT(usuario_id) DO UPDATE SET cashback_acumulado = cashback_acumulado + ?
        ''', (usuario_id, cashback, cashback))
        await db.commit()
        return cashback

async def obtener_descuento_usuario(usuario_id):
    global evento_oferta_activa, evento_oferta_porcentaje
    async with aiosqlite.connect(DB_PATH) as db:
        cursor = await db.execute('''
            SELECT f.nivel, fc.descuento FROM fidelizacion f
            JOIN fidelizacion_config fc ON f.nivel = fc.nivel
            WHERE f.usuario_id = ?
        ''', (usuario_id,))
        result = await cursor.fetchone()
        descuento_base = result[1] if result else 0
        if evento_oferta_activa:
            descuento_base += evento_oferta_porcentaje
        return min(descuento_base, 50)

async def procesar_comision_referido(comprador_id, monto_compra):
    async with aiosqlite.connect(DB_PATH) as db:
        cursor = await db.execute('SELECT referidor_id FROM referidos_relaciones WHERE referido_id = ?', (comprador_id,))
        result = await cursor.fetchone()
        if not result:
            return
        referidor_id = result[0]
        cursor = await db.execute('SELECT porcentaje_comision FROM referidos_config WHERE id = 1')
        config_ref = await cursor.fetchone()
        porcentaje = config_ref[0] if config_ref else 10
        comision = int(monto_compra * porcentaje / 100)
        await db.execute('''
            INSERT INTO usuarios_balance (discord_id, nombre, balance)
            VALUES (?, (SELECT nombre FROM clientes WHERE discord_id = ?), ?)
            ON CONFLICT(discord_id) DO UPDATE SET balance = balance + ?
        ''', (referidor_id, referidor_id, comision, comision))
        await db.execute('''
            UPDATE referidos_relaciones SET 
                primera_compra = 1,
                total_compras = total_compras + 1,
                total_gastado = total_gastado + ?,
                comisiones_generadas = comisiones_generadas + ?
            WHERE referido_id = ?
        ''', (monto_compra, comision, comprador_id))
        await db.commit()

async def procesar_comision_vendedor(vendedor_id, monto_compra):
    async with aiosqlite.connect(DB_PATH) as db:
        cursor = await db.execute('SELECT value FROM config_global WHERE key = "comision_vendedor"')
        result = await cursor.fetchone()
        comision_porcentaje = int(result[0]) if result else 10
        comision = int(monto_compra * comision_porcentaje / 100)
        await db.execute('''
            INSERT INTO vendedores (discord_id, nombre, comisiones_pendientes)
            VALUES (?, (SELECT nombre FROM clientes WHERE discord_id = ?), ?)
            ON CONFLICT(discord_id) DO UPDATE SET
                comisiones_pendientes = comisiones_pendientes + ?,
                total_ventas = total_ventas + 1
        ''', (vendedor_id, vendedor_id, comision, comision))
        await db.commit()

async def agregar_puntos_revancha(usuario_id, boletos_perdidos):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute('''
            INSERT INTO puntos_revancha (usuario_id, puntos) VALUES (?, ?)
            ON CONFLICT(usuario_id) DO UPDATE SET puntos = puntos + ?
        ''', (usuario_id, boletos_perdidos, boletos_perdidos))
        await db.commit()

async def actualizar_jackpot(monto_compra):
    global jackpot_activo, jackpot_total, jackpot_rifa_id, jackpot_porcentaje
    if not jackpot_activo:
        return
    rifa_activa = await bot.db.get_rifa_activa()
    if not rifa_activa or rifa_activa['id'] != jackpot_rifa_id:
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
    if usuario_id not in ranking_rifa[rifa_id]:
        ranking_rifa[rifa_id][usuario_id] = 0
    ranking_rifa[rifa_id][usuario_id] += cantidad

async def reiniciar_ranking_rifa(rifa_id):
    global ranking_rifa
    if rifa_id in ranking_rifa:
        ranking_rifa[rifa_id] = {}

async def registrar_ganador_historico(usuario_id, usuario_nick, premio, rifa_nombre, numero):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute('''
            INSERT INTO ganadores_historicos (usuario_id, usuario_nick, premio, rifa_nombre, numero)
            VALUES (?, ?, ?, ?, ?)
        ''', (usuario_id, usuario_nick, premio, rifa_nombre, numero))
        await db.commit()

async def es_numero_vip(rifa_id, numero):
    async with aiosqlite.connect(DB_PATH) as db:
        cursor = await db.execute('SELECT numeros_bloqueados FROM rifas WHERE id = ?', (rifa_id,))
        result = await cursor.fetchone()
        if not result or not result[0]:
            return False
        bloqueados_str = result[0]
        rangos = bloqueados_str.split(',')
        for r in rangos:
            if '-' in r:
                inicio, fin = map(int, r.split('-'))
                if inicio <= numero <= fin:
                    return True
            elif int(r) == numero:
                return True
    return False

# ============================================
# COMANDO DE AYUDA CON PAGINACIÓN (CORREGIDO)
# ============================================

@bot.command(name="ayuda")
async def cmd_ayuda(ctx):
    if not await verificar_canal(ctx):
        return
    
    member = ctx.guild.get_member(ctx.author.id)
    es_ceo = tiene_rol(member, ROLES['CEO'])
    es_director = tiene_rol(member, ROLES['DIRECTOR'])
    es_vendedor = tiene_rol(member, ROLES['RIFAS'])
    
    # ============================================
    # PÁGINA 1: BÁSICOS
    # ============================================
    embed1 = discord.Embed(
        title="🎟️ **SISTEMA DE RIFAS VP**",
        description=f"Prefijo: `{PREFIX}` | Versión: {VERSION}\n📄 **Página 1/10**",
        color=COLORS['primary']
    )
    embed1.add_field(name="👤 **COMANDOS BÁSICOS**", value="""
    `!rifa` - Ver rifa activa
    `!comprarrandom [cantidad]` - Comprar boletos aleatorios (números por DM)
    `!misboletos` - Ver tus boletos
    `!balance [@usuario]` - Ver balance (staff puede ver de otros)
    `!topvp` - Ranking de VP$
    `!ranking` - Top compradores de la rifa actual
    `!historial` - Tu historial de compras
    `!celiminacion [número]` - Comprar en rifa eliminación
    `!beliminacion` - Ver números disponibles en rifa eliminación
    `!mispuntos` - Ver puntos de revancha
    """, inline=False)
    
    # ============================================
    # PÁGINA 2: REFERIDOS + FIDELIZACIÓN + PROMOCIONES
    # ============================================
    embed2 = discord.Embed(
        title="🎟️ **SISTEMA DE RIFAS VP**",
        description=f"Prefijo: `{PREFIX}` | Versión: {VERSION}\n📄 **Página 2/10**",
        color=COLORS['primary']
    )
    embed2.add_field(name="🤝 **REFERIDOS**", value="""
    `!codigo` - Tu código de referido
    `!usar [código]` - Usar código de referido (solo una vez)
    `!misreferidos` - Ver tus referidos y comisiones
    """, inline=False)
    embed2.add_field(name="🏆 **FIDELIZACIÓN**", value="""
    `!nivel` - Tu nivel y beneficios
    `!topgastadores` - Ranking de gasto total
    `!cashback` - Tu cashback acumulado
    `!topcashback` - Ranking cashback
    `!verniveles` - Ver configuración de niveles
    """, inline=False)
    embed2.add_field(name="🎁 **PROMOCIONES**", value="""
    `!canjear [código]` - Canjear código promocional
    """, inline=False)
    
    # ============================================
    # PÁGINA 3: CAJAS + RULETA + APUESTAS
    # ============================================
    embed3 = discord.Embed(
        title="🎟️ **SISTEMA DE RIFAS VP**",
        description=f"Prefijo: `{PREFIX}` | Versión: {VERSION}\n📄 **Página 3/10**",
        color=COLORS['primary']
    )
    embed3.add_field(name="📦 **CAJAS MISTERIOSAS**", value="""
    `!cajas` - Ver catálogo de cajas
    `!comprarcaja [tipo] [cantidad]` - Comprar cajas
    `!miscajas` - Ver cajas sin abrir
    `!abrircaja [id]` - Abrir caja
    `!topcajas` - Ranking de ganadores de cajas
    """, inline=False)
    embed3.add_field(name="🎡 **RULETA DIARIA**", value="""
    `!ruleta` - Girar la ruleta (1 vez/día)
    `!ruleta_stats [@usuario]` - Estadísticas de ruleta
    """, inline=False)
    embed3.add_field(name="🎲 **APUESTAS/PREDICCIONES**", value="""
    `!apostar [número] [cantidad]` - Apostar en la rifa actual
    `!mis_apuestas` - Ver tus apuestas activas
    `!apuestas_stats [@usuario]` - Estadísticas de apuestas
    """, inline=False)
    
    # ============================================
    # PÁGINA 4: SUBASTAS + MARKETPLACE
    # ============================================
    embed4 = discord.Embed(
        title="🎟️ **SISTEMA DE RIFAS VP**",
        description=f"Prefijo: `{PREFIX}` | Versión: {VERSION}\n📄 **Página 4/10**",
        color=COLORS['primary']
    )
    embed4.add_field(name="🎫 **SUBASTAS**", value="""
    `!subastas` - Ver subastas activas
    `!pujar [id] [monto]` - Pujar en una subasta
    `!mis_pujas` - Ver tus pujas
    """, inline=False)
    embed4.add_field(name="🛒 **MARKETPLACE**", value="""
    `!marketplace [página]` - Ver boletos en venta
    `!vender_boleto [número] [precio] [ofertas]` - Vender boleto
    `!comprar_boleto [id]` - Comprar boleto directo
    `!ofertar [id] [monto] [mensaje]` - Hacer oferta
    `!mis_listados` - Ver tus listados y ofertas
    `!aceptar_oferta [id]` - Aceptar oferta
    `!rechazar_oferta [id]` - Rechazar oferta
    `!cancelar_listado [id]` - Cancelar venta
    """, inline=False)
    
    # ============================================
    # PÁGINA 5: REGALOS + BANCO + MISIONES
    # ============================================
    embed5 = discord.Embed(
        title="🎟️ **SISTEMA DE RIFAS VP**",
        description=f"Prefijo: `{PREFIX}` | Versión: {VERSION}\n📄 **Página 5/10**",
        color=COLORS['primary']
    )
    embed5.add_field(name="🎁 **REGALOS**", value="""
    `!regalar [@user] [cantidad] [mensaje]` - Regalar VP$
    `!solicitudes` - Ver regalos pendientes
    `!aceptar [id]` - Aceptar regalo
    `!rechazar [id]` - Rechazar regalo
    `!mis_regalos` - Historial de regalos
    """, inline=False)
    embed5.add_field(name="🏦 **BANCO VP**", value="""
    `!banco` - Ver productos del banco
    `!invertir [producto] [monto]` - Invertir VP$
    `!misinversiones` - Ver inversiones activas
    `!retirar [id]` - Retirar inversión
    `!cambiarng [cantidad]` - Cambiar NG$ a VP$ (crea ticket)
    """, inline=False)
    embed5.add_field(name="📋 **MISIONES**", value="""
    `!misiones` - Ver misiones diarias
    `!misiones_semanales` - Ver misiones semanales
    `!miracha` - Ver tu racha
    `!reclamar [mision_id]` - Reclamar recompensa
    """, inline=False)
    
    # ============================================
    # PÁGINA 6: PERSONALIZACIÓN + DISTRIBUIDORES + FRANQUICIAS
    # ============================================
    embed6 = discord.Embed(
        title="🎟️ **SISTEMA DE RIFAS VP**",
        description=f"Prefijo: `{PREFIX}` | Versión: {VERSION}\n📄 **Página 6/10**",
        color=COLORS['primary']
    )
    embed6.add_field(name="🎨 **PERSONALIZACIÓN**", value="""
    `!perfil [@usuario]` - Ver perfil personalizado
    `!tienda_perfil` - Ver tienda de personalización
    `!comprar_perfil [id]` - Comprar item
    `!equipar [id]` - Equipar item
    `!perfil_set [titulo/bio/color] [valor]` - Personalizar perfil
    """, inline=False)
    embed6.add_field(name="📦 **DISTRIBUIDORES**", value="""
    `!distribuidor` - Ver perfil de distribuidor
    `!productos` - Ver catálogo de productos
    `!comprar_producto [nombre] [cantidad]` - Comprar productos
    `!mis_productos` - Ver inventario
    """, inline=False)
    embed6.add_field(name="👑 **FRANQUICIAS**", value="""
    `!franquicia` - Ver perfil de franquicia
    `!franquicia_rifa [premio] [precio] [total]` - Crear rifa de franquicia
    `!franquicia_stats` - Ver estadísticas
    """, inline=False)
    
    # ============================================
    # PÁGINA 7: VENDEDORES
    # ============================================
    embed7 = discord.Embed(
        title="🎟️ **SISTEMA DE RIFAS VP**",
        description=f"Prefijo: `{PREFIX}` | Versión: {VERSION}\n📄 **Página 7/10**",
        color=COLORS['primary']
    )
    embed7.add_field(name="💰 **VENDEDORES**", value="""
    `!vender [@usuario] [número]` - Vender número específico
    `!venderrandom [@usuario] [cantidad]` - Vender aleatorios
    `!misventas` - Ver tus ventas y comisiones
    `!listaboletos` - Lista de boletos disponibles
    """, inline=False)
    
    # ============================================
    # PÁGINA 8: DIRECTORES
    # ============================================
    embed8 = discord.Embed(
        title="🎟️ **SISTEMA DE RIFAS VP**",
        description=f"Prefijo: `{PREFIX}` | Versión: {VERSION}\n📄 **Página 8/10**",
        color=COLORS['primary']
    )
    embed8.add_field(name="🎯 **DIRECTORES**", value="""
    `!crearifa [premio] [precio] [total] [bloqueados]` - Crear rifa
    `!aumentarnumeros [cantidad]` - Ampliar rifa
    `!cerrarifa` - Cerrar rifa
    `!iniciarsorteo [ganadores]` - Iniciar sorteo
    `!cancelarsorteo` - Cancelar sorteo
    `!finalizarrifa [id] [ganadores]` - Finalizar rifa
    `!vendedoradd [@usuario] [%]` - Añadir vendedor
    `!vercomisiones` - Ver comisiones pendientes
    `!pagarcomisiones` - Pagar comisiones
    `!setcomision [%]` - Configurar comisión vendedores
    `!reporte` - Reporte de rifa actual
    `!alertar [mensaje]` - Alerta a todos
    `!rifaeliminacion [total] [premio] [valor]` - Iniciar rifa eliminación
    `!rifaeliminacionr [número]` - Eliminar número
    `!rankingreset` - Resetear ranking de rifa
    `!topcomprador [id_rifa]` - Top compradores por ID
    `!verboletos [@usuario]` - Ver boletos de usuario
    `!subasta crear [item] [precio_base] [horas]` - Crear subasta
    `!subasta cancelar [id]` - Cancelar subasta
    """, inline=False)
    
    # ============================================
    # PÁGINA 9: CEO (1/2)
    # ============================================
    embed9 = discord.Embed(
        title="🎟️ **SISTEMA DE RIFAS VP**",
        description=f"Prefijo: `{PREFIX}` | Versión: {VERSION}\n📄 **Página 9/10**",
        color=COLORS['primary']
    )
    embed9.add_field(name="👑 **CEO (1/2)**", value="""
    `!acreditarvp [@usuario] [cantidad]` - Acreditar VP$
    `!retirarvp [@usuario] [cantidad]` - Retirar VP$
    `!procesarvp [@usuario]` - Marcar pago como en proceso
    `!procesadovp [@usuario] [vp]` - Confirmar pago y acreditar
    `!setrefcomision [%]` - Configurar comisión referidos
    `!setrefdescuento [%]` - Configurar descuento referidos
    `!setcashback [%]` - Configurar cashback
    `!pagarcashback` - Pagar todo el cashback acumulado
    `!resetcashback` - Resetear cashback
    `!setnivel [nivel] [campo] [valor]` - Configurar niveles
    `!estadisticas` - Estadísticas globales
    `!auditoria` - Ver transacciones
    `!exportar` - Exportar datos a CSV
    `!backup` - Crear backup manual
    `!resetallsistema` - Reiniciar todo el sistema
    `!version` - Versión del bot
    """, inline=False)
    
    # ============================================
    # PÁGINA 10: CEO (2/2)
    # ============================================
    embed10 = discord.Embed(
        title="🎟️ **SISTEMA DE RIFAS VP**",
        description=f"Prefijo: `{PREFIX}` | Versión: {VERSION}\n📄 **Página 10/10**",
        color=COLORS['primary']
    )
    embed10.add_field(name="👑 **CEO (2/2)**", value="""
    `!crearcodigo [codigo] [vp]` - Crear código promocional
    `!borrarcodigo [codigo]` - Borrar código
    `!evento 2x1 [on/off]` - Activar/desactivar evento 2x1
    `!evento cashbackdoble [on/off]` - Activar/desactivar cashback doble
    `!evento oferta [%] [dias]` - Activar oferta por días
    `!evento programar [tipo] [valor] [fecha_ini] [fecha_fin]` - Programar evento
    `!jackpot [base] [%] [id_rifa]` - Iniciar jackpot
    `!jackpotreset` - Resetear jackpot
    `!jackpotsortear [ganadores]` - Sortear jackpot
    `!puntosreset [@usuario]` - Resetear puntos de revancha
    `!config get [key]` - Ver configuración
    `!config set [key] [valor]` - Cambiar configuración
    `!config list` - Listar configuraciones
    `!caja crear [tipo] [nombre] [precio]` - Crear caja
    `!caja editar [id] [campo] [valor]` - Editar caja
    `!caja añadirpremio [caja_id] [premio] [prob]` - Añadir premio a caja
    `!mision crear [nombre] [requisito] [valor] [recompensa]` - Crear misión
    `!mision editar [id] [campo] [valor]` - Editar misión
    `!banco producto crear [nombre] [dias] [interes] [min] [max]` - Crear producto banco
    `!ruleta_config set premios [json]` - Configurar ruleta
    `!apuestas_config set activo true/false` - Configurar apuestas
    `!perfil_item crear [tipo] [nombre] [precio]` - Crear item de perfil
    `!ticketcerrar` - Cerrar ticket actual
    """, inline=False)
    
    # Lista de páginas
    paginas = [embed1, embed2, embed3, embed4, embed5, embed6, embed7, embed8, embed9, embed10]
    
    # Mostrar solo páginas según permisos (si no es staff, ocultar páginas 7-10)
    if not es_vendedor and not es_director and not es_ceo:
        paginas = paginas[:7]  # Solo páginas 1-6 para usuarios normales
    elif not es_director and not es_ceo:
        paginas = paginas[:8]  # Páginas 1-8 (sin CEO)
    elif not es_ceo:
        paginas = paginas[:9]  # Páginas 1-9 (sin página 10)
    
    pagina_actual = 0
    
    # Enviar primera página
    msg = await ctx.send(embed=paginas[pagina_actual])
    
    # Añadir reacciones si hay más de una página
    if len(paginas) > 1:
        await msg.add_reaction("⬅️")
        await msg.add_reaction("➡️")
        await msg.add_reaction("❌")
        
        def check(reaction, user):
            return user == ctx.author and reaction.message.id == msg.id and str(reaction.emoji) in ["⬅️", "➡️", "❌"]
        
        while True:
            try:
                reaction, user = await bot.wait_for("reaction_add", timeout=60.0, check=check)
                
                if str(reaction.emoji) == "➡️" and pagina_actual < len(paginas) - 1:
                    pagina_actual += 1
                    await msg.edit(embed=paginas[pagina_actual])
                elif str(reaction.emoji) == "⬅️" and pagina_actual > 0:
                    pagina_actual -= 1
                    await msg.edit(embed=paginas[pagina_actual])
                elif str(reaction.emoji) == "❌":
                    await msg.delete()
                    break
                
                await msg.remove_reaction(reaction.emoji, user)
                
            except asyncio.TimeoutError:
                try:
                    await msg.clear_reactions()
                except:
                    pass
                break

# ============================================
# EJECUCIÓN
# ============================================

if __name__ == "__main__":
    try:
        if not config.BOT_TOKEN:
            print("❌ No hay BOT_TOKEN")
            sys.exit(1)
        bot.run(config.BOT_TOKEN)
    except Exception as e:
        logger.error(f"Error fatal: {e}")
