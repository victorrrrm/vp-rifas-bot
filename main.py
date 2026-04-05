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

VERSION = "6.0.0"
PREFIX = "!"
start_time = datetime.now()
reconnect_attempts = 0
MAX_RECONNECT_ATTEMPTS = 999999

# Variables de eventos activos
evento_2x1 = False
evento_cashback_doble = False
evento_oferta_porcentaje = 0
evento_oferta_activa = False

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

COLORS = {
    'primary': 0xFFD700,
    'success': 0x00FF00,
    'error': 0xFF0000,
    'info': 0x0099FF
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
            logger.info("✅ Base de datos inicializada")
        except Exception as e:
            logger.error(f"❌ Error: {e}")
            traceback.print_exc()
        
        self.keep_alive_task.start()
        self.status_task.start()
        self.actualizar_jackpot_task.start()
        logger.info("✅ Tareas automáticas iniciadas")
    
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
    
    async def on_ready(self):
        logger.info(f"✅ Bot conectado como {self.user}")
        logger.info(f"🌐 En {len(self.guilds)} servidores")
        
        if self.volumen_montado:
            logger.info("💾 Volumen persistente activo")
        
        global reconnect_attempts
        reconnect_attempts = 0
        self.reconnecting = False
        
        await self.enviar_log_sistema(
            "🟢 **BOT INICIADO**", 
            f"Bot iniciado\nVersión: {VERSION}\nServidores: {len(self.guilds)}\nVolumen: {'✅' if self.volumen_montado else '❌'}"
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
                f"{PREFIX}ayuda | {len(self.guilds)} servers",
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
    global COMISION_VENDEDOR
    async with aiosqlite.connect(DB_PATH) as db:
        comision = int(monto_compra * COMISION_VENDEDOR / 100)
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
        
        global ultimos_ganadores
        cursor = await db.execute('SELECT usuario_nick, premio, fecha FROM ganadores_historicos ORDER BY fecha DESC LIMIT 10')
        ultimos_ganadores = await cursor.fetchall()
        ultimos_ganadores = [dict(g) for g in ultimos_ganadores]

async def es_numero_bloqueado(rifa_id, numero):
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
# COMANDO DE AYUDA (SOLO !ayuda)
# ============================================

@bot.command(name="ayuda")
async def cmd_ayuda(ctx):
    if not await verificar_canal(ctx):
        return
    
    member = ctx.guild.get_member(ctx.author.id)
    es_ceo = tiene_rol(member, ROLES['CEO'])
    es_director = tiene_rol(member, ROLES['DIRECTOR'])
    es_vendedor = tiene_rol(member, ROLES['RIFAS'])
    
    embed = discord.Embed(
        title="🎟️ **SISTEMA DE RIFAS VP**",
        description=f"Prefijo: `{PREFIX}` | Versión: {VERSION}",
        color=COLORS['primary']
    )
    
    basicos = """
    `!rifa` - Ver rifa activa
    `!comprarrandom [cantidad]` - Comprar boletos aleatorios
    `!misboletos` - Ver tus boletos
    `!balance` - Ver tu balance
    `!topvp` - Ranking de VP$
    `!ranking` - Top compradores de la rifa actual
    `!historial` - Tu historial
    `!celiminacion [número]` - Comprar en rifa eliminación
    `!beliminacion` - Ver números disponibles en rifa eliminación
    `!mispuntos` - Ver puntos de revancha
    """
    embed.add_field(name="👤 **BÁSICOS**", value=basicos, inline=False)
    
    referidos = """
    `!codigo` - Tu código de referido
    `!usar [código]` - Usar código (solo una vez)
    `!misreferidos` - Ver tus referidos
    """
    embed.add_field(name="🤝 **REFERIDOS**", value=referidos, inline=False)
    
    fidelizacion = """
    `!nivel` - Tu nivel y beneficios
    `!topgastadores` - Ranking de gasto
    `!cashback` - Tu cashback acumulado
    `!topcashback` - Ranking cashback
    `!verniveles` - Ver configuración de niveles
    """
    embed.add_field(name="🏆 **FIDELIZACIÓN**", value=fidelizacion, inline=False)
    
    promociones = """
    `!canjear [código]` - Canjear código promocional
    """
    embed.add_field(name="🎁 **PROMOCIONES**", value=promociones, inline=False)
    
    if es_vendedor or es_director or es_ceo:
        vendedor = """
        `!vender [@usuario] [número]` - Vender número específico
        `!venderrandom [@usuario] [cantidad]` - Vender aleatorios
        `!misventas` - Ver tus ventas
        `!listaboletos` - Lista de boletos
        """
        embed.add_field(name="💰 **VENDEDORES**", value=vendedor, inline=False)
    
    if es_director or es_ceo:
        director1 = """
        `!crearifa [premio] [precio] [total]` - Crear rifa
        `!aumentarnumeros [cantidad]` - Ampliar rifa
        `!cerrarifa` - Cerrar rifa
        `!iniciarsorteo [ganadores]` - Iniciar sorteo
        `!cancelarsorteo` - Cancelar sorteo
        `!finalizarrifa [id] [ganadores]` - Finalizar rifa
        `!vendedoradd [@usuario] [%]` - Añadir vendedor
        `!vercomisiones` - Ver comisiones
        `!pagarcomisiones` - Pagar comisiones
        """
        embed.add_field(name="🎯 **DIRECTORES (1/2)**", value=director1, inline=False)
        
        director2 = """
        `!reporte` - Reporte de rifa
        `!balance [@usuario]` - Ver balance de usuario
        `!rankingreset` - Resetear ranking de rifa
        `!topcomprador [id]` - Top compradores por ID
        `!topgastadoresreset` - Resetear top gastadores
        `!setnivel` - Configurar niveles
        `!setcomision [%]` - Configurar comisión vendedores
        `!alertar [mensaje]` - Alerta a todos
        `!rifaeliminacion [total] [premio] [valor]` - Iniciar rifa eliminación
        `!rifaeliminacionr` - Eliminar número
        """
        embed.add_field(name="🎯 **DIRECTORES (2/2)**", value=director2, inline=False)
    
    if es_ceo:
        ceo1 = """
        `!acreditarvp [@usuario] [cantidad]` - Acreditar VP$
        `!retirarvp [@usuario] [cantidad]` - Retirar VP$
        `!setrefcomision [%]` - Configurar comisión referidos
        `!setrefdescuento [%]` - Configurar descuento referidos
        `!setcashback [%]` - Configurar cashback
        `!pagarcashback` - Pagar cashback
        `!resetcashback` - Resetear cashback
        `!setnivel` - Configurar niveles
        """
        embed.add_field(name="👑 **CEO (1/2)**", value=ceo1, inline=False)
        
        ceo2 = """
        `!estadisticas` - Estadísticas globales
        `!auditoria` - Ver transacciones
        `!exportar` - Exportar a CSV
        `!backup` - Crear backup
        `!resetallsistema` - Reiniciar sistema
        `!version` - Versión del bot
        `!crearcodigo [codigo] [vp]` - Crear código promocional
        `!borrarcodigo [codigo]` - Borrar código
        `!2x1` - Activar/desactivar evento 2x1
        `!cashbackdoble` - Activar/desactivar cashback doble
        `!oferta [%]` - Activar oferta
        `!ofertadesactivar` - Desactivar oferta
        `!jackpot [base] [%] [id_rifa]` - Iniciar jackpot
        `!jackpotreset` - Resetear jackpot
        `!jackpotsortear [ganadores]` - Sortear jackpot
        `!puntosreset` - Resetear puntos
        `!puntosreset [@usuario]` - Resetear puntos de un usuario
        """
        embed.add_field(name="👑 **CEO (2/2)**", value=ceo2, inline=False)
    
    embed.set_footer(text="Ejemplo: !comprarrandom 3")
    await ctx.send(embed=embed)

# ============================================
# COMANDOS BÁSICOS
# ============================================

@bot.command(name="version")
async def cmd_version(ctx):
    uptime = datetime.now() - start_time
    horas = uptime.total_seconds() // 3600
    minutos = (uptime.total_seconds() % 3600) // 60
    
    embed = discord.Embed(
        title="🤖 **VP RIFAS BOT**",
        description=f"**Versión:** `{VERSION}`\n"
                    f"**Estado:** 🟢 Activo\n"
                    f"**Uptime:** {int(horas)}h {int(minutos)}m\n"
                    f"**Servidores:** {len(bot.guilds)}\n"
                    f"**Volumen:** {'✅ Activo' if bot.volumen_montado else '❌ No detectado'}",
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
    
    # Filtrar números bloqueados
    disponibles_filtrados = []
    for num in disponibles:
        if not await es_numero_bloqueado(rifa_activa['id'], num):
            disponibles_filtrados.append(num)
    
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
            await db.execute('''
                INSERT INTO boletos (rifa_id, numero, comprador_id, comprador_nick, precio_pagado)
                VALUES (?, ?, ?, ?, ?)
            ''', (rifa_activa['id'], num, str(ctx.author.id), ctx.author.name, precio_boleto))
            comprados.append(num)
        
        await db.commit()
    
    await actualizar_fidelizacion(str(ctx.author.id), precio_con_descuento)
    cashback = await aplicar_cashback(str(ctx.author.id), precio_con_descuento)
    await procesar_comision_referido(str(ctx.author.id), precio_con_descuento)
    await actualizar_jackpot(precio_con_descuento)
    await actualizar_ranking_rifa(rifa_activa['id'], str(ctx.author.id), len(comprados))
    
    await enviar_dm(str(ctx.author.id), "✅ Compra realizada", 
                    f"Has comprado {len(comprados)} boletos: {', '.join(map(str, comprados))}\n"
                    f"Total: ${precio_con_descuento}\n"
                    f"Descuento: {descuento}%\n"
                    f"Cashback acumulado: ${cashback}")
    
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
    embed = discord.Embed(
        title="🎟️ Tus boletos",
        description=f"Números: {', '.join(numeros)}",
        color=COLORS['primary']
    )
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
    
    embed = discord.Embed(
        title=f"💰 Balance de {target.name}",
        description=f"**{balance:,} VP$**",
        color=COLORS['primary']
    )
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
        cursor = await db.execute('''
            SELECT comprador_nick, COUNT(*) as boletos
            FROM boletos WHERE rifa_id = ?
            GROUP BY comprador_id
            ORDER BY boletos DESC LIMIT 10
        ''', (rifa_activa['id'],))
        ranking = await cursor.fetchall()
    
    if not ranking:
        await ctx.send(embed=embeds.crear_embed_info("Sin datos", "Aún no hay compras en esta rifa"))
        return
    
    embed = discord.Embed(title="🏆 TOP COMPRADORES", color=COLORS['primary'])
    for i, u in enumerate(ranking, 1):
        medalla = {1: "🥇", 2: "🥈", 3: "🥉"}.get(i, f"{i}.")
        embed.add_field(name=f"{medalla} {u['comprador_nick']}", value=f"[{u['boletos']}]", inline=False)
    await ctx.send(embed=embed)

@bot.command(name="topcomprador")
async def cmd_top_comprador(ctx, id_rifa: int):
    if not await check_admin(ctx):
        await ctx.send("❌ No tienes permiso")
        return
    
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        cursor = await db.execute('''
            SELECT comprador_nick, COUNT(*) as boletos
            FROM boletos WHERE rifa_id = ?
            GROUP BY comprador_id
            ORDER BY boletos DESC LIMIT 10
        ''', (id_rifa,))
        ranking = await cursor.fetchall()
        cursor = await db.execute('SELECT * FROM rifas WHERE id = ?', (id_rifa,))
        rifa = await cursor.fetchone()
    
    if not rifa:
        await ctx.send(embed=embeds.crear_embed_error(f"No existe rifa con ID {id_rifa}"))
        return
    
    if not ranking:
        await ctx.send(embed=embeds.crear_embed_info("Sin datos", f"No hay compras en la rifa #{id_rifa}"))
        return
    
    embed = discord.Embed(
        title=f"🏆 TOP COMPRADORES - Rifa #{id_rifa}",
        description=f"**Premio:** {rifa['premio']}",
        color=COLORS['primary']
    )
    for i, u in enumerate(ranking, 1):
        medalla = {1: "🥇", 2: "🥈", 3: "🥉"}.get(i, f"{i}.")
        embed.add_field(name=f"{medalla} {u['comprador_nick']}", value=f"[{u['boletos']} boletos]", inline=False)
    await ctx.send(embed=embed)

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

@bot.command(name="historial")
async def cmd_historial(ctx):
    if not await verificar_canal(ctx):
        return
    
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        cursor = await db.execute('''
            SELECT b.numero, r.nombre as rifa, b.fecha_compra, b.precio_pagado
            FROM boletos b
            JOIN rifas r ON b.rifa_id = r.id
            WHERE b.comprador_id = ?
            ORDER BY b.fecha_compra DESC LIMIT 20
        ''', (str(ctx.author.id),))
        boletos = await cursor.fetchall()
    
    if not boletos:
        await ctx.send(embed=embeds.crear_embed_error("Sin historial"))
        return
    
    embed = discord.Embed(title="📜 Tu historial", color=COLORS['primary'])
    for b in boletos[:10]:
        embed.add_field(
            name=f"{b['rifa']} - #{b['numero']}",
            value=f"${b['precio_pagado']} - {b['fecha_compra'][:10]}",
            inline=False
        )
    await ctx.send(embed=embed)

# ============================================
# SISTEMA DE REFERIDOS
# ============================================

@bot.command(name="codigo")
async def cmd_codigo(ctx):
    if not await verificar_canal(ctx):
        return
    
    codigo = await obtener_o_crear_codigo(str(ctx.author.id), ctx.author.name)
    
    embed = discord.Embed(
        title="🔗 Tu código de referido",
        description=f"`{codigo}`",
        color=COLORS['primary']
    )
    
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
        ya_tiene = await cursor.fetchone()
        if ya_tiene:
            await ctx.send(embed=embeds.crear_embed_error("Ya tienes un referidor"))
            return
        
        cursor = await db.execute('SELECT usuario_id FROM referidos_codigos WHERE codigo = ? AND usuario_id != ?', (codigo.upper(), str(ctx.author.id)))
        referidor = await cursor.fetchone()
        if not referidor:
            await ctx.send(embed=embeds.crear_embed_error("Código inválido"))
            return
        
        await db.execute('INSERT INTO referidos_relaciones (referido_id, referidor_id) VALUES (?, ?)', (str(ctx.author.id), referidor[0]))
        await db.commit()
    
    await ctx.send(embed=embeds.crear_embed_exito("Código aplicado correctamente"))

@bot.command(name="misreferidos")
async def cmd_mis_referidos(ctx):
    if not await verificar_canal(ctx):
        return
    
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        cursor = await db.execute('''
            SELECT r.referido_id, c.nombre, r.total_compras, r.comisiones_generadas
            FROM referidos_relaciones r
            LEFT JOIN clientes c ON r.referido_id = c.discord_id
            WHERE r.referidor_id = ?
            ORDER BY r.fecha_registro DESC LIMIT 20
        ''', (str(ctx.author.id),))
        referidos = await cursor.fetchall()
    
    if not referidos:
        await ctx.send(embed=embeds.crear_embed_error("No tienes referidos"))
        return
    
    embed = discord.Embed(title="👥 Tus referidos", color=COLORS['primary'])
    for ref in referidos[:10]:
        nombre = ref['nombre'] or "Usuario"
        embed.add_field(
            name=f"👤 {nombre}",
            value=f"Compras: {ref['total_compras']} | Comisiones: ${ref['comisiones_generadas']}",
            inline=False
        )
    await ctx.send(embed=embed)

# ============================================
# COMANDOS CEO PARA REFERIDOS
# ============================================

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

@bot.command(name="setrefdescuento")
async def cmd_set_ref_descuento(ctx, porcentaje: int):
    if not await check_ceo(ctx):
        return
    if porcentaje < 0 or porcentaje > 50:
        await ctx.send(embed=embeds.crear_embed_error("0-50%"))
        return
    global REFERIDOS_DESCUENTO
    REFERIDOS_DESCUENTO = porcentaje
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute('UPDATE referidos_config SET porcentaje_descuento = ? WHERE id = 1', (porcentaje,))
        await db.commit()
    await ctx.message.delete()
    await ctx.send(embed=embeds.crear_embed_exito(f"Descuento referidos: {porcentaje}%"))

# ============================================
# SISTEMA DE FIDELIZACIÓN
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
    
    embed = discord.Embed(
        title=f"🏆 Nivel: {data['nivel']}",
        description=f"Gasto total: **${data['gasto_total']} VP$**",
        color=COLORS['primary']
    )
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
        cursor = await db.execute('''
            SELECT c.nombre, f.gasto_total, f.nivel
            FROM fidelizacion f
            LEFT JOIN clientes c ON f.usuario_id = c.discord_id
            WHERE f.gasto_total > 0
            ORDER BY f.gasto_total DESC LIMIT 10
        ''')
        top = await cursor.fetchall()
    
    if not top:
        await ctx.send(embed=embeds.crear_embed_info("Sin datos", "No hay gastadores"))
        return
    
    embed = discord.Embed(title="🏆 TOP GASTADORES", color=COLORS['primary'])
    for i, u in enumerate(top, 1):
        nombre = u['nombre'] or "Usuario"
        medalla = {1: "🥇", 2: "🥈", 3: "🥉"}.get(i, f"{i}.")
        embed.add_field(
            name=f"{medalla} {nombre}",
            value=f"Gastado: ${u['gasto_total']} | {u['nivel']}",
            inline=False
        )
    await ctx.send(embed=embed)

@bot.command(name="topgastadoresreset")
async def cmd_top_gastadores_reset(ctx):
    if not await check_ceo(ctx):
        return
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute('UPDATE fidelizacion SET gasto_total = 0, nivel = "BRONCE"')
        await db.commit()
    await ctx.message.delete()
    await ctx.send(embed=embeds.crear_embed_exito("Ranking de gastadores reseteado"))

@bot.command(name="verniveles")
async def cmd_ver_niveles(ctx):
    if not await verificar_canal(ctx):
        return
    
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        cursor = await db.execute('SELECT * FROM fidelizacion_config ORDER BY gasto_minimo ASC')
        niveles = await cursor.fetchall()
    
    embed = discord.Embed(
        title="📊 **CONFIGURACIÓN DE NIVELES**",
        color=COLORS['primary']
    )
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

# ============================================
# SISTEMA DE CASHBACK
# ============================================

@bot.command(name="cashback")
async def cmd_cashback(ctx):
    if not await verificar_canal(ctx):
        return
    
    async with aiosqlite.connect(DB_PATH) as db:
        cursor = await db.execute('SELECT cashback_acumulado FROM cashback WHERE usuario_id = ?', (str(ctx.author.id),))
        result = await cursor.fetchone()
        cashback = result[0] if result else 0
    
    embed = discord.Embed(
        title="💰 Cashback",
        description=f"Acumulado: **${cashback} VP$**",
        color=COLORS['primary']
    )
    await ctx.send(embed=embed)

@bot.command(name="topcashback")
async def cmd_top_cashback(ctx):
    if not await verificar_canal(ctx):
        return
    
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        cursor = await db.execute('''
            SELECT c.usuario_id, cl.nombre, c.cashback_acumulado
            FROM cashback c
            LEFT JOIN clientes cl ON c.usuario_id = cl.discord_id
            WHERE c.cashback_acumulado > 0
            ORDER BY c.cashback_acumulado DESC LIMIT 10
        ''')
        top = await cursor.fetchall()
    
    if not top:
        await ctx.send(embed=embeds.crear_embed_info("Sin datos", "No hay cashback"))
        return
    
    embed = discord.Embed(title="💰 TOP CASHBACK", color=COLORS['primary'])
    for i, u in enumerate(top, 1):
        nombre = u['nombre'] or "Usuario"
        embed.add_field(name=f"{i}. {nombre}", value=f"**${u['cashback_acumulado']}**", inline=False)
    await ctx.send(embed=embed)

# ============================================
# COMANDOS CEO PARA CASHBACK
# ============================================

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
            await db.execute('''
                INSERT INTO usuarios_balance (discord_id, nombre, balance)
                VALUES (?, (SELECT nombre FROM clientes WHERE discord_id = ?), ?)
                ON CONFLICT(discord_id) DO UPDATE SET balance = balance + ?
            ''', (u['usuario_id'], u['usuario_id'], monto, monto))
            await db.execute('''
                INSERT INTO transacciones (tipo, monto, destino_id, descripcion) VALUES ('cashback', ?, ?, ?)
            ''', (monto, u['usuario_id'], "Pago de cashback"))
            await db.execute('UPDATE cashback SET cashback_acumulado = 0, cashback_recibido = cashback_recibido + ? WHERE usuario_id = ?', (monto, u['usuario_id']))
            total_pagado += monto
            await enviar_dm(u['usuario_id'], "💰 Cashback pagado", f"Has recibido ${monto} VP$ por cashback")
        await db.commit()
    
    await ctx.message.delete()
    await ctx.send(embed=embeds.crear_embed_exito(f"Pagados ${total_pagado} VP$ de cashback a {len(usuarios)} usuarios"))

@bot.command(name="resetcashback")
async def cmd_reset_cashback(ctx):
    if not await check_ceo(ctx):
        return
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute('UPDATE cashback SET cashback_acumulado = 0')
        await db.commit()
    await ctx.message.delete()
    await ctx.send(embed=embeds.crear_embed_exito("Cashback reseteados"))

# ============================================
# COMANDOS DE VENDEDOR
# ============================================

@bot.command(name="vender")
async def cmd_vender(ctx, usuario: discord.Member, numero: int):
    if not await verificar_canal(ctx):
        return
    if not await check_vendedor(ctx):
        await ctx.send(embed=embeds.crear_embed_error("Sin permiso"))
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
        await ctx.send(embed=embeds.crear_embed_error(f"Usuario necesita {precio_final} VP$"))
        return
    
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute('UPDATE usuarios_balance SET balance = ? WHERE discord_id = ?', (balance - precio_final, str(usuario.id)))
        await db.execute('''
            INSERT INTO boletos (rifa_id, numero, comprador_id, comprador_nick, vendedor_id, precio_pagado)
            VALUES (?, ?, ?, ?, ?, ?)
        ''', (rifa_activa['id'], numero, str(usuario.id), usuario.name, str(ctx.author.id), precio_boleto))
        await db.commit()
    
    await actualizar_fidelizacion(str(usuario.id), precio_final)
    await aplicar_cashback(str(usuario.id), precio_final)
    await procesar_comision_referido(str(usuario.id), precio_final)
    await procesar_comision_vendedor(str(ctx.author.id), precio_final)
    await actualizar_jackpot(precio_final)
    await actualizar_ranking_rifa(rifa_activa['id'], str(usuario.id), 1)
    
    await enviar_dm(str(usuario.id), "🎟️ Boleto comprado", f"Has comprado el boleto #{numero} por ${precio_final} VP$")
    await enviar_dm(str(ctx.author.id), "💰 Venta realizada", f"Has vendido el boleto #{numero} a {usuario.name} por ${precio_final} VP$")
    
    await ctx.message.delete()
    await ctx.send(embed=embeds.crear_embed_exito(f"✅ Venta realizada. Revisa tu DM."))

@bot.command(name="venderrandom")
async def cmd_vender_random(ctx, usuario: discord.Member, cantidad: int = 1):
    if not await verificar_canal(ctx):
        return
    if not await check_vendedor(ctx):
        await ctx.send(embed=embeds.crear_embed_error("Sin permiso"))
        return
    
    if cantidad < 1 or cantidad > 50:
        await ctx.send(embed=embeds.crear_embed_error("1-50 boletos"))
        return
    
    rifa_activa = await bot.db.get_rifa_activa()
    if not rifa_activa:
        await ctx.send(embed=embeds.crear_embed_error("No hay rifa"))
        return
    
    disponibles = await bot.db.get_boletos_disponibles(rifa_activa['id'])
    
    # Filtrar números bloqueados
    disponibles_filtrados = []
    for num in disponibles:
        if not await es_numero_bloqueado(rifa_activa['id'], num):
            disponibles_filtrados.append(num)
    
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
        await ctx.send(embed=embeds.crear_embed_error(f"Usuario necesita {precio_final} VP$"))
        return
    
    seleccionados = random.sample(disponibles_filtrados, boletos_a_recibir)
    comprados = []
    
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute('UPDATE usuarios_balance SET balance = ? WHERE discord_id = ?', (balance - precio_final, str(usuario.id)))
        for num in seleccionados:
            await db.execute('''
                INSERT INTO boletos (rifa_id, numero, comprador_id, comprador_nick, vendedor_id, precio_pagado)
                VALUES (?, ?, ?, ?, ?, ?)
            ''', (rifa_activa['id'], num, str(usuario.id), usuario.name, str(ctx.author.id), precio_boleto))
            comprados.append(num)
        await db.commit()
    
    await actualizar_fidelizacion(str(usuario.id), precio_final)
    cashback = await aplicar_cashback(str(usuario.id), precio_final)
    await procesar_comision_referido(str(usuario.id), precio_final)
    await procesar_comision_vendedor(str(ctx.author.id), precio_final)
    await actualizar_jackpot(precio_final)
    await actualizar_ranking_rifa(rifa_activa['id'], str(usuario.id), len(comprados))
    
    await enviar_dm(str(usuario.id), "🎟️ Compra realizada", f"Has comprado {len(comprados)} boletos por ${precio_final} VP$")
    await enviar_dm(str(ctx.author.id), "💰 Venta realizada", f"Has vendido {len(comprados)} boletos a {usuario.name} por ${precio_final} VP$")
    
    await ctx.message.delete()
    await ctx.send(embed=embeds.crear_embed_exito(f"✅ Venta realizada. Revisa tu DM."))

@bot.command(name="misventas")
async def cmd_mis_ventas(ctx):
    if not await verificar_canal(ctx):
        return
    if not await check_vendedor(ctx):
        await ctx.send(embed=embeds.crear_embed_error("Sin permiso"))
        return
    
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        cursor = await db.execute('''
            SELECT b.numero, r.nombre as rifa, b.comprador_nick, b.precio_pagado, b.fecha_compra
            FROM boletos b
            JOIN rifas r ON b.rifa_id = r.id
            WHERE b.vendedor_id = ?
            ORDER BY b.fecha_compra DESC LIMIT 20
        ''', (str(ctx.author.id),))
        ventas = await cursor.fetchall()
        cursor = await db.execute('SELECT comisiones_pendientes FROM vendedores WHERE discord_id = ?', (str(ctx.author.id),))
        vendedor = await cursor.fetchone()
    
    embed = discord.Embed(title="💰 Tus ventas", color=COLORS['primary'])
    if vendedor and vendedor[0] > 0:
        embed.add_field(name="Comisiones pendientes", value=f"**${vendedor[0]}**", inline=False)
    if ventas:
        for v in ventas[:5]:
            embed.add_field(name=f"#{v['numero']} - {v['rifa']}", value=f"{v['comprador_nick']} | ${v['precio_pagado']} | {v['fecha_compra'][:10]}", inline=False)
    else:
        embed.description = "No tienes ventas"
    await ctx.send(embed=embed)

@bot.command(name="listaboletos")
async def cmd_lista_boletos(ctx):
    if not await verificar_canal(ctx):
        return
    if not await check_vendedor(ctx):
        await ctx.send(embed=embeds.crear_embed_error("Sin permiso"))
        return
    
    rifa_activa = await bot.db.get_rifa_activa()
    if not rifa_activa:
        await ctx.send(embed=embeds.crear_embed_error("No hay rifa"))
        return
    
    disponibles = await bot.db.get_boletos_disponibles(rifa_activa['id'])
    vendidos = await bot.db.get_boletos_vendidos(rifa_activa['id'])
    
    embed = discord.Embed(
        title=f"📋 Boletos - {rifa_activa['nombre']}",
        description=f"**Total:** {rifa_activa['total_boletos']}\n**Vendidos:** {vendidos}\n**Disponibles:** {len(disponibles)}",
        color=COLORS['info']
    )
    await ctx.send(embed=embed)

# ============================================
# SISTEMA DE CAJAS MISTERIOSAS
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
        cursor = await db.execute('''
            SELECT cc.id, c.nombre, c.tipo
            FROM cajas_compradas cc
            JOIN cajas c ON cc.caja_id = c.id
            WHERE cc.usuario_id = ? AND cc.abierta = 0
            ORDER BY cc.fecha_compra ASC
        ''', (str(ctx.author.id),))
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
        cursor = await db.execute('''
            SELECT cc.*, c.premios, c.probabilidades, c.nombre
            FROM cajas_compradas cc
            JOIN cajas c ON cc.caja_id = c.id
            WHERE cc.id = ? AND cc.usuario_id = ? AND cc.abierta = 0
        ''', (caja_id, str(ctx.author.id)))
        caja = await cursor.fetchone()
        
        if not caja:
            await ctx.send(embed=embeds.crear_embed_error("Caja no encontrada o ya abierta"))
            return
        
        premios = json.loads(caja['premios'])
        probabilidades = json.loads(caja['probabilidades'])
        elegido = random.choices(premios, weights=probabilidades, k=1)[0]
        
        await db.execute('UPDATE cajas_compradas SET abierta = 1, premio = ?, fecha_apertura = CURRENT_TIMESTAMP WHERE id = ?', (elegido, caja_id))
        if elegido > 0:
            await db.execute('''
                INSERT INTO usuarios_balance (discord_id, nombre, balance) VALUES (?, ?, ?)
                ON CONFLICT(discord_id) DO UPDATE SET balance = balance + ?
            ''', (str(ctx.author.id), ctx.author.name, elegido, elegido))
        await db.commit()
    
    if elegido > 0:
        embed = discord.Embed(title="🎉 CAJA ABIERTA", description=f"Has obtenido **${elegido:,} VP$**", color=COLORS['success'])
    else:
        embed = discord.Embed(title="😢 CAJA ABIERTA", description="No has ganado nada", color=COLORS['error'])
    await ctx.send(embed=embed)

# ============================================
# SISTEMA DE BANCO VP
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
    
    embed = discord.Embed(
        title="🏦 **BANCO VP**",
        description="Invierte tus VP$ y gana intereses",
        color=COLORS['primary']
    )
    embed.add_field(name="📈 Ahorro Básico", value="• Plazo: 7 días\n• Interés: 5%\n• Monto: 10k - 500k VP$", inline=False)
    embed.add_field(name="📈 Ahorro Plus", value="• Plazo: 14 días\n• Interés: 12%\n• Monto: 50k - 2M VP$", inline=False)
    embed.add_field(name="📈 Ahorro VIP", value="• Plazo: 30 días\n• Interés: 25%\n• Monto: 200k - 10M VP$", inline=False)
    embed.add_field(name="💱 Mercado de Cambio", value=f"• 1,000,000 NG$ → {int(1000000 * tasa_compra):,} VP$\n• 100,000 VP$ → {int(100000 * tasa_venta):,} NG$", inline=False)
    await ctx.send(embed=embed)

@bot.command(name="invertir")
async def cmd_invertir(ctx, producto: str, monto: int):
    if not await verificar_canal(ctx):
        return
    
    productos = {
        'basico': {'dias': 7, 'interes': 5, 'min': 10000, 'max': 500000},
        'plus': {'dias': 14, 'interes': 12, 'min': 50000, 'max': 2000000},
        'vip': {'dias': 30, 'interes': 25, 'min': 200000, 'max': 10000000}
    }
    
    if producto not in productos:
        await ctx.send(embed=embeds.crear_embed_error("Producto inválido. Usa: basico, plus, vip"))
        return
    
    prod = productos[producto]
    if monto < prod['min'] or monto > prod['max']:
        await ctx.send(embed=embeds.crear_embed_error(f"Monto entre {prod['min']:,} y {prod['max']:,} VP$"))
        return
    
    async with aiosqlite.connect(DB_PATH) as db:
        cursor = await db.execute('SELECT balance FROM usuarios_balance WHERE discord_id = ?', (str(ctx.author.id),))
        result = await cursor.fetchone()
        balance = result[0] if result else 0
        
        if balance < monto:
            await ctx.send(embed=embeds.crear_embed_error(f"Necesitas {monto:,} VP$"))
            return
        
        fecha_fin = datetime.now() + timedelta(days=prod['dias'])
        await db.execute('UPDATE usuarios_balance SET balance = balance - ? WHERE discord_id = ?', (monto, str(ctx.author.id)))
        await db.execute('INSERT INTO inversiones (usuario_id, producto, monto, interes, fecha_fin) VALUES (?, ?, ?, ?, ?)', (str(ctx.author.id), producto, monto, prod['interes'], fecha_fin))
        await db.commit()
    
    embed = discord.Embed(title="✅ Inversión realizada", description=f"Has invertido **{monto:,} VP$** en **Ahorro {producto.upper()}**", color=COLORS['success'])
    embed.add_field(name="📅 Plazo", value=f"{prod['dias']} días", inline=True)
    embed.add_field(name="💰 Interés", value=f"{prod['interes']}%", inline=True)
    embed.add_field(name="💵 Retiro", value=f"≈ {int(monto * (1 + prod['interes']/100)):,} VP$", inline=True)
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
        embed.add_field(name=f"#{inv['id']} - Ahorro {inv['producto'].upper()}", value=f"Monto: ${inv['monto']:,}\nInterés: {inv['interes']}%\nRetiro: {dias_restantes} días", inline=False)
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
        comision = int(ganancia_bruta * 5 / 100)
        ganancia_neta = ganancia_bruta - comision
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

@bot.command(name="cambiar")
async def cmd_cambiar(ctx, moneda: str, cantidad: int):
    if not await verificar_canal(ctx):
        return
    if cantidad <= 0:
        await ctx.send(embed=embeds.crear_embed_error("Cantidad positiva"))
        return
    
    async with aiosqlite.connect(DB_PATH) as db:
        cursor = await db.execute('SELECT tasa_compra, tasa_venta FROM banco_config WHERE id = 1')
        config_data = await cursor.fetchone()
        tasa_compra = config_data[0] if config_data else 0.9
        tasa_venta = config_data[1] if config_data else 1.1
    
    if moneda.lower() == 'ng':
        cantidad_vp = int(cantidad * tasa_compra)
        await db.execute('INSERT INTO pagos_pendientes (usuario_id, monto, metodo, estado) VALUES (?, ?, "cambio_ng", "pendiente")', (str(ctx.author.id), cantidad))
        await db.commit()
        embed = discord.Embed(title="⏳ Cambio solicitado", description=f"Solicitaste cambiar **{cantidad:,} NG$** por **{cantidad_vp:,} VP$**", color=COLORS['info'])
        embed.set_footer(text="Un staff verificará tu pago")
        await ctx.send(embed=embed)
        
    elif moneda.lower() == 'vp':
        cantidad_ng = int(cantidad * tasa_venta)
        cursor = await db.execute('SELECT balance FROM usuarios_balance WHERE discord_id = ?', (str(ctx.author.id),))
        result = await cursor.fetchone()
        balance = result[0] if result else 0
        
        if balance < cantidad:
            await ctx.send(embed=embeds.crear_embed_error(f"Necesitas {cantidad:,} VP$"))
            return
        
        await db.execute('UPDATE usuarios_balance SET balance = balance - ? WHERE discord_id = ?', (cantidad, str(ctx.author.id)))
        await db.execute('INSERT INTO transacciones_cambio (usuario_id, tipo, cantidad, comision) VALUES (?, "vp_a_ng", ?, ?)', (str(ctx.author.id), cantidad_ng, cantidad_ng - cantidad))
        await db.commit()
        
        embed = discord.Embed(title="✅ Cambio realizado", description=f"Has cambiado **{cantidad:,} VP$** por **{cantidad_ng:,} NG$**", color=COLORS['success'])
        await ctx.send(embed=embed)
        await enviar_dm(str(ctx.author.id), "💰 Cambio de moneda", f"Has cambiado {cantidad} VP$ por {cantidad_ng} NG$")
    else:
        await ctx.send(embed=embeds.crear_embed_error("Moneda inválida. Usa: ng o vp"))

# ============================================
# SISTEMA DE MISIONES DIARIAS
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
            cursor = await db.execute('SELECT progreso, completada FROM progreso_misiones WHERE usuario_id = ? AND mision_id = ?', (str(ctx.author.id), m['id']))
            result = await cursor.fetchone()
            if result:
                progreso[m['id']] = {'progreso': result[0], 'completada': result[1]}
            else:
                progreso[m['id']] = {'progreso': 0, 'completada': False}
        
        cursor = await db.execute('SELECT racha FROM rachas WHERE usuario_id = ?', (str(ctx.author.id),))
        racha_result = await cursor.fetchone()
        racha = racha_result[0] if racha_result else 0
    
    embed = discord.Embed(title="📋 MISIONES DIARIAS", description=f"Racha actual: **{racha}** días 🔥", color=COLORS['primary'])
    for m in misiones:
        estado = "✅" if progreso[m['id']]['completada'] else "⏳"
        texto = f"{estado} {m['descripcion']}\nRecompensa: ${m['recompensa']:,} VP$"
        if not progreso[m['id']]['completada']:
            texto += f"\nProgreso: {progreso[m['id']]['progreso']}/{m['valor_requisito']}"
        embed.add_field(name=m['nombre'], value=texto, inline=False)
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
    bonos = [(3, "+500 VP$"), (7, "+2,000 VP$"), (14, "+10,000 VP$"), (30, "+50,000 VP$ + rol exclusivo")]
    texto_bonos = ""
    for dias, bono in bonos:
        if racha < dias:
            texto_bonos += f"• En {dias} días: {bono}\n"
    if texto_bonos:
        embed.add_field(name="🎁 Próximos bonos", value=texto_bonos, inline=False)
    await ctx.send(embed=embed)

# ============================================
# COMANDOS DE DISTRIBUIDORES
# ============================================

@bot.command(name="distribuidor")
async def cmd_distribuidor(ctx):
    if not await verificar_canal(ctx):
        return
    
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        cursor = await db.execute('SELECT * FROM distribuidores WHERE discord_id = ?', (str(ctx.author.id),))
        distribuidor = await cursor.fetchone()
        if not distribuidor:
            await ctx.send(embed=embeds.crear_embed_error("No eres distribuidor. Contacta al CEO"))
            return
        
        nivel_franquicia = 0
        for nivel, datos in ROLES_FRANQUICIA.items():
            if tiene_rol(ctx.author, datos['rol_id']):
                nivel_franquicia = nivel
                break
    
    embed = discord.Embed(title="📦 Tu perfil de distribuidor", color=COLORS['primary'])
    letra_nivel = {1: 'A', 2: 'B', 3: 'C', 4: 'D', 5: 'E', 6: 'F'}.get(distribuidor['nivel'], 'A')
    embed.add_field(name="📊 Nivel", value=f"{letra_nivel} (Nivel {distribuidor['nivel']})", inline=True)
    embed.add_field(name="🏆 Franquicia", value=f"Nivel {nivel_franquicia}" if nivel_franquicia else "Sin franquicia", inline=True)
    embed.add_field(name="💰 Comisión", value=f"{distribuidor['comision']}%", inline=True)
    embed.add_field(name="📈 Ventas totales", value=f"{distribuidor['ventas_totales']}", inline=True)
    embed.add_field(name="⏳ Comisiones pendientes", value=f"${distribuidor['comisiones_pendientes']:,} VP$", inline=True)
    embed.add_field(name="✅ Comisiones pagadas", value=f"${distribuidor['comisiones_pagadas']:,} VP$", inline=True)
    await ctx.send(embed=embed)

@bot.command(name="productos")
async def cmd_productos(ctx):
    if not await verificar_canal(ctx):
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
    if not await verificar_canal(ctx):
        return
    if not await check_distribuidor(ctx, 1):
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
        distribuidor = await cursor.fetchone()
        nivel_distribuidor = distribuidor[0] if distribuidor else 1
        
        if nivel_distribuidor < producto['nivel_minimo']:
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
        inventario = await cursor.fetchone()
        if inventario:
            await db.execute('UPDATE inventario_distribuidor SET cantidad = cantidad + ? WHERE distribuidor_id = ? AND producto_id = ?', (cantidad, str(ctx.author.id), producto['id']))
        else:
            await db.execute('INSERT INTO inventario_distribuidor (distribuidor_id, producto_id, cantidad) VALUES (?, ?, ?)', (str(ctx.author.id), producto['id'], cantidad))
        await db.commit()
    
    embed = discord.Embed(title="✅ Compra realizada", description=f"Has comprado {cantidad}x {producto['nombre']} por ${precio_total:,} VP$", color=COLORS['success'])
    await ctx.send(embed=embed)

@bot.command(name="mis_productos")
async def cmd_mis_productos(ctx):
    if not await verificar_canal(ctx):
        return
    if not await check_distribuidor(ctx, 1):
        await ctx.send(embed=embeds.crear_embed_error("No tienes permiso"))
        return
    
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        cursor = await db.execute('''
            SELECT i.cantidad, p.nombre, p.precio_normal
            FROM inventario_distribuidor i
            JOIN productos p ON i.producto_id = p.id
            WHERE i.distribuidor_id = ? AND i.cantidad > 0
        ''', (str(ctx.author.id),))
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
# COMANDOS DE ADMINISTRACIÓN (CEO)
# ============================================

@bot.command(name="acreditarvp")
async def cmd_acreditarvp(ctx, usuario: discord.Member, cantidad: int):
    if not await check_ceo(ctx):
        return
    if cantidad <= 0:
        await ctx.send(embed=embeds.crear_embed_error("Cantidad positiva"))
        return
    
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute('''
            INSERT INTO usuarios_balance (discord_id, nombre, balance) VALUES (?, ?, ?)
            ON CONFLICT(discord_id) DO UPDATE SET balance = balance + ?
        ''', (str(usuario.id), usuario.name, cantidad, cantidad))
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
        cursor = await db.execute('SELECT id, monto FROM pagos_pendientes WHERE usuario_id = ? AND estado = "pendiente" ORDER BY fecha DESC LIMIT 1', (str(usuario.id),))
        pago = await cursor.fetchone()
        if not pago:
            await ctx.send(embed=embeds.crear_embed_error(f"{usuario.name} no tiene pagos pendientes"))
            return
        await db.execute('UPDATE pagos_pendientes SET estado = "procesando" WHERE id = ?', (pago[0],))
        await db.commit()
    
    embed = discord.Embed(title="⏳ Pago en proceso", description=f"Pago de **${pago[1]:,} NG$** de {usuario.mention} en proceso", color=COLORS['info'])
    await ctx.send(embed=embed)
    await enviar_dm(str(usuario.id), "💰 Pago en proceso", f"Tu pago de {pago[1]:,} NG$ está siendo procesado")

@bot.command(name="procesadovp")
async def cmd_procesado_vp(ctx, usuario: discord.Member, cantidad: int):
    if not await check_ceo(ctx):
        return
    
    async with aiosqlite.connect(DB_PATH) as db:
        cursor = await db.execute('SELECT tasa_compra FROM banco_config WHERE id = 1')
        config_data = await cursor.fetchone()
        tasa = config_data[0] if config_data else 0.9
        cantidad_vp = int(cantidad * tasa)
        
        await db.execute('''
            INSERT INTO usuarios_balance (discord_id, nombre, balance) VALUES (?, ?, ?)
            ON CONFLICT(discord_id) DO UPDATE SET balance = balance + ?
        ''', (str(usuario.id), usuario.name, cantidad_vp, cantidad_vp))
        await db.execute('UPDATE pagos_pendientes SET estado = "completado" WHERE usuario_id = ?', (str(usuario.id),))
        await db.commit()
    
    embed = discord.Embed(title="✅ Pago procesado", description=f"Se acreditaron **${cantidad_vp:,} VP$** a {usuario.mention}", color=COLORS['success'])
    embed.add_field(name="💰 Monto original", value=f"{cantidad:,} NG$", inline=True)
    embed.add_field(name="💵 VP$ acreditados", value=f"{cantidad_vp:,} VP$", inline=True)
    await ctx.send(embed=embed)
    await enviar_dm(str(usuario.id), "💰 Pago confirmado", f"Has recibido {cantidad_vp:,} VP$ en tu balance")

@bot.command(name="verboletos")
async def cmd_ver_boletos(ctx, usuario: discord.Member):
    if not await check_ceo(ctx):
        return
    
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        cursor = await db.execute('''
            SELECT b.numero, r.nombre as rifa, b.precio_pagado, b.fecha_compra
            FROM boletos b
            JOIN rifas r ON b.rifa_id = r.id
            WHERE b.comprador_id = ?
            ORDER BY b.fecha_compra DESC
        ''', (str(usuario.id),))
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
        cursor = await db.execute('''
            SELECT b.numero, r.nombre as rifa, b.comprador_nick, b.precio_pagado, b.fecha_compra
            FROM boletos b
            JOIN rifas r ON b.rifa_id = r.id
            ORDER BY b.fecha_compra DESC
        ''')
        boletos = await cursor.fetchall()
    
    with open(archivo, 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(['Número', 'Rifa', 'Comprador', 'Precio', 'Fecha'])
        for b in boletos:
            writer.writerow([b['numero'], b['rifa'], b['comprador_nick'], b['precio_pagado'], b['fecha_compra']])
    
    await ctx.author.send(file=discord.File(archivo))
    await ctx.message.delete()
    await ctx.send(embed=embeds.crear_embed_exito("✅ Datos exportados. Revisa tu DM."))

@bot.command(name="backup")
async def cmd_backup(ctx):
    if not await check_ceo(ctx):
        return
    
    fecha = datetime.now().strftime("%Y%m%d_%H%M%S")
    backup = f"/tmp/backup_{fecha}.db"
    shutil.copy2(DB_PATH, backup)
    
    await ctx.author.send(file=discord.File(backup))
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
        await db.execute("DELETE FROM sqlite_sequence")
        await db.commit()
    
    reset_pending = None
    await ctx.message.delete()
    await ctx.send(embed=embeds.crear_embed_exito("✅ Sistema reiniciado"))

# ============================================
# COMANDOS DE EVENTOS
# ============================================

@bot.command(name="2x1")
async def cmd_2x1(ctx):
    if not await check_ceo(ctx):
        return
    global evento_2x1
    evento_2x1 = not evento_2x1
    estado = "ACTIVADO" if evento_2x1 else "DESACTIVADO"
    await ctx.message.delete()
    await ctx.send(embed=embeds.crear_embed_exito(f"🎉 Evento 2x1 {estado}"))

@bot.command(name="cashbackdoble")
async def cmd_cashback_doble(ctx):
    if not await check_ceo(ctx):
        return
    global evento_cashback_doble
    evento_cashback_doble = not evento_cashback_doble
    estado = "ACTIVADO" if evento_cashback_doble else "DESACTIVADO"
    await ctx.message.delete()
    await ctx.send(embed=embeds.crear_embed_exito(f"🎉 Cashback doble {estado}"))

@bot.command(name="oferta")
async def cmd_oferta(ctx, porcentaje: int):
    if not await check_ceo(ctx):
        return
    global evento_oferta_activa, evento_oferta_porcentaje
    if porcentaje < 0 or porcentaje > 30:
        await ctx.send(embed=embeds.crear_embed_error("0-30%"))
        return
    evento_oferta_activa = True
    evento_oferta_porcentaje = porcentaje
    await ctx.message.delete()
    await ctx.send(embed=embeds.crear_embed_exito(f"🎉 Oferta activada: {porcentaje}% de descuento adicional"))

@bot.command(name="ofertadesactivar")
async def cmd_oferta_desactivar(ctx):
    if not await check_ceo(ctx):
        return
    global evento_oferta_activa, evento_oferta_porcentaje
    evento_oferta_activa = False
    evento_oferta_porcentaje = 0
    await ctx.message.delete()
    await ctx.send(embed=embeds.crear_embed_exito("🎉 Oferta desactivada"))

# ============================================
# COMANDOS DE PROMOCIONES
# ============================================

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
        await db.execute('''
            INSERT INTO usuarios_balance (discord_id, nombre, balance) VALUES (?, ?, ?)
            ON CONFLICT(discord_id) DO UPDATE SET balance = balance + ?
        ''', (str(ctx.author.id), ctx.author.name, recompensa, recompensa))
        await db.commit()
    await enviar_dm(str(ctx.author.id), "🎁 Código canjeado", f"Has canjeado el código `{codigo}` y recibido ${recompensa} VP$")
    await ctx.message.delete()
    await ctx.send(embed=embeds.crear_embed_exito(f"✅ Código canjeado. Revisa tu DM."))

# ============================================
# COMANDOS DE SORTEO Y RIFAS
# ============================================

@bot.command(name="crearifa")
async def cmd_crear_rifa(ctx, premio: str, precio: int, total: int, bloqueados: str = None):
    if not await check_admin(ctx):
        return
    
    nombre = f"Rifa {datetime.now().strftime('%d/%m')}"
    
    async with aiosqlite.connect(DB_PATH) as db:
        cursor = await db.execute('''
            INSERT INTO rifas (nombre, premio, valor_premio, precio_boleto, total_boletos, numeros_bloqueados, estado)
            VALUES (?, ?, ?, ?, ?, ?, 'activa')
        ''', (nombre, premio, precio, precio, total, bloqueados))
        rifa_id = cursor.lastrowid
        await db.commit()
    
    embed = embeds.crear_embed_exito(f"✅ Rifa creada ID: {rifa_id}\nPremio: {premio}\nPrecio: ${precio}\nTotal: {total} boletos")
    if bloqueados:
        embed.add_field(name="🔒 Números VIP", value=f"{bloqueados} (solo venta staff)", inline=False)
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

@bot.command(name="cancelarsorteo")
async def cmd_cancelar_sorteo(ctx):
    if not await check_admin(ctx):
        return
    
    global sorteo_en_curso, sorteo_cancelado
    sorteo_cancelado = True
    sorteo_en_curso = False
    await ctx.message.delete()
    await ctx.send(embed=embeds.crear_embed_exito("Sorteo cancelado"))

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

# ============================================
# COMANDOS DE VENDEDORES (ADMIN)
# ============================================

@bot.command(name="vendedoradd")
async def cmd_vendedor_add(ctx, usuario: discord.Member, comision: int = 10):
    if not await check_admin(ctx):
        return
    
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute('''
            INSERT INTO vendedores (discord_id, nombre, comision) VALUES (?, ?, ?)
            ON CONFLICT(discord_id) DO UPDATE SET comision = ?, nombre = ?
        ''', (str(usuario.id), usuario.name, comision, comision, usuario.name))
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
            await db.execute('''
                INSERT INTO usuarios_balance (discord_id, nombre, balance) VALUES (?, ?, ?)
                ON CONFLICT(discord_id) DO UPDATE SET balance = balance + ?
            ''', (v['discord_id'], v['nombre'], monto, monto))
            await db.execute('INSERT INTO transacciones (tipo, monto, destino_id, descripcion) VALUES ("comision", ?, ?, ?)', (monto, v['discord_id'], "Pago de comisiones"))
            await db.execute('UPDATE vendedores SET comisiones_pendientes = 0, comisiones_pagadas = comisiones_pagadas + ? WHERE discord_id = ?', (monto, v['discord_id']))
            total_pagado += monto
            await enviar_dm(v['discord_id'], "💰 Comisiones pagadas", f"Has recibido ${monto} VP$ por tus ventas")
        await db.commit()
    
    await ctx.message.delete()
    await ctx.send(embed=embeds.crear_embed_exito(f"Pagadas ${total_pagado} VP$ en comisiones a {len(vendedores)} vendedores"))

@bot.command(name="setcomision")
async def cmd_set_comision(ctx, porcentaje: int):
    if not await check_admin(ctx):
        return
    if porcentaje < 0 or porcentaje > 30:
        await ctx.send(embed=embeds.crear_embed_error("0-30%"))
        return
    global COMISION_VENDEDOR
    COMISION_VENDEDOR = porcentaje
    await ctx.message.delete()
    await ctx.send(embed=embeds.crear_embed_exito(f"Comisión vendedores: {porcentaje}%"))

# ============================================
# COMANDOS DE REPORTE Y ALERTA
# ============================================

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

# ============================================
# SISTEMA DE JACKPOT
# ============================================

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
            await db2.execute('''
                INSERT INTO usuarios_balance (discord_id, nombre, balance) VALUES (?, ?, ?)
                ON CONFLICT(discord_id) DO UPDATE SET balance = balance + ?
            ''', (ganador[0], ganador[1], premio_por_ganador, premio_por_ganador))
        await enviar_dm(ganador[0], "🎰 GANASTE EL JACKPOT!", f"Has ganado ${premio_por_ganador} VP$ del jackpot")
    await ctx.send(embed=embed)
    
    jackpot_activo = False
    jackpot_total = 0

# ============================================
# SISTEMA DE RIFA ELIMINACIÓN
# ============================================

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

# ============================================
# SISTEMA DE PUNTOS REVANCHA
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

# ============================================
# COMANDOS DE FRANQUICIA
# ============================================

@bot.command(name="franquicia")
async def cmd_franquicia(ctx):
    if not await verificar_canal(ctx, CATEGORIA_FRANQUICIAS):
        return
    if not await check_franquicia(ctx):
        await ctx.send(embed=embeds.crear_embed_error("No tienes una franquicia activa"))
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
    if not await verificar_canal(ctx, CATEGORIA_FRANQUICIAS):
        return
    if not await check_franquicia(ctx):
        await ctx.send(embed=embeds.crear_embed_error("No tienes una franquicia activa"))
        return
    
    nombre = f"Rifa {ctx.author.name} - {datetime.now().strftime('%d/%m')}"
    
    async with aiosqlite.connect(DB_PATH) as db:
        cursor = await db.execute('''
            INSERT INTO rifas (nombre, premio, valor_premio, precio_boleto, total_boletos, estado)
            VALUES (?, ?, ?, ?, ?, 'activa')
        ''', (nombre, premio, precio, precio, total))
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
    if not await verificar_canal(ctx, CATEGORIA_FRANQUICIAS):
        return
    if not await check_franquicia(ctx):
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
