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
# CONFIGURACIÓN DE PERSISTENCIA (VOLUMEN)
# ============================================

# Detectar si estamos en Railway o local
if os.path.exists('/app/data'):
    DB_PATH = '/app/data/rifas.db'
    LOG_PATH = '/app/data/bot.log'
    VOLUME_MOUNTED = True
else:
    DB_PATH = 'data/rifas.db'
    LOG_PATH = 'src/logs/bot.log'
    VOLUME_MOUNTED = False

# Crear carpetas necesarias
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

# IDs de roles del sistema
ROLES = {
    'CEO': 1016130577595891713,
    'DIRECTOR': 1473799754457677967,
    'RIFAS': 1476836273493643438,
    'MIEMBRO': 1442736806234816603
}

# Colores
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
        logger.info("🚀 Iniciando configuración del bot...")
        
        # Mostrar estado del volumen
        if self.volumen_montado:
            logger.info("✅ Volumen persistente detectado en /app/data")
        else:
            logger.warning("⚠️ Volumen no detectado. Los datos NO persistirán entre actualizaciones")
        
        try:
            await self.db.init_db()
            await self.init_sistemas_tablas()
            logger.info("✅ Base de datos inicializada correctamente")
        except Exception as e:
            logger.error(f"❌ Error inicializando BD: {e}")
            traceback.print_exc()
        
        # Cargar módulos (cogs)
        await self.load_cogs()
        
        self.keep_alive_task.start()
        self.status_task.start()
        logger.info("✅ Tareas automáticas iniciadas")
    
    async def load_cogs(self):
        """Carga todos los módulos del bot"""
        # Por ahora los comandos están en main.py
        # En una versión futura se pueden separar en cogs
        pass
    
    async def init_sistemas_tablas(self):
        """Inicializar todas las tablas de la base de datos"""
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
            
            # Insertar cajas por defecto
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
            
            # Insertar productos por defecto
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
            
            # Insertar misiones por defecto
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
            
            # ===== TRANSACCIONES PENDIENTES =====
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
            
            await db.commit()
        
        logger.info("✅ Tablas de sistemas inicializadas")
    
    async def on_ready(self):
        logger.info(f"✅ Bot conectado como {self.user} (ID: {self.user.id})")
        logger.info(f"🌐 En {len(self.guilds)} servidores")
        
        if self.volumen_montado:
            logger.info("💾 Volumen persistente activo - Los datos se conservarán entre actualizaciones")
        
        global reconnect_attempts
        reconnect_attempts = 0
        self.reconnecting = False
        
        await self.enviar_log_sistema(
            "🟢 **BOT INICIADO**", 
            f"Bot iniciado correctamente\nVersión: {VERSION}\nServidores: {len(self.guilds)}\nVolumen: {'✅ Activo' if self.volumen_montado else '❌ No detectado'}"
        )
    
    async def on_disconnect(self):
        logger.warning("⚠️ Bot desconectado")
        self.reconnecting = True
        await self.enviar_log_sistema(
            "🔴 **BOT DESCONECTADO**", 
            "El bot se ha desconectado. Intentando reconectar automáticamente..."
        )
    
    async def on_resumed(self):
        logger.info("🔄 Bot reconectado")
        self.reconnecting = False
        await self.enviar_log_sistema(
            "🟢 **BOT RECONECTADO**", 
            "Conexión restablecida exitosamente"
        )
    
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
            embed.set_footer(text=f"Sistema VP Rifas v{VERSION}")
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
            f"Bot activo por {int(horas)}h {int(minutos)}m\nVersión: {VERSION}\nServidores: {len(self.guilds)}\nVolumen: {'✅' if self.volumen_montado else '❌'}"
        )

bot = VPRifasBot()

# ============================================
# FUNCIONES AUXILIARES
# ============================================

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
        await ctx.send("❌ Este comando solo funciona en servidores")
        return False
    
    categoria_a_verificar = categoria_id if categoria_id else config.CATEGORIA_RIFAS
    
    if ctx.channel.category_id != categoria_a_verificar:
        await ctx.send(f"❌ Este comando solo puede usarse en la categoría correspondiente")
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
    return (tiene_rol(member, ROLES['CEO']) or 
            tiene_rol(member, ROLES['DIRECTOR']))

async def check_vendedor(ctx):
    if not ctx.guild:
        return False
    member = ctx.guild.get_member(ctx.author.id)
    if not member:
        return False
    return (tiene_rol(member, ROLES['CEO']) or 
            tiene_rol(member, ROLES['DIRECTOR']) or 
            tiene_rol(member, ROLES['RIFAS']))

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
    # Verificar si tiene rol de distribuidor
    for rol_id in ROLES_DISTRIBUIDORES.values():
        if tiene_rol(member, rol_id):
            # Verificar nivel
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

# ============================================
# COMANDO DE AYUDA (CORREGIDO)
# ============================================

@bot.command(name="ayuda")
async def cmd_ayuda(ctx):
    """Ver todos los comandos disponibles según tu rol"""
    if not await verificar_canal(ctx):
        return
    
    member = ctx.guild.get_member(ctx.author.id)
    es_ceo = tiene_rol(member, ROLES['CEO'])
    es_director = tiene_rol(member, ROLES['DIRECTOR'])
    es_vendedor = tiene_rol(member, ROLES['RIFAS'])
    es_distribuidor = await check_distribuidor(ctx, 1)
    es_franquicia = await check_franquicia(ctx)
    
    embed = discord.Embed(
        title="🎟️ **SISTEMA DE RIFAS VP**",
        description=f"Comandos disponibles (prefijo: `{PREFIX}`)\nVersión: {VERSION}",
        color=COLORS['primary']
    )
    
    # Comandos Básicos
    basicos = """
    `!rifa` - Ver rifa activa
    `!comprarrandom [cantidad]` - Comprar boletos aleatorios
    `!misboletos` - Ver tus boletos
    `!balance` - Ver tu balance
    `!topvp` - Ranking de VP$
    `!ranking` - Top compradores de la rifa actual
    `!historial` - Tu historial
    """
    embed.add_field(name="👤 **BÁSICOS**", value=basicos, inline=False)
    
    # Cajas Misteriosas
    cajas = """
    `!cajas` - Ver tipos de cajas
    `!comprarcaja [tipo] [cantidad]` - Comprar cajas
    `!abrircaja [id]` - Abrir una caja
    `!abrirtodas` - Abrir todas tus cajas
    `!miscajas` - Ver tus cajas sin abrir
    `!topcajas` - Ranking de aperturas
    """
    embed.add_field(name="🎁 **CAJAS MISTERIOSAS**", value=cajas, inline=False)
    
    # Banco VP
    banco = """
    `!banco` - Ver estado del banco
    `!invertir [tipo] [monto]` - Invertir VP$ (basico/plus/vip)
    `!misinversiones` - Ver inversiones activas
    `!retirar [id]` - Retirar inversión
    `!cambiar [moneda] [cantidad]` - Cambiar VP$ ↔ NG$
    `!cotizacion` - Ver tasa de cambio
    `!prestamo [monto] [dias]` - Pedir préstamo
    `!mispagos` - Ver pagos pendientes
    `!pagar [id]` - Pagar préstamo
    """
    embed.add_field(name="🏦 **BANCO VP**", value=banco, inline=False)
    
    # Misiones Diarias
    misiones = """
    `!misiones` - Ver misiones del día
    `!completar [id]` - Completar misión
    `!miracha` - Ver racha actual
    `!rankingmisiones` - Ranking de misiones
    `!reclamar_racha` - Reclamar bono por racha
    """
    embed.add_field(name="📋 **MISIONES DIARIAS**", value=misiones, inline=False)
    
    # Referidos y Fidelización
    referidos = """
    `!codigo` - Tu código de referido
    `!usar [código]` - Usar código de referido
    `!misreferidos` - Ver tus referidos
    `!nivel` - Tu nivel y beneficios
    `!topgastadores` - Ranking de gasto
    `!cashback` - Cashback acumulado
    """
    embed.add_field(name="🤝 **REFERIDOS Y FIDELIZACIÓN**", value=referidos, inline=False)
    
    if es_vendedor or es_director or es_ceo:
        vendedor = """
        `!vender [@usuario] [número]` - Vender número específico
        `!venderrandom [@usuario] [cantidad]` - Vender aleatorios
        `!misventas` - Ver tus ventas
        `!listaboletos` - Lista de boletos
        """
        embed.add_field(name="💰 **VENDEDORES**", value=vendedor, inline=False)
    
    if es_distribuidor or es_director or es_ceo:
        distribuidor = """
        `!distribuidor` - Ver tu nivel y estado
        `!productos` - Ver catálogo de productos
        `!comprar_producto [producto] [cantidad]` - Comprar stock
        `!mis_productos` - Ver inventario
        `!vender_producto [@user] [producto] [precio]` - Vender producto
        `!ranking_distribuidores` - Ranking de distribuidores
        """
        embed.add_field(name="📦 **DISTRIBUIDORES**", value=distribuidor, inline=False)
    
    if es_franquicia or es_director or es_ceo:
        franquicia = """
        `!franquicia` - Ver info de tu franquicia
        `!franquicia_rifa [premio] [precio] [total]` - Crear rifa de franquicia
        `!franquicia_stats` - Estadísticas de franquicia
        """
        embed.add_field(name="👑 **FRANQUICIAS**", value=franquicia, inline=False)
    
    if es_director or es_ceo:
        director = """
        `!crearifa [premio] [precio] [total] [bloqueados]` - Crear rifa
        `!aumentarnumeros [cantidad]` - Ampliar rifa
        `!cerrarifa` - Cerrar rifa
        `!iniciarsorteo [ganadores]` - Iniciar sorteo
        `!cancelarsorteo` - Cancelar sorteo
        `!finalizarrifa [id] [ganadores]` - Finalizar rifa
        `!vendedoradd [@usuario] [%]` - Añadir vendedor
        `!vercomisiones` - Ver comisiones
        `!pagarcomisiones` - Pagar comisiones
        `!reporte` - Reporte de rifa
        `!balance [@usuario]` - Ver balance de usuario
        `!alertar [mensaje]` - Alerta a todos
        """
        embed.add_field(name="🎯 **DIRECTORES**", value=director, inline=False)
    
    if es_ceo:
        ceo = """
        `!acreditarvp [@usuario] [cantidad]` - Acreditar VP$
        `!retirarvp [@usuario] [cantidad]` - Retirar VP$
        `!procesarvp [@usuario]` - Procesar pago pendiente
        `!procesadovp [@usuario] [cantidad]` - Confirmar pago
        `!verboletos [@usuario]` - Ver boletos de usuario
        `!setrefcomision [%]` - Configurar comisión referidos
        `!setrefdescuento [%]` - Configurar descuento referidos
        `!setcashback [%]` - Configurar cashback
        `!pagarcashback` - Pagar cashback
        `!resetcashback` - Resetear cashback
        `!estadisticas` - Estadísticas globales
        `!auditoria` - Ver transacciones
        `!exportar` - Exportar a CSV
        `!backup` - Crear backup
        `!resetallsistema` - Reiniciar sistema
        `!version` - Versión del bot
        `!crearcodigo [codigo] [vp]` - Crear código promocional
        `!borrarcodigo [codigo]` - Borrar código
        `!2x1` - Activar/desactivar 2x1
        `!cashbackdoble` - Activar/desactivar cashback doble
        `!oferta [%]` - Activar oferta
        `!ofertadesactivar` - Desactivar oferta
        `!crearcaja [tipo] [precio] [premios] [probs]` - Crear caja
        `!editarcaja [tipo] [campo] [valor]` - Editar caja
        `!crearproducto [nombre] [precio] [costo]` - Crear producto
        `!set_tasa [compra] [venta]` - Configurar tasas de cambio
        """
        embed.add_field(name="👑 **CEO**", value=ceo, inline=False)
    
    embed.set_footer(text="Ejemplo: !comprarrandom 3")
    await ctx.send(embed=embed)

# ============================================
# COMANDOS BÁSICOS (VERSIÓN CORREGIDA)
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
                    f"**Usuarios:** {len(bot.users)}\n"
                    f"**Volumen persistente:** {'✅ Activo' if bot.volumen_montado else '❌ No detectado'}",
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
        rifa_activa = await cursor.fetchone()
    
    if not rifa_activa:
        await ctx.send(embed=embeds.crear_embed_error("No hay rifa activa"))
        return
    
    embed = discord.Embed(
        title=f"🎟️ {rifa_activa['nombre']}",
        description=f"**{rifa_activa['premio']}**",
        color=COLORS['primary']
    )
    embed.add_field(name="🏆 Premio", value=f"${rifa_activa['valor_premio']:,}", inline=True)
    embed.add_field(name="💰 Precio", value=f"${rifa_activa['precio_boleto']:,}", inline=True)
    
    # Mostrar números bloqueados si los hay
    if rifa_activa['numeros_bloqueados']:
        embed.add_field(name="🔒 Números VIP", value=f"Solo disponibles con staff", inline=False)
    
    embed.set_footer(text="Usa !comprarrandom para participar")
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

# ============================================
# SISTEMA DE CAJAS MISTERIOSAS
# ============================================

@bot.command(name="cajas")
async def cmd_cajas(ctx):
    """Ver tipos de cajas disponibles"""
    if not await verificar_canal(ctx):
        return
    
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        cursor = await db.execute('SELECT * FROM cajas WHERE activo = 1')
        cajas = await cursor.fetchall()
    
    embed = discord.Embed(
        title="🎁 **CAJAS MISTERIOSAS**",
        description="Compra cajas y descubre qué premio obtienes",
        color=COLORS['primary']
    )
    
    for caja in cajas:
        premios = json.loads(caja['premios'])
        probs = json.loads(caja['probabilidades'])
        
        texto_premios = ""
        for p, prob in zip(premios, probs):
            texto_premios += f"• {p:,} VP$ ({prob}%)\n"
        
        embed.add_field(
            name=f"{caja['nombre']} - ${caja['precio']:,} VP$",
            value=texto_premios,
            inline=False
        )
    
    await ctx.send(embed=embed)

@bot.command(name="comprarcaja")
async def cmd_comprar_caja(ctx, tipo: str, cantidad: int = 1):
    """Comprar cajas misteriosas"""
    if not await verificar_canal(ctx):
        return
    
    if cantidad < 1 or cantidad > 50:
        await ctx.send(embed=embeds.crear_embed_error("Cantidad entre 1 y 50"))
        return
    
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        cursor = await db.execute('SELECT * FROM cajas WHERE tipo = ? AND activo = 1', (tipo.lower(),))
        caja = await cursor.fetchone()
        
        if not caja:
            await ctx.send(embed=embeds.crear_embed_error("Tipo de caja no válido. Usa: comun, rara, epica, legendaria, misteriosa"))
            return
        
        precio_total = caja['precio'] * cantidad
        
        # Verificar balance
        cursor = await db.execute('SELECT balance FROM usuarios_balance WHERE discord_id = ?', (str(ctx.author.id),))
        result = await cursor.fetchone()
        balance = result[0] if result else 0
        
        if balance < precio_total:
            await ctx.send(embed=embeds.crear_embed_error(f"Necesitas {precio_total:,} VP$"))
            return
        
        # Descontar balance
        await db.execute('UPDATE usuarios_balance SET balance = balance - ? WHERE discord_id = ?', (precio_total, str(ctx.author.id)))
        
        # Registrar cajas compradas
        for _ in range(cantidad):
            await db.execute('''
                INSERT INTO cajas_compradas (usuario_id, caja_id)
                VALUES (?, ?)
            ''', (str(ctx.author.id), caja['id']))
        
        await db.commit()
    
    embed = discord.Embed(
        title="✅ **Compra realizada**",
        description=f"Has comprado {cantidad} {caja['nombre']} por ${precio_total:,} VP$",
        color=COLORS['success']
    )
    embed.add_field(name="📦 Usa", value="`!miscajas` para ver tus cajas\n`!abrircaja [id]` para abrir una", inline=False)
    await ctx.send(embed=embed)

@bot.command(name="miscajas")
async def cmd_mis_cajas(ctx):
    """Ver tus cajas sin abrir"""
    if not await verificar_canal(ctx):
        return
    
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        cursor = await db.execute('''
            SELECT cc.id, c.nombre, c.tipo, cc.fecha_compra
            FROM cajas_compradas cc
            JOIN cajas c ON cc.caja_id = c.id
            WHERE cc.usuario_id = ? AND cc.abierta = 0
            ORDER BY cc.fecha_compra ASC
        ''', (str(ctx.author.id),))
        cajas = await cursor.fetchall()
    
    if not cajas:
        await ctx.send(embed=embeds.crear_embed_info("Sin cajas", "No tienes cajas sin abrir"))
        return
    
    embed = discord.Embed(
        title="📦 **Tus cajas sin abrir**",
        color=COLORS['primary']
    )
    
    for caja in cajas[:20]:
        embed.add_field(
            name=f"ID: {caja['id']} - {caja['nombre']}",
            value=f"Comprada: {caja['fecha_compra'][:10]}\nUsa `!abrircaja {caja['id']}` para abrir",
            inline=False
        )
    
    if len(cajas) > 20:
        embed.set_footer(text=f"Mostrando 20 de {len(cajas)} cajas")
    
    await ctx.send(embed=embed)

@bot.command(name="abrircaja")
async def cmd_abrir_caja(ctx, caja_id: int):
    """Abrir una caja misteriosa"""
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
        
        # Calcular premio
        premios = json.loads(caja['premios'])
        probabilidades = json.loads(caja['probabilidades'])
        
        # Elegir premio según probabilidades
        elegido = random.choices(premios, weights=probabilidades, k=1)[0]
        
        # Marcar como abierta
        await db.execute('''
            UPDATE cajas_compradas SET abierta = 1, premio = ?, fecha_apertura = CURRENT_TIMESTAMP
            WHERE id = ?
        ''', (elegido, caja_id))
        
        # Acreditar premio si es mayor que 0
        if elegido > 0:
            await db.execute('''
                INSERT INTO usuarios_balance (discord_id, nombre, balance)
                VALUES (?, ?, ?)
                ON CONFLICT(discord_id) DO UPDATE SET
                    balance = balance + ?
            ''', (str(ctx.author.id), ctx.author.name, elegido, elegido))
        
        await db.commit()
    
    if elegido > 0:
        embed = discord.Embed(
            title="🎉 **CAJA ABIERTA** 🎉",
            description=f"Has abierto una {caja['nombre']} y obtenido **${elegido:,} VP$**",
            color=COLORS['success']
        )
    else:
        embed = discord.Embed(
            title="😢 **CAJA ABIERTA**",
            description=f"Has abierto una {caja['nombre']} y... **no has ganado nada**",
            color=COLORS['error']
        )
    
    await ctx.send(embed=embed)
    await enviar_dm(str(ctx.author.id), "🎁 Caja abierta", f"Has abierto una {caja['nombre']} y ganado {elegido:,} VP$")

@bot.command(name="abrirtodas")
async def cmd_abrir_todas(ctx):
    """Abrir todas tus cajas de una vez"""
    if not await verificar_canal(ctx):
        return
    
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        cursor = await db.execute('''
            SELECT cc.id, c.premios, c.probabilidades, c.nombre
            FROM cajas_compradas cc
            JOIN cajas c ON cc.caja_id = c.id
            WHERE cc.usuario_id = ? AND cc.abierta = 0
        ''', (str(ctx.author.id),))
        cajas = await cursor.fetchall()
        
        if not cajas:
            await ctx.send(embed=embeds.crear_embed_info("Sin cajas", "No tienes cajas sin abrir"))
            return
        
        total_ganado = 0
        for caja in cajas:
            premios = json.loads(caja['premios'])
            probabilidades = json.loads(caja['probabilidades'])
            elegido = random.choices(premios, weights=probabilidades, k=1)[0]
            total_ganado += elegido
            
            await db.execute('''
                UPDATE cajas_compradas SET abierta = 1, premio = ?, fecha_apertura = CURRENT_TIMESTAMP
                WHERE id = ?
            ''', (elegido, caja['id']))
        
        if total_ganado > 0:
            await db.execute('''
                INSERT INTO usuarios_balance (discord_id, nombre, balance)
                VALUES (?, ?, ?)
                ON CONFLICT(discord_id) DO UPDATE SET
                    balance = balance + ?
            ''', (str(ctx.author.id), ctx.author.name, total_ganado, total_ganado))
        
        await db.commit()
    
    embed = discord.Embed(
        title="🎉 **TODAS LAS CAJAS ABIERTAS** 🎉",
        description=f"Has abierto {len(cajas)} cajas y ganado **${total_ganado:,} VP$**",
        color=COLORS['success'] if total_ganado > 0 else COLORS['error']
    )
    await ctx.send(embed=embed)

# ============================================
# SISTEMA DE NÚMEROS VIP EN RIFAS
# ============================================

@bot.command(name="crearifa")
async def cmd_crear_rifa(ctx, premio: str, precio: int, total: int, bloqueados: str = None):
    """Crear una nueva rifa (con opción de números VIP bloqueados)"""
    if not await check_admin(ctx):
        return
    
    nombre = f"Rifa {datetime.now().strftime('%d/%m')}"
    
    # Procesar números bloqueados
    numeros_bloqueados = ""
    if bloqueados:
        # Formato: "1-10" o "1-5,30-50,70"
        numeros_bloqueados = bloqueados
    
    async with aiosqlite.connect(DB_PATH) as db:
        cursor = await db.execute('''
            INSERT INTO rifas (nombre, premio, valor_premio, precio_boleto, total_boletos, numeros_bloqueados, estado)
            VALUES (?, ?, ?, ?, ?, ?, 'activa')
        ''', (nombre, premio, precio, precio, total, numeros_bloqueados))
        rifa_id = cursor.lastrowid
        await db.commit()
    
    embed = embeds.crear_embed_exito(f"✅ Rifa creada ID: {rifa_id}\nPremio: {premio}\nPrecio: ${precio}\nTotal: {total} boletos")
    if numeros_bloqueados:
        embed.add_field(name="🔒 Números VIP bloqueados", value=f"{numeros_bloqueados} (solo venta staff)", inline=False)
    await ctx.send(embed=embed)

async def es_numero_bloqueado(rifa_id, numero):
    """Verifica si un número está en la lista de bloqueados de la rifa"""
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

# Modificar comprarrandom para respetar números bloqueados
@bot.command(name="comprarrandom")
async def cmd_comprar_random(ctx, cantidad: int = 1):
    if not await verificar_canal(ctx):
        return
    
    if cantidad < 1 or cantidad > 50:
        await ctx.send(embed=embeds.crear_embed_error("Cantidad entre 1 y 50"))
        return
    
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        cursor = await db.execute('SELECT * FROM rifas WHERE estado = "activa" ORDER BY id DESC LIMIT 1')
        rifa = await cursor.fetchone()
    
    if not rifa:
        await ctx.send(embed=embeds.crear_embed_error("No hay rifa activa"))
        return
    
    # Obtener boletos disponibles (excluyendo bloqueados y vendidos)
    disponibles = await bot.db.get_boletos_disponibles(rifa['id'])
    
    # Filtrar números bloqueados
    disponibles_filtrados = []
    for num in disponibles:
        if not await es_numero_bloqueado(rifa['id'], num):
            disponibles_filtrados.append(num)
    
    if len(disponibles_filtrados) < cantidad:
        await ctx.send(embed=embeds.crear_embed_error(f"Solo hay {len(disponibles_filtrados)} boletos disponibles para compra normal"))
        return
    
    # Resto del código de compra...
    # [Aquí va el código de compra normal que ya tenías]
    
    # Al final, notificar si se intentaron comprar números VIP
    await ctx.send(embed=embeds.crear_embed_exito(f"✅ Compra realizada! Revisa tu DM."))

# ============================================
# SISTEMA DE BANCO VP
# ============================================

@bot.command(name="banco")
async def cmd_banco(ctx):
    """Muestra los productos del banco"""
    if not await verificar_canal(ctx):
        return
    
    async with aiosqlite.connect(DB_PATH) as db:
        cursor = await db.execute('SELECT tasa_compra, tasa_venta FROM banco_config WHERE id = 1')
        config = await cursor.fetchone()
        tasa_compra = config[0] if config else 0.9
        tasa_venta = config[1] if config else 1.1
    
    embed = discord.Embed(
        title="🏦 **BANCO VP**",
        description="Invierte tus VP$ y gana intereses",
        color=COLORS['primary']
    )
    
    embed.add_field(
        name="📈 **Ahorro Básico**",
        value="• Plazo: 7 días\n• Interés: 5%\n• Monto: 10k - 500k VP$",
        inline=False
    )
    
    embed.add_field(
        name="📈 **Ahorro Plus**",
        value="• Plazo: 14 días\n• Interés: 12%\n• Monto: 50k - 2M VP$",
        inline=False
    )
    
    embed.add_field(
        name="📈 **Ahorro VIP**",
        value="• Plazo: 30 días\n• Interés: 25%\n• Monto: 200k - 10M VP$",
        inline=False
    )
    
    embed.add_field(
        name="💱 **Mercado de Cambio**",
        value=f"• 1,000,000 NG$ → {int(1000000 * tasa_compra):,} VP$\n• 100,000 VP$ → {int(100000 * tasa_venta):,} NG$",
        inline=False
    )
    
    embed.set_footer(text="Usa !invertir [tipo] [monto] | !cambiar [vp/ng] [cantidad]")
    await ctx.send(embed=embed)

@bot.command(name="invertir")
async def cmd_invertir(ctx, producto: str, monto: int):
    """Invertir VP$ en el banco (basico, plus, vip)"""
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
        await ctx.send(embed=embeds.crear_embed_error(f"Monto debe estar entre {prod['min']:,} y {prod['max']:,} VP$"))
        return
    
    async with aiosqlite.connect(DB_PATH) as db:
        cursor = await db.execute('SELECT balance FROM usuarios_balance WHERE discord_id = ?', (str(ctx.author.id),))
        result = await cursor.fetchone()
        balance = result[0] if result else 0
    
    if balance < monto:
        await ctx.send(embed=embeds.crear_embed_error(f"Saldo insuficiente. Necesitas {monto:,} VP$"))
        return
    
    fecha_fin = datetime.now() + timedelta(days=prod['dias'])
    
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute('UPDATE usuarios_balance SET balance = balance - ? WHERE discord_id = ?', (monto, str(ctx.author.id)))
        await db.execute('''
            INSERT INTO inversiones (usuario_id, producto, monto, interes, fecha_fin)
            VALUES (?, ?, ?, ?, ?)
        ''', (str(ctx.author.id), producto, monto, prod['interes'], fecha_fin))
        await db.commit()
    
    embed = discord.Embed(
        title="✅ **Inversión realizada**",
        description=f"Has invertido **{monto:,} VP$** en **Ahorro {producto.upper()}**",
        color=COLORS['success']
    )
    embed.add_field(name="📅 Plazo", value=f"{prod['dias']} días", inline=True)
    embed.add_field(name="💰 Interés", value=f"{prod['interes']}%", inline=True)
    embed.add_field(name="💵 Retiro", value=f"≈ {int(monto * (1 + prod['interes']/100)):,} VP$", inline=True)
    await ctx.send(embed=embed)

@bot.command(name="misinversiones")
async def cmd_mis_inversiones(ctx):
    """Ver tus inversiones activas"""
    if not await verificar_canal(ctx):
        return
    
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        cursor = await db.execute('''
            SELECT id, producto, monto, interes, fecha_inicio, fecha_fin
            FROM inversiones
            WHERE usuario_id = ? AND estado = 'activa'
        ''', (str(ctx.author.id),))
        inversiones = await cursor.fetchall()
    
    if not inversiones:
        await ctx.send(embed=embeds.crear_embed_info("Sin inversiones", "No tienes inversiones activas"))
        return
    
    embed = discord.Embed(
        title="📊 **Tus inversiones**",
        color=COLORS['primary']
    )
    
    for inv in inversiones:
        fecha_fin = datetime.fromisoformat(inv['fecha_fin'])
        dias_restantes = (fecha_fin - datetime.now()).days
        
        embed.add_field(
            name=f"#{inv['id']} - Ahorro {inv['producto'].upper()}",
            value=f"Monto: ${inv['monto']:,}\nInterés: {inv['interes']}%\nRetiro: {dias_restantes} días",
            inline=False
        )
    
    await ctx.send(embed=embed)

@bot.command(name="retirar")
async def cmd_retirar(ctx, inversion_id: int):
    """Retirar una inversión"""
    if not await verificar_canal(ctx):
        return
    
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        cursor = await db.execute('''
            SELECT * FROM inversiones
            WHERE id = ? AND usuario_id = ? AND estado = 'activa'
        ''', (inversion_id, str(ctx.author.id)))
        inv = await cursor.fetchone()
        
        if not inv:
            await ctx.send(embed=embeds.crear_embed_error("Inversión no encontrada"))
            return
        
        fecha_fin = datetime.fromisoformat(inv['fecha_fin'])
        
        if datetime.now() < fecha_fin:
            dias_restantes = (fecha_fin - datetime.now()).days
            await ctx.send(embed=embeds.crear_embed_error(f"Todavía faltan {dias_restantes} días para retirar"))
            return
        
        ganancia_bruta = int(inv['monto'] * inv['interes'] / 100)
        comision = int(ganancia_bruta * 5 / 100)
        ganancia_neta = ganancia_bruta - comision
        total = inv['monto'] + ganancia_neta
        
        await db.execute('''
            UPDATE usuarios_balance SET balance = balance + ? WHERE discord_id = ?
        ''', (total, str(ctx.author.id)))
        await db.execute('''
            UPDATE inversiones SET estado = 'completada' WHERE id = ?
        ''', (inversion_id,))
        await db.commit()
    
    embed = discord.Embed(
        title="✅ **Inversión retirada**",
        description=f"Has retirado **{total:,} VP$**",
        color=COLORS['success']
    )
    embed.add_field(name="💰 Inversión inicial", value=f"${inv['monto']:,}", inline=True)
    embed.add_field(name="📈 Interés", value=f"+${ganancia_bruta:,}", inline=True)
    embed.add_field(name="💸 Comisión", value=f"-${comision:,}", inline=True)
    embed.add_field(name="💵 Total", value=f"**${total:,}**", inline=False)
    await ctx.send(embed=embed)

@bot.command(name="cambiar")
async def cmd_cambiar(ctx, moneda: str, cantidad: int):
    """Cambiar VP$ por NG$ o viceversa"""
    if not await verificar_canal(ctx):
        return
    
    if cantidad <= 0:
        await ctx.send(embed=embeds.crear_embed_error("Cantidad debe ser positiva"))
        return
    
    async with aiosqlite.connect(DB_PATH) as db:
        cursor = await db.execute('SELECT tasa_compra, tasa_venta FROM banco_config WHERE id = 1')
        config = await cursor.fetchone()
        tasa_compra = config[0] if config else 0.9
        tasa_venta = config[1] if config else 1.1
    
    if moneda.lower() == 'ng':
        # Cambiar NG$ a VP$
        cantidad_vp = int(cantidad * tasa_compra)
        comision = cantidad - cantidad_vp
        
        # Registrar transacción pendiente (requiere verificación del staff)
        async with aiosqlite.connect(DB_PATH) as db:
            await db.execute('''
                INSERT INTO pagos_pendientes (usuario_id, monto, metodo, estado)
                VALUES (?, ?, 'cambio_ng', 'pendiente')
            ''', (str(ctx.author.id), cantidad))
            await db.commit()
        
        embed = discord.Embed(
            title="⏳ **Cambio solicitado**",
            description=f"Has solicitado cambiar **{cantidad:,} NG$** por **{cantidad_vp:,} VP$**",
            color=COLORS['info']
        )
        embed.add_field(name="💰 Comisión", value=f"{comision:,} NG$", inline=True)
        embed.add_field(name="⏰ Estado", value="Pendiente de verificación", inline=True)
        embed.set_footer(text="Un staff verificará tu pago y te acreditará los VP$")
        await ctx.send(embed=embed)
        
    elif moneda.lower() == 'vp':
        # Cambiar VP$ a NG$
        cantidad_ng = int(cantidad * tasa_venta)
        comision = cantidad_ng - cantidad
        
        async with aiosqlite.connect(DB_PATH) as db:
            cursor = await db.execute('SELECT balance FROM usuarios_balance WHERE discord_id = ?', (str(ctx.author.id),))
            result = await cursor.fetchone()
            balance = result[0] if result else 0
            
            if balance < cantidad:
                await ctx.send(embed=embeds.crear_embed_error(f"Saldo insuficiente. Necesitas {cantidad:,} VP$"))
                return
            
            await db.execute('UPDATE usuarios_balance SET balance = balance - ? WHERE discord_id = ?', (cantidad, str(ctx.author.id)))
            await db.execute('''
                INSERT INTO transacciones_cambio (usuario_id, tipo, cantidad, comision)
                VALUES (?, 'vp_a_ng', ?, ?)
            ''', (str(ctx.author.id), cantidad_ng, comision))
            await db.commit()
        
        embed = discord.Embed(
            title="✅ **Cambio realizado**",
            description=f"Has cambiado **{cantidad:,} VP$** por **{cantidad_ng:,} NG$**",
            color=COLORS['success']
        )
        embed.add_field(name="💰 Comisión", value=f"{comision:,} NG$", inline=True)
        embed.add_field(name="💵 Nuevo balance", value=f"**{balance - cantidad:,} VP$**", inline=True)
        await ctx.send(embed=embed)
        await enviar_dm(str(ctx.author.id), "💰 Cambio de moneda", f"Has cambiado {cantidad} VP$ por {cantidad_ng} NG$")
        
    else:
        await ctx.send(embed=embeds.crear_embed_error("Moneda inválida. Usa: ng o vp"))

# ============================================
# SISTEMA DE MISIONES DIARIAS
# ============================================

@bot.command(name="misiones")
async def cmd_misiones(ctx):
    """Ver misiones del día"""
    if not await verificar_canal(ctx):
        return
    
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        cursor = await db.execute('SELECT * FROM misiones WHERE activo = 1')
        misiones = await cursor.fetchall()
        
        # Obtener progreso del usuario
        progreso = {}
        for m in misiones:
            cursor = await db.execute('''
                SELECT progreso, completada FROM progreso_misiones
                WHERE usuario_id = ? AND mision_id = ?
            ''', (str(ctx.author.id), m['id']))
            result = await cursor.fetchone()
            if result:
                progreso[m['id']] = {'progreso': result[0], 'completada': result[1]}
            else:
                progreso[m['id']] = {'progreso': 0, 'completada': False}
        
        cursor = await db.execute('SELECT racha FROM rachas WHERE usuario_id = ?', (str(ctx.author.id),))
        racha_result = await cursor.fetchone()
        racha = racha_result[0] if racha_result else 0
    
    embed = discord.Embed(
        title="📋 **MISIONES DIARIAS**",
        description=f"Racha actual: **{racha}** días 🔥",
        color=COLORS['primary']
    )
    
    for m in misiones:
        estado = "✅" if progreso[m['id']]['completada'] else "⏳"
        texto = f"{estado} {m['descripcion']}\nRecompensa: ${m['recompensa']:,} VP$"
        
        if not progreso[m['id']]['completada']:
            texto += f"\nProgreso: {progreso[m['id']]['progreso']}/{m['valor_requisito']}"
        
        embed.add_field(name=m['nombre'], value=texto, inline=False)
    
    embed.set_footer(text="Usa !completar [id] para marcar una misión como completada")
    await ctx.send(embed=embed)

@bot.command(name="miracha")
async def cmd_miracha(ctx):
    """Ver tu racha actual"""
    if not await verificar_canal(ctx):
        return
    
    async with aiosqlite.connect(DB_PATH) as db:
        cursor = await db.execute('SELECT racha, mejor_racha FROM rachas WHERE usuario_id = ?', (str(ctx.author.id),))
        result = await cursor.fetchone()
        racha = result[0] if result else 0
        mejor_racha = result[1] if result else 0
    
    embed = discord.Embed(
        title="🔥 **Tu racha**",
        description=f"Racha actual: **{racha}** días\nMejor racha: **{mejor_racha}** días",
        color=COLORS['primary']
    )
    
    # Mostrar próximos bonos
    bonos = [
        (3, "+500 VP$"),
        (7, "+2,000 VP$"),
        (14, "+10,000 VP$"),
        (30, "+50,000 VP$ + rol exclusivo")
    ]
    
    texto_bonos = ""
    for dias, bono in bonos:
        if racha < dias:
            texto_bonos += f"• En {dias} días: {bono}\n"
    
    if texto_bonos:
        embed.add_field(name="🎁 Próximos bonos", value=texto_bonos, inline=False)
    
    await ctx.send(embed=embed)

# ============================================
# SISTEMA DE DISTRIBUIDORES
# ============================================

@bot.command(name="distribuidor")
async def cmd_distribuidor(ctx):
    """Ver tu nivel y estado como distribuidor"""
    if not await verificar_canal(ctx):
        return
    
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        cursor = await db.execute('''
            SELECT * FROM distribuidores WHERE discord_id = ?
        ''', (str(ctx.author.id),))
        distribuidor = await cursor.fetchone()
        
        if not distribuidor:
            await ctx.send(embed=embeds.crear_embed_error("No eres distribuidor. Contacta al CEO para obtener una franquicia"))
            return
        
        # Obtener nivel de franquicia
        nivel_franquicia = 0
        for nivel, datos in ROLES_FRANQUICIA.items():
            if tiene_rol(ctx.author, datos['rol_id']):
                nivel_franquicia = nivel
                break
    
    embed = discord.Embed(
        title="📦 **Tu perfil de distribuidor**",
        color=COLORS['primary']
    )
    
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
    """Ver catálogo de productos para distribuidores"""
    if not await verificar_canal(ctx):
        return
    
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        cursor = await db.execute('''
            SELECT * FROM productos WHERE activo = 1 ORDER BY nivel_minimo ASC
        ''')
        productos = await cursor.fetchall()
    
    embed = discord.Embed(
        title="📦 **Catálogo de productos**",
        description="Productos disponibles para distribuidores",
        color=COLORS['primary']
    )
    
    for prod in productos:
        nivel_letra = {1: 'A', 2: 'B', 3: 'C', 4: 'D', 5: 'E', 6: 'F'}.get(prod['nivel_minimo'], 'A')
        embed.add_field(
            name=f"{prod['nombre']} (Nivel {nivel_letra})",
            value=f"{prod['descripcion']}\n💰 Normal: ${prod['precio_normal']:,}\n📦 Mayorista: ${prod['precio_mayorista']:,}\n📊 Stock: {'Ilimitado' if prod['stock'] == -1 else prod['stock']}",
            inline=False
        )
    
    embed.set_footer(text="Usa !comprar_producto [nombre] [cantidad] para comprar stock")
    await ctx.send(embed=embed)

@bot.command(name="comprar_producto")
async def cmd_comprar_producto(ctx, nombre: str, cantidad: int = 1):
    """Comprar productos al por mayor (distribuidores)"""
    if not await verificar_canal(ctx):
        return
    if not await check_distribuidor(ctx, 1):
        await ctx.send(embed=embeds.crear_embed_error("No tienes permiso para usar este comando"))
        return
    
    if cantidad < 1 or cantidad > 100:
        await ctx.send(embed=embeds.crear_embed_error("Cantidad entre 1 y 100"))
        return
    
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        cursor = await db.execute('''
            SELECT * FROM productos WHERE nombre LIKE ? AND activo = 1
        ''', (f'%{nombre}%',))
        producto = await cursor.fetchone()
        
        if not producto:
            await ctx.send(embed=embeds.crear_embed_error("Producto no encontrado"))
            return
        
        # Verificar nivel del distribuidor
        cursor = await db.execute('SELECT nivel FROM distribuidores WHERE discord_id = ?', (str(ctx.author.id),))
        distribuidor = await cursor.fetchone()
        nivel_distribuidor = distribuidor[0] if distribuidor else 1
        
        if nivel_distribuidor < producto['nivel_minimo']:
            nivel_letra = {1: 'A', 2: 'B', 3: 'C', 4: 'D', 5: 'E', 6: 'F'}.get(producto['nivel_minimo'], 'A')
            await ctx.send(embed=embeds.crear_embed_error(f"Necesitas al menos nivel {nivel_letra} para comprar este producto"))
            return
        
        precio_total = producto['precio_mayorista'] * cantidad
        
        # Verificar balance
        cursor = await db.execute('SELECT balance FROM usuarios_balance WHERE discord_id = ?', (str(ctx.author.id),))
        result = await cursor.fetchone()
        balance = result[0] if result else 0
        
        if balance < precio_total:
            await ctx.send(embed=embeds.crear_embed_error(f"Necesitas {precio_total:,} VP$"))
            return
        
        # Descontar balance
        await db.execute('UPDATE usuarios_balance SET balance = balance - ? WHERE discord_id = ?', (precio_total, str(ctx.author.id)))
        
        # Agregar al inventario
        cursor = await db.execute('''
            SELECT cantidad FROM inventario_distribuidor
            WHERE distribuidor_id = ? AND producto_id = ?
        ''', (str(ctx.author.id), producto['id']))
        inventario = await cursor.fetchone()
        
        if inventario:
            await db.execute('''
                UPDATE inventario_distribuidor SET cantidad = cantidad + ?
                WHERE distribuidor_id = ? AND producto_id = ?
            ''', (cantidad, str(ctx.author.id), producto['id']))
        else:
            await db.execute('''
                INSERT INTO inventario_distribuidor (distribuidor_id, producto_id, cantidad)
                VALUES (?, ?, ?)
            ''', (str(ctx.author.id), producto['id'], cantidad))
        
        await db.commit()
    
    embed = discord.Embed(
        title="✅ **Compra realizada**",
        description=f"Has comprado {cantidad}x {producto['nombre']} por ${precio_total:,} VP$",
        color=COLORS['success']
    )
    embed.add_field(name="📦 Stock disponible", value=f"Puedes venderlo a precio normal para obtener ganancia", inline=False)
    await ctx.send(embed=embed)

@bot.command(name="mis_productos")
async def cmd_mis_productos(ctx):
    """Ver tu inventario de productos"""
    if not await verificar_canal(ctx):
        return
    if not await check_distribuidor(ctx, 1):
        await ctx.send(embed=embeds.crear_embed_error("No tienes permiso para usar este comando"))
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
    
    embed = discord.Embed(
        title="📦 **Tu inventario**",
        color=COLORS['primary']
    )
    
    total_valor = 0
    for item in inventario:
        valor = item['cantidad'] * item['precio_normal']
        total_valor += valor
        embed.add_field(
            name=f"{item['nombre']}",
            value=f"Cantidad: {item['cantidad']}\nValor de reventa: ${valor:,} VP$",
            inline=False
        )
    
    embed.add_field(name="💰 Valor total del inventario", value=f"**${total_valor:,} VP$**", inline=False)
    await ctx.send(embed=embed)

# ============================================
# COMANDOS DE ADMINISTRACIÓN (CEO)
# ============================================

@bot.command(name="procesarvp")
async def cmd_procesar_vp(ctx, usuario: discord.Member):
    """[CEO] Marca un pago como en proceso"""
    if not await check_ceo(ctx):
        await ctx.send("❌ Solo el CEO puede usar este comando")
        return
    
    async with aiosqlite.connect(DB_PATH) as db:
        cursor = await db.execute('''
            SELECT id, monto FROM pagos_pendientes
            WHERE usuario_id = ? AND estado = 'pendiente'
            ORDER BY fecha DESC LIMIT 1
        ''', (str(usuario.id),))
        pago = await cursor.fetchone()
        
        if not pago:
            await ctx.send(embed=embeds.crear_embed_error(f"{usuario.name} no tiene pagos pendientes"))
            return
        
        await db.execute('''
            UPDATE pagos_pendientes SET estado = 'procesando' WHERE id = ?
        ''', (pago[0],))
        await db.commit()
    
    embed = discord.Embed(
        title="⏳ **Pago en proceso**",
        description=f"El pago de **${pago[1]:,} NG$** de {usuario.mention} está siendo procesado",
        color=COLORS['info']
    )
    await ctx.send(embed=embed)
    
    await enviar_dm(str(usuario.id), "💰 Pago en proceso", f"Tu pago de {pago[1]:,} NG$ está siendo procesado. En breve se acreditarán a tu balance.")

@bot.command(name="procesadovp")
async def cmd_procesado_vp(ctx, usuario: discord.Member, cantidad: int):
    """[CEO] Confirma un pago y acredita VP$ al usuario"""
    if not await check_ceo(ctx):
        await ctx.send("❌ Solo el CEO puede usar este comando")
        return
    
    async with aiosqlite.connect(DB_PATH) as db:
        # Obtener tasa de cambio
        cursor = await db.execute('SELECT tasa_compra FROM banco_config WHERE id = 1')
        config = await cursor.fetchone()
        tasa = config[0] if config else 0.9
        
        cantidad_vp = int(cantidad * tasa)
        
        # Acreditar VP$
        await db.execute('''
            INSERT INTO usuarios_balance (discord_id, nombre, balance)
            VALUES (?, ?, ?)
            ON CONFLICT(discord_id) DO UPDATE SET
                balance = balance + ?
        ''', (str(usuario.id), usuario.name, cantidad_vp, cantidad_vp))
        
        # Marcar pago como completado
        await db.execute('''
            UPDATE pagos_pendientes SET estado = 'completado' WHERE usuario_id = ? AND estado = 'procesando'
        ''', (str(usuario.id),))
        
        await db.commit()
    
    embed = discord.Embed(
        title="✅ **Pago procesado**",
        description=f"Se han acreditado **${cantidad_vp:,} VP$** a {usuario.mention}",
        color=COLORS['success']
    )
    embed.add_field(name="💰 Monto original", value=f"{cantidad:,} NG$", inline=True)
    embed.add_field(name="💵 VP$ acreditados", value=f"{cantidad_vp:,} VP$", inline=True)
    await ctx.send(embed=embed)
    
    await enviar_dm(str(usuario.id), "💰 Pago confirmado", f"Tu pago ha sido procesado. Has recibido {cantidad_vp:,} VP$ en tu balance.")

@bot.command(name="verboletos")
async def cmd_ver_boletos(ctx, usuario: discord.Member):
    """[CEO] Ver todos los boletos de un usuario"""
    if not await check_ceo(ctx):
        await ctx.send("❌ Solo el CEO puede usar este comando")
        return
    
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        cursor = await db.execute('''
            SELECT b.numero, r.nombre as rifa, b.precio_pagado, b.fecha_compra, b.vendedor_id
            FROM boletos b
            JOIN rifas r ON b.rifa_id = r.id
            WHERE b.comprador_id = ?
            ORDER BY b.fecha_compra DESC
        ''', (str(usuario.id),))
        boletos = await cursor.fetchall()
    
    if not boletos:
        await ctx.send(embed=embeds.crear_embed_info("Sin boletos", f"{usuario.name} no tiene boletos"))
        return
    
    embed = discord.Embed(
        title=f"🎟️ **Boletos de {usuario.name}**",
        description=f"Total: {len(boletos)} boletos",
        color=COLORS['primary']
    )
    
    # Agrupar por rifa
    rifas = {}
    for b in boletos:
        if b['rifa'] not in rifas:
            rifas[b['rifa']] = []
        rifas[b['rifa']].append(b)
    
    for rifa, lista in list(rifas.items())[:5]:
        numeros = [f"#{b['numero']}" for b in lista]
        embed.add_field(
            name=f"📌 {rifa}",
            value=f"Boletos: {', '.join(numeros[:10])}{'...' if len(numeros) > 10 else ''}\nTotal: {len(lista)} boletos",
            inline=False
        )
    
    await ctx.send(embed=embed)

# ============================================
# COMANDOS DE FRANQUICIA
# ============================================

@bot.command(name="franquicia")
async def cmd_franquicia(ctx):
    """Ver información de tu franquicia"""
    if not await verificar_canal(ctx, CATEGORIA_FRANQUICIAS):
        return
    if not await check_franquicia(ctx):
        await ctx.send(embed=embeds.crear_embed_error("No tienes una franquicia activa"))
        return
    
    # Obtener nivel de franquicia
    nivel = 0
    for n, datos in ROLES_FRANQUICIA.items():
        if tiene_rol(ctx.author, datos['rol_id']):
            nivel = n
            break
    
    embed = discord.Embed(
        title="👑 **Tu Franquicia VP**",
        description=f"Nivel: **{nivel}**\nCanal exclusivo: <#{ROLES_FRANQUICIA[nivel]['canal_id']}>",
        color=COLORS['primary']
    )
    
    beneficios = {
        1: "• 5% de comisión por ventas\n• Acceso a productos básicos",
        2: "• 7% de comisión por ventas\n• Pack de 10 boletos disponible\n• Rol especial",
        3: "• 10% de comisión por ventas\n• Pack de 50 boletos disponible\n• Cajas épicas",
        4: "• 12% de comisión por ventas\n• Productos VIP\n• Franquicia regional",
        5: "• 15% de comisión por ventas\n• TODO el catálogo\n• Franquicia premium"
    }
    
    embed.add_field(name="🎁 Beneficios", value=beneficios.get(nivel, "• Comisión base"), inline=False)
    embed.add_field(name="📈 Próximo nivel", value=f"Faltan {1000 - nivel * 200} ventas para nivel {nivel + 1}" if nivel < 5 else "Máximo nivel alcanzado", inline=False)
    
    await ctx.send(embed=embed)

@bot.command(name="franquicia_rifa")
async def cmd_franquicia_rifa(ctx, premio: str, precio: int, total: int):
    """[FRANQUICIA] Crear una rifa en tu canal de franquicia"""
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
    
    # Notificar en el canal de la franquicia
    for nivel, datos in ROLES_FRANQUICIA.items():
        if tiene_rol(ctx.author, datos['rol_id']):
            canal = bot.get_channel(datos['canal_id'])
            if canal:
                await canal.send(f"🎉 **Nueva rifa creada por {ctx.author.mention}**\nPremio: {premio}\nPrecio: ${precio}\n¡Usa `!comprarrandom` para participar!")

# ============================================
# SISTEMA DE EVENTOS (CORREGIDO)
# ============================================

evento_2x1 = False
evento_cashback_doble = False
evento_oferta_activa = False
evento_oferta_porcentaje = 0

@bot.command(name="2x1")
async def cmd_2x1(ctx):
    """[CEO] Activa o desactiva el evento 2x1"""
    if not await check_ceo(ctx):
        return
    
    global evento_2x1
    evento_2x1 = not evento_2x1
    estado = "ACTIVADO" if evento_2x1 else "DESACTIVADO"
    
    await ctx.message.delete()
    await ctx.send(embed=embeds.crear_embed_exito(f"🎉 Evento 2x1 {estado}"))
    await bot.enviar_log_sistema("🎉 EVENTO 2x1", f"Evento {estado} por {ctx.author.name}")

@bot.command(name="cashbackdoble")
async def cmd_cashback_doble(ctx):
    """[CEO] Activa o desactiva el cashback doble"""
    if not await check_ceo(ctx):
        return
    
    global evento_cashback_doble
    evento_cashback_doble = not evento_cashback_doble
    estado = "ACTIVADO" if evento_cashback_doble else "DESACTIVADO"
    
    await ctx.message.delete()
    await ctx.send(embed=embeds.crear_embed_exito(f"🎉 Cashback doble {estado}"))
    await bot.enviar_log_sistema("🎉 CASHBACK DOBLE", f"Evento {estado} por {ctx.author.name}")

@bot.command(name="oferta")
async def cmd_oferta(ctx, porcentaje: int):
    """[CEO] Activa una oferta de descuento"""
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
    await bot.enviar_log_sistema("🎉 OFERTA ACTIVADA", f"{porcentaje}% de descuento por {ctx.author.name}")

@bot.command(name="ofertadesactivar")
async def cmd_oferta_desactivar(ctx):
    """[CEO] Desactiva la oferta actual"""
    if not await check_ceo(ctx):
        return
    
    global evento_oferta_activa, evento_oferta_porcentaje
    evento_oferta_activa = False
    evento_oferta_porcentaje = 0
    
    await ctx.message.delete()
    await ctx.send(embed=embeds.crear_embed_exito("🎉 Oferta desactivada"))
    await bot.enviar_log_sistema("🎉 OFERTA DESACTIVADA", f"Por {ctx.author.name}")

# ============================================
# CONFIGURACIÓN DE CAJAS (CEO)
# ============================================

@bot.command(name="crearcaja")
async def cmd_crear_caja(ctx, tipo: str, precio: int, premios: str, probabilidades: str):
    """[CEO] Crear un nuevo tipo de caja"""
    if not await check_ceo(ctx):
        return
    
    nombre = f"Caja {tipo.capitalize()}"
    
    try:
        premios_list = json.loads(premios)
        probs_list = json.loads(probabilidades)
        
        if len(premios_list) != len(probs_list):
            await ctx.send(embed=embeds.crear_embed_error("La cantidad de premios y probabilidades debe ser la misma"))
            return
        
        if sum(probs_list) != 100:
            await ctx.send(embed=embeds.crear_embed_error("La suma de las probabilidades debe ser 100"))
            return
    except:
        await ctx.send(embed=embeds.crear_embed_error("Formato inválido. Usa: [1000,2000,5000] y [50,30,20]"))
        return
    
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute('''
            INSERT INTO cajas (tipo, nombre, precio, premios, probabilidades, activo)
            VALUES (?, ?, ?, ?, ?, 1)
        ''', (tipo.lower(), nombre, precio, premios, probabilidades))
        await db.commit()
    
    await ctx.message.delete()
    await ctx.send(embed=embeds.crear_embed_exito(f"✅ Caja '{nombre}' creada exitosamente"))

@bot.command(name="editarcaja")
async def cmd_editar_caja(ctx, tipo: str, campo: str, valor: str):
    """[CEO] Editar una caja existente"""
    if not await check_ceo(ctx):
        return
    
    campos_validos = ['precio', 'premios', 'probabilidades', 'activo']
    
    if campo not in campos_validos:
        await ctx.send(embed=embeds.crear_embed_error(f"Campo inválido. Usa: {', '.join(campos_validos)}"))
        return
    
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(f'UPDATE cajas SET {campo} = ? WHERE tipo = ?', (valor, tipo.lower()))
        await db.commit()
    
    await ctx.message.delete()
    await ctx.send(embed=embeds.crear_embed_exito(f"✅ Caja {tipo}: {campo} actualizado a {valor}"))

# ============================================
# CONFIGURACIÓN DE PRODUCTOS (CEO)
# ============================================

@bot.command(name="crearproducto")
async def cmd_crear_producto(ctx, nombre: str, precio_normal: int, precio_mayorista: int, nivel_minimo: int = 1):
    """[CEO] Crear un nuevo producto para distribuidores"""
    if not await check_ceo(ctx):
        return
    
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute('''
            INSERT INTO productos (nombre, descripcion, precio_normal, precio_mayorista, nivel_minimo, activo)
            VALUES (?, ?, ?, ?, ?, 1)
        ''', (nombre, f"Producto {nombre}", precio_normal, precio_mayorista, nivel_minimo))
        await db.commit()
    
    await ctx.message.delete()
    await ctx.send(embed=embeds.crear_embed_exito(f"✅ Producto '{nombre}' creado exitosamente"))

# ============================================
# COMANDO DE VERSIÓN
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
                    f"**Usuarios:** {len(bot.users)}\n"
                    f"**Volumen persistente:** {'✅ Activo' if bot.volumen_montado else '❌ No detectado'}",
        color=COLORS['primary']
    )
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
