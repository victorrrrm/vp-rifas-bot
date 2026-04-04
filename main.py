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
        
        if self.volumen_montado:
            logger.info("✅ Volumen persistente detectado en /app/data")
        else:
            logger.warning("⚠️ Volumen no detectado. Los datos NO persistirán")
        
        try:
            await self.db.init_db()
            await self.init_sistemas_tablas()
            logger.info("✅ Base de datos inicializada correctamente")
        except Exception as e:
            logger.error(f"❌ Error inicializando BD: {e}")
            traceback.print_exc()
        
        self.keep_alive_task.start()
        self.status_task.start()
        logger.info("✅ Tareas automáticas iniciadas")
    
    async def init_sistemas_tablas(self):
        """Inicializar todas las tablas"""
        async with aiosqlite.connect(DB_PATH) as db:
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
                    es_vip BOOLEAN DEFAULT 0,
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
            
            # Tabla de cajas compradas
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
            
            # Tabla de inventario
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
            
            # Tabla de misiones
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
            
            # Tabla de progreso misiones
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
            
            # Tabla de rachas
            await db.execute('''
                CREATE TABLE IF NOT EXISTS rachas (
                    usuario_id TEXT PRIMARY KEY,
                    racha INTEGER DEFAULT 0,
                    ultima_completada TIMESTAMP,
                    mejor_racha INTEGER DEFAULT 0
                )
            ''')
            
            # Tabla de inversiones
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
            
            # Tabla de préstamos
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
            
            # Tabla de transacciones cambio
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
            
            # Tabla de config banco
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
            
            # Config banco por defecto
            await db.execute('''
                INSERT OR IGNORE INTO banco_config (id, tasa_compra, tasa_venta)
                VALUES (1, 0.9, 1.1)
            ''')
            
            await db.commit()
        
        logger.info("✅ Tablas inicializadas")
    
    async def on_ready(self):
        logger.info(f"✅ Bot conectado como {self.user} (ID: {self.user.id})")
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
    """Obtener conexión a la base de datos"""
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

async def es_numero_bloqueado(rifa_id, numero):
    """Verifica si un número está bloqueado en la rifa"""
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
# COMANDO DE AYUDA
# ============================================

@bot.command(name="ayuda")
async def cmd_ayuda(ctx):
    """Ver todos los comandos disponibles"""
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
    `!comprarrandom [cantidad]` - Comprar boletos
    `!misboletos` - Ver tus boletos
    `!balance` - Ver balance
    `!topvp` - Ranking VP$
    `!ranking` - Top compradores
    `!historial` - Tu historial
    """
    embed.add_field(name="👤 BÁSICOS", value=basicos, inline=False)
    
    cajas = """
    `!cajas` - Ver cajas disponibles
    `!comprarcaja [tipo] [cantidad]` - Comprar cajas
    `!abrircaja [id]` - Abrir caja
    `!miscajas` - Ver tus cajas
    """
    embed.add_field(name="🎁 CAJAS", value=cajas, inline=False)
    
    banco = """
    `!banco` - Ver banco
    `!invertir [tipo] [monto]` - Invertir
    `!misinversiones` - Ver inversiones
    `!retirar [id]` - Retirar inversión
    `!cambiar [moneda] [cantidad]` - Cambiar moneda
    """
    embed.add_field(name="🏦 BANCO", value=banco, inline=False)
    
    misiones = """
    `!misiones` - Ver misiones
    `!miracha` - Ver racha
    `!reclamar_racha` - Reclamar bono
    """
    embed.add_field(name="📋 MISIONES", value=misiones, inline=False)
    
    if es_vendedor or es_director or es_ceo:
        vendedor = """
        `!vender [@user] [número]` - Vender boleto
        `!venderrandom [@user] [cantidad]` - Vender aleatorios
        `!misventas` - Ver ventas
        """
        embed.add_field(name="💰 VENDEDORES", value=vendedor, inline=False)
    
    if es_director or es_ceo:
        director = """
        `!crearifa [premio] [precio] [total] [bloqueados]` - Crear rifa
        `!cerrarifa` - Cerrar rifa
        `!iniciarsorteo [ganadores]` - Sorteo
        `!reporte` - Reporte
        """
        embed.add_field(name="🎯 DIRECTORES", value=director, inline=False)
    
    if es_ceo:
        ceo = """
        `!acreditarvp [@user] [cantidad]` - Acreditar VP$
        `!procesarvp [@user]` - Procesar pago
        `!procesadovp [@user] [cantidad]` - Confirmar pago
        `!verboletos [@user]` - Ver boletos de usuario
        `!2x1` - Activar 2x1
        `!cashbackdoble` - Activar cashback doble
        `!crearcaja [tipo] [precio] [premios] [probs]` - Crear caja
        `!estadisticas` - Estadísticas
        """
        embed.add_field(name="👑 CEO", value=ceo, inline=False)
    
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
    
    await ctx.send(embed=embed)

@bot.command(name="balance")
async def cmd_balance(ctx, usuario: discord.Member = None):
    if not await verificar_canal(ctx):
        return
    
    target = usuario if usuario else ctx.author
    
    if usuario and not await check_admin(ctx):
        await ctx.send(embed=embeds.crear_embed_error("Sin permiso"))
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
    if not await verificar_canal(ctx):
        return
    
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        cursor = await db.execute('SELECT * FROM cajas WHERE activo = 1')
        cajas = await cursor.fetchall()
    
    embed = discord.Embed(
        title="🎁 **CAJAS MISTERIOSAS**",
        color=COLORS['primary']
    )
    
    for caja in cajas:
        premios = json.loads(caja['premios'])
        probs = json.loads(caja['probabilidades'])
        texto = ""
        for p, prob in zip(premios[:3], probs[:3]):
            texto += f"• {p:,} VP$ ({prob}%)\n"
        if len(premios) > 3:
            texto += f"... y {len(premios)-3} más"
        
        embed.add_field(
            name=f"{caja['nombre']} - ${caja['precio']:,} VP$",
            value=texto,
            inline=False
        )
    
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
            await db.execute('''
                INSERT INTO cajas_compradas (usuario_id, caja_id)
                VALUES (?, ?)
            ''', (str(ctx.author.id), caja['id']))
        
        await db.commit()
    
    embed = discord.Embed(
        title="✅ Compra realizada",
        description=f"Compraste {cantidad}x {caja['nombre']} por ${precio_total:,} VP$",
        color=COLORS['success']
    )
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
    
    embed = discord.Embed(
        title="📦 Tus cajas sin abrir",
        color=COLORS['primary']
    )
    
    for caja in cajas[:20]:
        embed.add_field(
            name=f"ID: {caja['id']} - {caja['nombre']}",
            value=f"Usa `!abrircaja {caja['id']}` para abrir",
            inline=False
        )
    
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
        
        await db.execute('''
            UPDATE cajas_compradas SET abierta = 1, premio = ?, fecha_apertura = CURRENT_TIMESTAMP
            WHERE id = ?
        ''', (elegido, caja_id))
        
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
            title="🎉 CAJA ABIERTA",
            description=f"Has obtenido **${elegido:,} VP$**",
            color=COLORS['success']
        )
    else:
        embed = discord.Embed(
            title="😢 CAJA ABIERTA",
            description="No has ganado nada",
            color=COLORS['error']
        )
    
    await ctx.send(embed=embed)

# ============================================
# COMANDOS DE ADMINISTRACIÓN (CEO)
# ============================================

@bot.command(name="procesarvp")
async def cmd_procesar_vp(ctx, usuario: discord.Member):
    if not await check_ceo(ctx):
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
        
        await db.execute("UPDATE pagos_pendientes SET estado = 'procesando' WHERE id = ?", (pago[0],))
        await db.commit()
    
    embed = discord.Embed(
        title="⏳ Pago en proceso",
        description=f"Pago de **${pago[1]:,} NG$** de {usuario.mention} en proceso",
        color=COLORS['info']
    )
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
            INSERT INTO usuarios_balance (discord_id, nombre, balance)
            VALUES (?, ?, ?)
            ON CONFLICT(discord_id) DO UPDATE SET
                balance = balance + ?
        ''', (str(usuario.id), usuario.name, cantidad_vp, cantidad_vp))
        
        await db.execute("UPDATE pagos_pendientes SET estado = 'completado' WHERE usuario_id = ?", (str(usuario.id),))
        
        await db.commit()
    
    embed = discord.Embed(
        title="✅ Pago procesado",
        description=f"Se acreditaron **${cantidad_vp:,} VP$** a {usuario.mention}",
        color=COLORS['success']
    )
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
    
    embed = discord.Embed(
        title=f"🎟️ Boletos de {usuario.name}",
        description=f"Total: {len(boletos)} boletos",
        color=COLORS['primary']
    )
    
    for b in boletos[:15]:
        embed.add_field(
            name=f"#{b['numero']} - {b['rifa']}",
            value=f"${b['precio_pagado']:,} - {b['fecha_compra'][:10]}",
            inline=False
        )
    
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

@bot.command(name="crearcaja")
async def cmd_crear_caja(ctx, tipo: str, precio: int, premios: str, probabilidades: str):
    if not await check_ceo(ctx):
        return
    
    try:
        premios_list = json.loads(premios)
        probs_list = json.loads(probabilidades)
        
        if len(premios_list) != len(probs_list):
            await ctx.send(embed=embeds.crear_embed_error("Premios y probabilidades deben tener la misma longitud"))
            return
        
        if abs(sum(probs_list) - 100) > 0.01:
            await ctx.send(embed=embeds.crear_embed_error("La suma de probabilidades debe ser 100"))
            return
    except:
        await ctx.send(embed=embeds.crear_embed_error("Formato inválido. Usa: [1000,2000] y [50,50]"))
        return
    
    nombre = f"Caja {tipo.capitalize()}"
    
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute('''
            INSERT INTO cajas (tipo, nombre, precio, premios, probabilidades, activo)
            VALUES (?, ?, ?, ?, ?, 1)
        ''', (tipo.lower(), nombre, precio, premios, probabilidades))
        await db.commit()
    
    await ctx.message.delete()
    await ctx.send(embed=embeds.crear_embed_exito(f"✅ Caja '{nombre}' creada"))

# ============================================
# COMANDOS DE SORTEO
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
        
        cursor = await db.execute('''
            SELECT numero, comprador_id, comprador_nick FROM boletos
            WHERE rifa_id = ? AND estado = 'pagado'
        ''', (rifa['id'],))
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
    
    await ctx.send(embed=embed)
    
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute('UPDATE rifas SET estado = "finalizada" WHERE id = ?', (rifa['id'],))
        await db.commit()

# ============================================
# COMANDO DE VERSIÓN Y EJECUCIÓN
# ============================================

if __name__ == "__main__":
    try:
        if not config.BOT_TOKEN:
            print("❌ No hay BOT_TOKEN")
            sys.exit(1)
        
        bot.run(config.BOT_TOKEN)
    except Exception as e:
        logger.error(f"Error fatal: {e}")
