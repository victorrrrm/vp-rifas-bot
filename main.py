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
from datetime import datetime, timedelta
import config
from src.database.database import Database
import src.utils.embeds as embeds

# Crear carpetas necesarias
os.makedirs('data', exist_ok=True)
os.makedirs('src/logs', exist_ok=True)
os.makedirs('backups', exist_ok=True)

# ============================================
# CONFIGURACIÓN GLOBAL
# ============================================

VERSION = "3.2.0"
PREFIX = "!"
start_time = datetime.now()
reconnect_attempts = 0
MAX_RECONNECT_ATTEMPTS = 999999

reset_pending = None
reset_user_pending = None
sorteo_en_curso = False
sorteo_cancelado = False

REFERIDOS_PORCENTAJE = 10
REFERIDOS_DESCUENTO = 10
CASHBACK_PORCENTAJE = 10

ROLES_FIDELIZACION = {
    'BRONCE': 1483720270496661515,
    'PLATA': 1483720387178139758,
    'ORO': 1483720490601418822,
    'PLATINO': 1483720672185155625,
    'DIAMANTE': 1483720783422296165,
    'MASTER': 1483721013144584192
}

# ============================================
# LOGGING
# ============================================

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('src/logs/bot.log'),
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
        
    async def setup_hook(self):
        logger.info("🚀 Iniciando configuración del bot...")
        try:
            await self.db.init_db()
            await self.init_sistemas_tablas()
            logger.info("✅ Base de datos inicializada correctamente")
        except Exception as e:
            logger.error(f"❌ Error inicializando BD: {e}")
            traceback.print_exc()
        
        self.keep_alive_task.start()
        self.status_task.start()
        self.supervivencia_task.start()
        logger.info("✅ Tareas automáticas iniciadas")
    
    async def init_sistemas_tablas(self):
        async with aiosqlite.connect('data/rifas.db') as db:
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
            
            await db.commit()
        
        logger.info("✅ Tablas de sistemas inicializadas")
    
    async def on_ready(self):
        logger.info(f"✅ Bot conectado como {self.user} (ID: {self.user.id})")
        logger.info(f"🌐 En {len(self.guilds)} servidores")
        
        global reconnect_attempts
        reconnect_attempts = 0
        self.reconnecting = False
        
        await self.enviar_log_sistema(
            "🟢 **BOT INICIADO**", 
            f"Bot iniciado correctamente\nVersión: {VERSION}\nServidores: {len(self.guilds)}"
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
            f"Bot activo por {int(horas)}h {int(minutos)}m\nVersión: {VERSION}\nServidores: {len(self.guilds)}"
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

async def verificar_canal(ctx):
    if not ctx.guild:
        await ctx.send("❌ Este comando solo funciona en servidores")
        return False
    
    if ctx.channel.category_id != config.CATEGORIA_RIFAS:
        await ctx.send(f"❌ Este comando solo puede usarse en canales de la categoría <#{config.CATEGORIA_RIFAS}>")
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
    return (tiene_rol(member, config.ROLES['CEO']) or 
            tiene_rol(member, config.ROLES['DIRECTOR']))

async def check_vendedor(ctx):
    if not ctx.guild:
        return False
    member = ctx.guild.get_member(ctx.author.id)
    if not member:
        return False
    return (tiene_rol(member, config.ROLES['CEO']) or 
            tiene_rol(member, config.ROLES['DIRECTOR']) or 
            tiene_rol(member, config.ROLES['RIFAS']))

async def check_ceo(ctx):
    if not ctx.guild:
        return False
    member = ctx.guild.get_member(ctx.author.id)
    if not member:
        return False
    return tiene_rol(member, config.ROLES['CEO'])

async def generar_codigo_unico(usuario_id):
    hash_obj = hashlib.md5(usuario_id.encode())
    hash_hex = hash_obj.hexdigest()[:8].upper()
    return f"VP-{hash_hex}"

async def obtener_o_crear_codigo(usuario_id, usuario_nombre):
    async with aiosqlite.connect('data/rifas.db') as db:
        cursor = await db.execute('''
            SELECT codigo FROM referidos_codigos WHERE usuario_id = ?
        ''', (usuario_id,))
        result = await cursor.fetchone()
        
        if result:
            return result[0]
        else:
            codigo = await generar_codigo_unico(usuario_id)
            await db.execute('''
                INSERT INTO referidos_codigos (usuario_id, codigo)
                VALUES (?, ?)
            ''', (usuario_id, codigo))
            await db.commit()
            return codigo

async def obtener_nivel_por_gasto(gasto_total):
    async with aiosqlite.connect('data/rifas.db') as db:
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
    async with aiosqlite.connect('data/rifas.db') as db:
        cursor = await db.execute('''
            SELECT gasto_total FROM fidelizacion WHERE usuario_id = ?
        ''', (usuario_id,))
        result = await cursor.fetchone()
        
        if result:
            nuevo_gasto = result[0] + monto_compra
            await db.execute('''
                UPDATE fidelizacion SET gasto_total = ?, fecha_actualizacion = CURRENT_TIMESTAMP
                WHERE usuario_id = ?
            ''', (nuevo_gasto, usuario_id))
        else:
            nuevo_gasto = monto_compra
            await db.execute('''
                INSERT INTO fidelizacion (usuario_id, gasto_total)
                VALUES (?, ?)
            ''', (usuario_id, nuevo_gasto))
        
        nuevo_nivel = await obtener_nivel_por_gasto(nuevo_gasto)
        
        await db.execute('''
            UPDATE fidelizacion SET nivel = ? WHERE usuario_id = ?
        ''', (nuevo_nivel, usuario_id))
        
        await db.commit()
        return nuevo_nivel

async def aplicar_cashback(usuario_id, monto_compra):
    async with aiosqlite.connect('data/rifas.db') as db:
        cursor = await db.execute('''
            SELECT porcentaje FROM cashback_config WHERE id = 1
        ''')
        config_cb = await cursor.fetchone()
        porcentaje = config_cb[0] if config_cb else 10
        
        cashback = int(monto_compra * porcentaje / 100)
        
        await db.execute('''
            INSERT INTO cashback (usuario_id, cashback_acumulado)
            VALUES (?, ?)
            ON CONFLICT(usuario_id) DO UPDATE SET
                cashback_acumulado = cashback_acumulado + ?,
                ultima_actualizacion = CURRENT_TIMESTAMP
        ''', (usuario_id, cashback, cashback))
        
        await db.commit()
        return cashback

async def obtener_descuento_usuario(usuario_id):
    async with aiosqlite.connect('data/rifas.db') as db:
        cursor = await db.execute('''
            SELECT f.nivel, fc.descuento 
            FROM fidelizacion f
            JOIN fidelizacion_config fc ON f.nivel = fc.nivel
            WHERE f.usuario_id = ?
        ''', (usuario_id,))
        result = await cursor.fetchone()
        
        if result:
            return result[1]
        return 0

# ============================================
# COMANDOS
# ============================================

@bot.command(name="ayuda")
async def cmd_ayuda(ctx):
    if not await verificar_canal(ctx):
        return
    
    embed = discord.Embed(
        title="🎟️ **SISTEMA DE RIFAS VP**",
        description=f"Comandos disponibles (prefijo: `{PREFIX}`)\nVersión: {VERSION}",
        color=0xFFD700
    )
    
    embed.add_field(name="👤 **BÁSICOS**", value="""
    `!rifa` - Ver rifa activa
    `!comprarrandom [cantidad]` - Comprar boletos aleatorios
    `!misboletos` - Ver tus boletos
    `!balance` - Ver tu balance
    `!topvp` - Ranking VP$
    """, inline=False)
    
    embed.add_field(name="🤝 **REFERIDOS**", value="""
    `!codigo` - Tu código de referido
    `!usar [código]` - Usar código (solo una vez)
    `!misreferidos` - Ver tus referidos
    """, inline=False)
    
    embed.add_field(name="🏆 **FIDELIZACIÓN**", value="""
    `!nivel` - Tu nivel y beneficios
    `!topgastadores` - Ranking de gasto
    `!cashback` - Tu cashback acumulado
    `!topcashback` - Ranking cashback
    """, inline=False)
    
    await ctx.send(embed=embed)

@bot.command(name="codigo")
async def cmd_codigo(ctx):
    if not await verificar_canal(ctx):
        return
    
    codigo = await obtener_o_crear_codigo(str(ctx.author.id), ctx.author.name)
    
    embed = discord.Embed(
        title="🔗 **TU CÓDIGO DE REFERIDO**",
        description=f"`{codigo}`",
        color=0xFFD700
    )
    
    async with aiosqlite.connect('data/rifas.db') as db:
        cursor = await db.execute('''
            SELECT COUNT(*), SUM(comisiones_generadas) 
            FROM referidos_relaciones 
            WHERE referidor_id = ?
        ''', (str(ctx.author.id),))
        stats = await cursor.fetchone()
        total_referidos = stats[0] if stats else 0
        total_comisiones = stats[1] if stats and stats[1] else 0
    
    embed.add_field(name="📊 Tus referidos", value=f"**{total_referidos}** personas", inline=True)
    embed.add_field(name="💰 Comisiones", value=f"**{total_comisiones} VP$**", inline=True)
    
    await ctx.send(embed=embed)

@bot.command(name="usar")
async def cmd_usar_codigo(ctx, codigo: str):
    if not await verificar_canal(ctx):
        return
    
    async with aiosqlite.connect('data/rifas.db') as db:
        cursor = await db.execute('''
            SELECT * FROM referidos_relaciones WHERE referido_id = ?
        ''', (str(ctx.author.id),))
        ya_tiene = await cursor.fetchone()
        
        if ya_tiene:
            await ctx.send(embed=discord.Embed(title="❌ Error", description="Ya tienes un referidor registrado", color=0xFF0000))
            return
        
        cursor = await db.execute('''
            SELECT usuario_id FROM referidos_codigos WHERE codigo = ? AND usuario_id != ?
        ''', (codigo.upper(), str(ctx.author.id)))
        referidor = await cursor.fetchone()
        
        if not referidor:
            await ctx.send(embed=discord.Embed(title="❌ Error", description="Código inválido", color=0xFF0000))
            return
        
        await db.execute('''
            INSERT INTO referidos_relaciones (referido_id, referidor_id)
            VALUES (?, ?)
        ''', (str(ctx.author.id), referidor[0]))
        await db.commit()
    
    await ctx.send(embed=discord.Embed(title="✅ Código aplicado", color=0x00FF00))

@bot.command(name="comprarrandom")
async def cmd_comprar_random(ctx, cantidad: int = 1):
    if not await verificar_canal(ctx):
        return
    
    if cantidad < 1 or cantidad > 50:
        await ctx.send(embed=discord.Embed(title="❌ Error", description="Cantidad entre 1 y 50", color=0xFF0000))
        return
    
    rifa_activa = await bot.db.get_rifa_activa()
    if not rifa_activa:
        await ctx.send(embed=discord.Embed(title="❌ Error", description="No hay rifa activa", color=0xFF0000))
        return
    
    disponibles = await bot.db.get_boletos_disponibles(rifa_activa['id'])
    if len(disponibles) < cantidad:
        await ctx.send(embed=discord.Embed(title="❌ Error", description=f"Solo hay {len(disponibles)} boletos", color=0xFF0000))
        return
    
    async with aiosqlite.connect('data/rifas.db') as db:
        cursor = await db.execute('''
            SELECT balance FROM usuarios_balance WHERE discord_id = ?
        ''', (str(ctx.author.id),))
        result = await cursor.fetchone()
        balance = result[0] if result else 0
    
    precio_total = rifa_activa['precio_boleto'] * cantidad
    
    if balance < precio_total:
        await ctx.send(embed=discord.Embed(title="❌ Error", description=f"Necesitas {precio_total} VP$", color=0xFF0000))
        return
    
    seleccionados = random.sample(disponibles, cantidad)
    comprados = []
    
    async with aiosqlite.connect('data/rifas.db') as db:
        await db.execute('''
            UPDATE usuarios_balance SET balance = balance - ? WHERE discord_id = ?
        ''', (precio_total, str(ctx.author.id)))
        
        for num in seleccionados:
            await bot.db.comprar_boleto(
                rifa_activa['id'],
                num,
                str(ctx.author.id),
                ctx.author.name,
                None
            )
            comprados.append(num)
        await db.commit()
    
    await ctx.send(embed=discord.Embed(
        title="✅ Compra exitosa",
        description=f"Tus boletos: {', '.join(map(str, comprados))}\nTotal: ${precio_total}",
        color=0x00FF00
    ))

@bot.command(name="balance")
async def cmd_balance(ctx):
    if not await verificar_canal(ctx):
        return
    
    async with aiosqlite.connect('data/rifas.db') as db:
        cursor = await db.execute('''
            SELECT balance FROM usuarios_balance WHERE discord_id = ?
        ''', (str(ctx.author.id),))
        result = await cursor.fetchone()
        balance = result[0] if result else 0
    
    await ctx.send(embed=discord.Embed(title="💰 Tu balance", description=f"**{balance} VP$**", color=0xFFD700))

@bot.command(name="nivel")
async def cmd_nivel(ctx):
    if not await verificar_canal(ctx):
        return
    
    async with aiosqlite.connect('data/rifas.db') as db:
        cursor = await db.execute('''
            SELECT gasto_total, nivel FROM fidelizacion WHERE usuario_id = ?
        ''', (str(ctx.author.id),))
        data = await cursor.fetchone()
    
    if not data:
        await ctx.send(embed=discord.Embed(title="Sin compras", description="Aún no tienes historial", color=0x0099FF))
        return
    
    await ctx.send(embed=discord.Embed(
        title=f"🏆 Nivel: {data[1]}",
        description=f"Gasto total: ${data[0]:,}",
        color=0xFFD700
    ))

@bot.command(name="cashback")
async def cmd_cashback(ctx):
    if not await verificar_canal(ctx):
        return
    
    async with aiosqlite.connect('data/rifas.db') as db:
        cursor = await db.execute('''
            SELECT cashback_acumulado FROM cashback WHERE usuario_id = ?
        ''', (str(ctx.author.id),))
        result = await cursor.fetchone()
        cashback = result[0] if result else 0
    
    await ctx.send(embed=discord.Embed(title="💰 Cashback", description=f"Acumulado: ${cashback}", color=0x00FF00))

@bot.command(name="misboletos")
async def cmd_mis_boletos(ctx):
    if not await verificar_canal(ctx):
        return
    
    rifa_activa = await bot.db.get_rifa_activa()
    if not rifa_activa:
        await ctx.send(embed=discord.Embed(title="❌ Error", description="No hay rifa activa", color=0xFF0000))
        return
    
    async with aiosqlite.connect('data/rifas.db') as db:
        cursor = await db.execute('''
            SELECT numero FROM boletos 
            WHERE rifa_id = ? AND comprador_id = ?
        ''', (rifa_activa['id'], str(ctx.author.id)))
        boletos = await cursor.fetchall()
    
    if not boletos:
        await ctx.send(embed=discord.Embed(title="Sin boletos", description="No tienes boletos en esta rifa", color=0x0099FF))
        return
    
    numeros = [str(b[0]) for b in boletos]
    await ctx.send(embed=discord.Embed(
        title="🎟️ Tus boletos",
        description=f"Números: {', '.join(numeros)}",
        color=0xFFD700
    ))

@bot.command(name="rifa")
async def cmd_rifa(ctx):
    if not await verificar_canal(ctx):
        return
    
    rifa_activa = await bot.db.get_rifa_activa()
    if not rifa_activa:
        await ctx.send(embed=discord.Embed(title="Sin rifa", description="No hay rifa activa", color=0x0099FF))
        return
    
    await ctx.send(embed=discord.Embed(
        title=f"🎟️ {rifa_activa['nombre']}",
        description=f"Premio: {rifa_activa['premio']}\nPrecio: ${rifa_activa['precio_boleto']}",
        color=0xFFD700
    ))

# ============================================
# EJECUCIÓN
# ============================================

if __name__ == "__main__":
    try:
        bot.run(config.BOT_TOKEN)
    except Exception as e:
        logger.error(f"Error fatal: {e}")
