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
        logger.info("✅ Tareas automáticas iniciadas")
    
    async def init_sistemas_tablas(self):
        async with aiosqlite.connect('data/rifas.db') as db:
            # Tabla de códigos de referidos
            await db.execute('''
                CREATE TABLE IF NOT EXISTS referidos_codigos (
                    usuario_id TEXT PRIMARY KEY,
                    codigo TEXT UNIQUE NOT NULL,
                    fecha_creacion TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            ''')
            
            # Tabla de relaciones referido-referidor
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
            
            # Tabla de comisiones pagadas
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
            
            # Tabla de configuración de referidos
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
            
            # Tabla de fidelización por gasto
            await db.execute('''
                CREATE TABLE IF NOT EXISTS fidelizacion (
                    usuario_id TEXT PRIMARY KEY,
                    gasto_total INTEGER DEFAULT 0,
                    nivel TEXT DEFAULT 'BRONCE',
                    fecha_actualizacion TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            ''')
            
            # Tabla de configuración de niveles
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
            
            # Tabla de cashback
            await db.execute('''
                CREATE TABLE IF NOT EXISTS cashback (
                    usuario_id TEXT PRIMARY KEY,
                    cashback_acumulado INTEGER DEFAULT 0,
                    cashback_recibido INTEGER DEFAULT 0,
                    ultima_actualizacion TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            ''')
            
            # Tabla de configuración de cashback
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

async def procesar_comision_referido(comprador_id, monto_compra):
    async with aiosqlite.connect('data/rifas.db') as db:
        cursor = await db.execute('''
            SELECT referidor_id FROM referidos_relaciones WHERE referido_id = ?
        ''', (comprador_id,))
        result = await cursor.fetchone()
        
        if not result:
            return
        
        referidor_id = result[0]
        
        cursor = await db.execute('''
            SELECT porcentaje_comision FROM referidos_config WHERE id = 1
        ''')
        config_ref = await cursor.fetchone()
        porcentaje = config_ref[0] if config_ref else 10
        
        comision = int(monto_compra * porcentaje / 100)
        
        await db.execute('''
            INSERT INTO usuarios_balance (discord_id, nombre, balance)
            VALUES (?, (SELECT nombre FROM clientes WHERE discord_id = ?), ?)
            ON CONFLICT(discord_id) DO UPDATE SET
                balance = balance + ?
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

# ============================================
# COMANDOS DE USUARIO
# ============================================

@bot.command(name="ayuda")
async def cmd_ayuda(ctx):
    if not await verificar_canal(ctx):
        return
    
    member = ctx.guild.get_member(ctx.author.id)
    es_ceo = tiene_rol(member, config.ROLES['CEO'])
    es_director = tiene_rol(member, config.ROLES['DIRECTOR'])
    es_vendedor = tiene_rol(member, config.ROLES['RIFAS'])
    
    embed = discord.Embed(
        title="🎟️ **SISTEMA DE RIFAS VP**",
        description=f"Comandos disponibles (prefijo: `{PREFIX}`)\nVersión: {VERSION}",
        color=config.COLORS['primary']
    )
    
    basicos = """
    `!rifa` - Ver rifa activa
    `!comprarrandom [cantidad]` - Comprar boletos aleatorios
    `!misboletos` - Ver tus boletos
    `!balance` - Ver tu balance
    `!topvp` - Ranking de VP$
    `!ranking` - Top compradores
    `!historial` - Tu historial
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
    """
    embed.add_field(name="🏆 **FIDELIZACIÓN**", value=fidelizacion, inline=False)
    
    if es_vendedor or es_director or es_ceo:
        vendedor = """
        `!venderrandom [@usuario] [cantidad]` - Vender aleatorios
        `!misventas` - Ver tus ventas
        `!listaboletos` - Lista de boletos
        """
        embed.add_field(name="💰 **VENDEDORES**", value=vendedor, inline=False)
    
    if es_director or es_ceo:
        director = """
        `!crearifa [nombre] [premio] [valor] [precio] [total]` - Crear rifa
        `!aumentarnumeros [cantidad]` - Ampliar rifa
        `!cerrarifa` - Cerrar rifa
        `!iniciarsorteo [ganadores]` - Iniciar sorteo
        `!cancelarsorteo` - Cancelar sorteo
        `!finalizarrifa [id] [ganadores]` - Finalizar rifa
        `!vendedoradd [@usuario] [%]` - Añadir vendedor
        `!pagarcomisiones` - Ver comisiones
        `!confirmarpago [@usuario]` - Pagar comisiones
        `!reporte` - Reporte de rifa
        """
        embed.add_field(name="🎯 **DIRECTORES**", value=director, inline=False)
    
    if es_ceo:
        ceo = """
        `!acreditarvp [@usuario] [cantidad]` - Acreditar VP$
        `!retirarvp [@usuario] [cantidad]` - Retirar VP$
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
        """
        embed.add_field(name="👑 **CEO**", value=ceo, inline=False)
    
    embed.set_footer(text="Ejemplo: !comprarrandom 3")
    await ctx.send(embed=embed)

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
                    f"**Usuarios:** {len(bot.users)}",
        color=config.COLORS['primary']
    )
    await ctx.send(embed=embed)

@bot.command(name="rifa")
async def cmd_rifa(ctx):
    if not await verificar_canal(ctx):
        return
    
    rifa_activa = await bot.db.get_rifa_activa()
    if not rifa_activa:
        await ctx.send(embed=embeds.crear_embed_error("No hay rifa activa"))
        return
    
    embed = discord.Embed(
        title=f"🎟️ {rifa_activa['nombre']}",
        description=f"**{rifa_activa['premio']}**",
        color=config.COLORS['primary']
    )
    embed.add_field(name="🏆 Premio", value=f"${rifa_activa['valor_premio']:,}", inline=True)
    embed.add_field(name="💰 Precio", value=f"${rifa_activa['precio_boleto']:,}", inline=True)
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
    if len(disponibles) < cantidad:
        await ctx.send(embed=embeds.crear_embed_error(f"Solo hay {len(disponibles)} boletos"))
        return
    
    precio_boleto = rifa_activa['precio_boleto']
    precio_total = precio_boleto * cantidad
    descuento = await obtener_descuento_usuario(str(ctx.author.id))
    precio_con_descuento = int(precio_total * (100 - descuento) / 100)
    
    async with aiosqlite.connect('data/rifas.db') as db:
        cursor = await db.execute('''
            SELECT balance FROM usuarios_balance WHERE discord_id = ?
        ''', (str(ctx.author.id),))
        result = await cursor.fetchone()
        balance = result[0] if result else 0
    
    if balance < precio_con_descuento:
        await ctx.send(embed=embeds.crear_embed_error(f"Necesitas {precio_con_descuento} VP$"))
        return
    
    seleccionados = random.sample(disponibles, cantidad)
    comprados = []
    
    async with aiosqlite.connect('data/rifas.db') as db:
        await db.execute('''
            UPDATE usuarios_balance SET balance = ? WHERE discord_id = ?
        ''', (balance - precio_con_descuento, str(ctx.author.id)))
        
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
    
    embed = discord.Embed(
        title="✅ Compra exitosa",
        description=f"**Tus números:** {', '.join(map(str, comprados))}\n"
                    f"**Total:** ${precio_con_descuento}\n"
                    f"**Descuento:** {descuento}%\n"
                    f"**Cashback:** ${cashback}",
        color=config.COLORS['success']
    )
    await ctx.send(embed=embed)
    await enviar_log(ctx, "🎟️ COMPRA", f"{ctx.author.name} compró {len(comprados)} boletos")

@bot.command(name="misboletos")
async def cmd_mis_boletos(ctx):
    if not await verificar_canal(ctx):
        return
    
    rifa_activa = await bot.db.get_rifa_activa()
    if not rifa_activa:
        await ctx.send(embed=embeds.crear_embed_error("No hay rifa activa"))
        return
    
    async with aiosqlite.connect('data/rifas.db') as db:
        cursor = await db.execute('''
            SELECT numero FROM boletos 
            WHERE rifa_id = ? AND comprador_id = ?
        ''', (rifa_activa['id'], str(ctx.author.id)))
        boletos = await cursor.fetchall()
    
    if not boletos:
        await ctx.send(embed=embeds.crear_embed_error("No tienes boletos"))
        return
    
    numeros = [str(b[0]) for b in boletos]
    embed = discord.Embed(
        title="🎟️ Tus boletos",
        description=f"Números: {', '.join(numeros)}",
        color=config.COLORS['primary']
    )
    await ctx.send(embed=embed)

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
    
    embed = discord.Embed(
        title="💰 Tu balance",
        description=f"**{balance} VP$**",
        color=config.COLORS['primary']
    )
    await ctx.send(embed=embed)

@bot.command(name="topvp")
async def cmd_top_vp(ctx):
    if not await verificar_canal(ctx):
        return
    
    async with aiosqlite.connect('data/rifas.db') as db:
        db.row_factory = aiosqlite.Row
        cursor = await db.execute('''
            SELECT nombre, balance FROM usuarios_balance 
            WHERE balance > 0 
            ORDER BY balance DESC 
            LIMIT 10
        ''')
        usuarios = await cursor.fetchall()
    
    if not usuarios:
        await ctx.send(embed=embeds.crear_embed_info("Sin datos", "No hay usuarios con VP$"))
        return
    
    embed = discord.Embed(title="🏆 TOP 10 VP$", color=config.COLORS['primary'])
    for i, u in enumerate(usuarios, 1):
        medalla = {1: "🥇", 2: "🥈", 3: "🥉"}.get(i, f"{i}.")
        embed.add_field(name=f"{medalla} {u['nombre']}", value=f"**{u['balance']} VP$**", inline=False)
    
    await ctx.send(embed=embed)

@bot.command(name="ranking")
async def cmd_ranking(ctx):
    if not await verificar_canal(ctx):
        return
    
    async with aiosqlite.connect('data/rifas.db') as db:
        db.row_factory = aiosqlite.Row
        cursor = await db.execute('''
            SELECT nombre, total_compras, total_gastado 
            FROM clientes 
            ORDER BY total_gastado DESC 
            LIMIT 10
        ''')
        usuarios = await cursor.fetchall()
    
    embed = discord.Embed(title="🏆 TOP COMPRADORES", color=config.COLORS['primary'])
    for i, u in enumerate(usuarios, 1):
        medalla = {1: "🥇", 2: "🥈", 3: "🥉"}.get(i, f"{i}.")
        embed.add_field(
            name=f"{medalla} {u['nombre']}",
            value=f"{u['total_compras']} boletos | ${u['total_gastado']:,}",
            inline=False
        )
    
    await ctx.send(embed=embed)

@bot.command(name="historial")
async def cmd_historial(ctx):
    if not await verificar_canal(ctx):
        return
    
    async with aiosqlite.connect('data/rifas.db') as db:
        db.row_factory = aiosqlite.Row
        cursor = await db.execute('''
            SELECT b.numero, r.nombre as rifa, b.fecha_compra, b.precio_pagado
            FROM boletos b
            JOIN rifas r ON b.rifa_id = r.id
            WHERE b.comprador_id = ?
            ORDER BY b.fecha_compra DESC
            LIMIT 20
        ''', (str(ctx.author.id),))
        boletos = await cursor.fetchall()
    
    if not boletos:
        await ctx.send(embed=embeds.crear_embed_error("Sin historial"))
        return
    
    embed = discord.Embed(title="📜 Tu historial", color=config.COLORS['primary'])
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
        color=config.COLORS['primary']
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
    
    embed.add_field(name="📊 Referidos", value=f"**{total_referidos}**", inline=True)
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
            await ctx.send(embed=embeds.crear_embed_error("Ya tienes un referidor"))
            return
        
        cursor = await db.execute('''
            SELECT usuario_id FROM referidos_codigos WHERE codigo = ? AND usuario_id != ?
        ''', (codigo.upper(), str(ctx.author.id)))
        referidor = await cursor.fetchone()
        
        if not referidor:
            await ctx.send(embed=embeds.crear_embed_error("Código inválido"))
            return
        
        await db.execute('''
            INSERT INTO referidos_relaciones (referido_id, referidor_id)
            VALUES (?, ?)
        ''', (str(ctx.author.id), referidor[0]))
        await db.commit()
    
    await ctx.send(embed=embeds.crear_embed_exito("Código aplicado correctamente"))

@bot.command(name="misreferidos")
async def cmd_mis_referidos(ctx):
    if not await verificar_canal(ctx):
        return
    
    async with aiosqlite.connect('data/rifas.db') as db:
        db.row_factory = aiosqlite.Row
        cursor = await db.execute('''
            SELECT r.referido_id, c.nombre, r.total_compras, r.comisiones_generadas
            FROM referidos_relaciones r
            LEFT JOIN clientes c ON r.referido_id = c.discord_id
            WHERE r.referidor_id = ?
            ORDER BY r.fecha_registro DESC
            LIMIT 20
        ''', (str(ctx.author.id),))
        referidos = await cursor.fetchall()
    
    if not referidos:
        await ctx.send(embed=embeds.crear_embed_error("No tienes referidos"))
        return
    
    embed = discord.Embed(title="👥 Tus referidos", color=config.COLORS['primary'])
    for ref in referidos[:10]:
        nombre = ref['nombre'] or f"Usuario"
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
    
    async with aiosqlite.connect('data/rifas.db') as db:
        await db.execute('''
            UPDATE referidos_config SET porcentaje_comision = ? WHERE id = 1
        ''', (porcentaje,))
        await db.commit()
    
    await ctx.send(embed=embeds.crear_embed_exito(f"Comisión: {porcentaje}%"))

@bot.command(name="setrefdescuento")
async def cmd_set_ref_descuento(ctx, porcentaje: int):
    if not await check_ceo(ctx):
        return
    
    if porcentaje < 0 or porcentaje > 50:
        await ctx.send(embed=embeds.crear_embed_error("0-50%"))
        return
    
    global REFERIDOS_DESCUENTO
    REFERIDOS_DESCUENTO = porcentaje
    
    async with aiosqlite.connect('data/rifas.db') as db:
        await db.execute('''
            UPDATE referidos_config SET porcentaje_descuento = ? WHERE id = 1
        ''', (porcentaje,))
        await db.commit()
    
    await ctx.send(embed=embeds.crear_embed_exito(f"Descuento: {porcentaje}%"))

# ============================================
# SISTEMA DE FIDELIZACIÓN
# ============================================

@bot.command(name="nivel")
async def cmd_nivel(ctx):
    if not await verificar_canal(ctx):
        return
    
    async with aiosqlite.connect('data/rifas.db') as db:
        db.row_factory = aiosqlite.Row
        cursor = await db.execute('''
            SELECT gasto_total, nivel FROM fidelizacion WHERE usuario_id = ?
        ''', (str(ctx.author.id),))
        data = await cursor.fetchone()
        
        cursor = await db.execute('''
            SELECT * FROM fidelizacion_config WHERE nivel = ?
        ''', (data['nivel'] if data else 'BRONCE',))
        beneficios = await cursor.fetchone()
    
    if not data:
        await ctx.send(embed=embeds.crear_embed_info("Sin compras", "Aún no tienes historial"))
        return
    
    embed = discord.Embed(
        title=f"🏆 Nivel: {data['nivel']}",
        description=f"Gasto total: **${data['gasto_total']:,} VP$**",
        color=config.COLORS['primary']
    )
    
    if beneficios and beneficios['descuento'] > 0:
        embed.add_field(name="💰 Descuento", value=f"**{beneficios['descuento']}%**", inline=True)
    
    await ctx.send(embed=embed)

@bot.command(name="topgastadores")
async def cmd_top_gastadores(ctx):
    if not await verificar_canal(ctx):
        return
    
    async with aiosqlite.connect('data/rifas.db') as db:
        db.row_factory = aiosqlite.Row
        cursor = await db.execute('''
            SELECT c.nombre, f.gasto_total, f.nivel
            FROM fidelizacion f
            LEFT JOIN clientes c ON f.usuario_id = c.discord_id
            WHERE f.gasto_total > 0
            ORDER BY f.gasto_total DESC
            LIMIT 10
        ''')
        top = await cursor.fetchall()
    
    if not top:
        await ctx.send(embed=embeds.crear_embed_info("Sin datos", "No hay gastadores"))
        return
    
    embed = discord.Embed(title="🏆 TOP GASTADORES", color=config.COLORS['primary'])
    for i, u in enumerate(top, 1):
        nombre = u['nombre'] or "Usuario"
        embed.add_field(
            name=f"{i}. {nombre}",
            value=f"Gastado: ${u['gasto_total']:,} | {u['nivel']}",
            inline=False
        )
    
    await ctx.send(embed=embed)

# ============================================
# SISTEMA DE CASHBACK
# ============================================

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
    
    embed = discord.Embed(
        title="💰 Cashback",
        description=f"Acumulado: **${cashback} VP$**",
        color=config.COLORS['primary']
    )
    await ctx.send(embed=embed)

@bot.command(name="topcashback")
async def cmd_top_cashback(ctx):
    if not await verificar_canal(ctx):
        return
    
    async with aiosqlite.connect('data/rifas.db') as db:
        db.row_factory = aiosqlite.Row
        cursor = await db.execute('''
            SELECT c.usuario_id, cl.nombre, c.cashback_acumulado
            FROM cashback c
            LEFT JOIN clientes cl ON c.usuario_id = cl.discord_id
            WHERE c.cashback_acumulado > 0
            ORDER BY c.cashback_acumulado DESC
            LIMIT 10
        ''')
        top = await cursor.fetchall()
    
    if not top:
        await ctx.send(embed=embeds.crear_embed_info("Sin datos", "No hay cashback"))
        return
    
    embed = discord.Embed(title="💰 TOP CASHBACK", color=config.COLORS['primary'])
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
    
    async with aiosqlite.connect('data/rifas.db') as db:
        await db.execute('''
            UPDATE cashback_config SET porcentaje = ? WHERE id = 1
        ''', (porcentaje,))
        await db.commit()
    
    await ctx.send(embed=embeds.crear_embed_exito(f"Cashback: {porcentaje}%"))

@bot.command(name="pagarcashback")
async def cmd_pagar_cashback(ctx):
    if not await check_ceo(ctx):
        return
    
    await ctx.send("💰 Procesando pagos...")
    
    async with aiosqlite.connect('data/rifas.db') as db:
        db.row_factory = aiosqlite.Row
        cursor = await db.execute('''
            SELECT usuario_id, cashback_acumulado FROM cashback WHERE cashback_acumulado > 0
        ''')
        usuarios = await cursor.fetchall()
        
        if not usuarios:
            await ctx.send(embed=embeds.crear_embed_error("No hay cashback"))
            return
        
        total_pagado = 0
        for u in usuarios:
            await db.execute('''
                INSERT INTO usuarios_balance (discord_id, nombre, balance)
                VALUES (?, (SELECT nombre FROM clientes WHERE discord_id = ?), ?)
                ON CONFLICT(discord_id) DO UPDATE SET
                    balance = balance + ?
            ''', (u['usuario_id'], u['usuario_id'], u['cashback_acumulado'], u['cashback_acumulado']))
            
            await db.execute('''
                UPDATE cashback SET cashback_acumulado = 0 WHERE usuario_id = ?
            ''', (u['usuario_id'],))
            
            total_pagado += u['cashback_acumulado']
        
        await db.commit()
    
    await ctx.send(embed=embeds.crear_embed_exito(f"Pagados ${total_pagado}"))

@bot.command(name="resetcashback")
async def cmd_reset_cashback(ctx):
    if not await check_ceo(ctx):
        return
    
    async with aiosqlite.connect('data/rifas.db') as db:
        await db.execute('UPDATE cashback SET cashback_acumulado = 0')
        await db.commit()
    
    await ctx.send(embed=embeds.crear_embed_exito("Cashback reseteados"))

# ============================================
# COMANDOS DE VENDEDOR
# ============================================

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
    if len(disponibles) < cantidad:
        await ctx.send(embed=embeds.crear_embed_error(f"Solo {len(disponibles)} disponibles"))
        return
    
    async with aiosqlite.connect('data/rifas.db') as db:
        cursor = await db.execute('''
            SELECT balance FROM usuarios_balance WHERE discord_id = ?
        ''', (str(usuario.id),))
        result = await cursor.fetchone()
        balance = result[0] if result else 0
    
    precio_total = rifa_activa['precio_boleto'] * cantidad
    descuento = await obtener_descuento_usuario(str(usuario.id))
    precio_final = int(precio_total * (100 - descuento) / 100)
    
    if balance < precio_final:
        await ctx.send(embed=embeds.crear_embed_error(f"Usuario necesita {precio_final} VP$"))
        return
    
    seleccionados = random.sample(disponibles, cantidad)
    comprados = []
    
    async with aiosqlite.connect('data/rifas.db') as db:
        await db.execute('''
            UPDATE usuarios_balance SET balance = ? WHERE discord_id = ?
        ''', (balance - precio_final, str(usuario.id)))
        
        for num in seleccionados:
            await db.execute('''
                INSERT INTO boletos (rifa_id, numero, comprador_id, comprador_nick, vendedor_id, precio_pagado)
                VALUES (?, ?, ?, ?, ?, ?)
            ''', (rifa_activa['id'], num, str(usuario.id), usuario.name, str(ctx.author.id), rifa_activa['precio_boleto']))
            comprados.append(num)
        
        await db.commit()
    
    await ctx.send(embed=embeds.crear_embed_exito(
        f"Venta a {usuario.name}\nNúmeros: {', '.join(map(str, comprados))}\nTotal: ${precio_final}"
    ))

@bot.command(name="misventas")
async def cmd_mis_ventas(ctx):
    if not await verificar_canal(ctx):
        return
    if not await check_vendedor(ctx):
        await ctx.send(embed=embeds.crear_embed_error("Sin permiso"))
        return
    
    async with aiosqlite.connect('data/rifas.db') as db:
        db.row_factory = aiosqlite.Row
        cursor = await db.execute('''
            SELECT b.numero, r.nombre as rifa, b.comprador_nick, b.precio_pagado, b.fecha_compra
            FROM boletos b
            JOIN rifas r ON b.rifa_id = r.id
            WHERE b.vendedor_id = ?
            ORDER BY b.fecha_compra DESC
            LIMIT 20
        ''', (str(ctx.author.id),))
        ventas = await cursor.fetchall()
        
        cursor = await db.execute('''
            SELECT comisiones_pendientes FROM vendedores WHERE discord_id = ?
        ''', (str(ctx.author.id),))
        vendedor = await cursor.fetchone()
    
    embed = discord.Embed(title="💰 Tus ventas", color=config.COLORS['primary'])
    
    if vendedor and vendedor[0] > 0:
        embed.add_field(name="Comisiones", value=f"**${vendedor[0]}**", inline=False)
    
    if ventas:
        for v in ventas[:5]:
            embed.add_field(
                name=f"#{v['numero']} - {v['rifa']}",
                value=f"{v['comprador_nick']} | ${v['precio_pagado']} | {v['fecha_compra'][:10]}",
                inline=False
            )
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
        description=f"**Total:** {rifa_activa['total_boletos']}\n"
                    f"**Disponibles:** {len(disponibles)}\n"
                    f"**Vendidos:** {vendidos}",
        color=config.COLORS['info']
    )
    
    if disponibles:
        muestra = random.sample(disponibles, min(10, len(disponibles)))
        muestra.sort()
        embed.add_field(name="Ejemplos disponibles", value=f"`{', '.join(map(str, muestra))}`", inline=False)
    
    await ctx.send(embed=embed)

# ============================================
# COMANDOS DE ADMIN
# ============================================

@bot.command(name="crearifa")
async def cmd_crear_rifa(ctx, nombre: str, premio: str, valor: int, precio: int, total: int):
    if not await check_admin(ctx):
        return
    
    rifa_id = await bot.db.crear_rifa(nombre, premio, valor, precio, total)
    
    embed = embeds.crear_embed_exito(f"✅ Rifa creada ID: {rifa_id}")
    await ctx.send(embed=embed)

@bot.command(name="aumentarnumeros")
async def cmd_aumentar_numeros(ctx, cantidad: int):
    if not await check_admin(ctx):
        return
    
    rifa = await bot.db.get_rifa_activa()
    if not rifa:
        await ctx.send(embed=embeds.crear_embed_error("No hay rifa"))
        return
    
    if cantidad <= 0:
        await ctx.send(embed=embeds.crear_embed_error("Cantidad positiva"))
        return
    
    async with aiosqlite.connect('data/rifas.db') as db:
        nuevo_total = rifa['total_boletos'] + cantidad
        await db.execute('''
            UPDATE rifas SET total_boletos = ? WHERE id = ?
        ''', (nuevo_total, rifa['id']))
        await db.commit()
    
    await ctx.send(embed=embeds.crear_embed_exito(f"✅ Total: {nuevo_total} boletos"))

@bot.command(name="cerrarifa")
async def cmd_cerrar_rifa(ctx):
    if not await check_admin(ctx):
        return
    
    rifa = await bot.db.get_rifa_activa()
    if not rifa:
        await ctx.send(embed=embeds.crear_embed_error("No hay rifa"))
        return
    
    await bot.db.cerrar_rifa(rifa['id'])
    await ctx.send(embed=embeds.crear_embed_exito("✅ Rifa cerrada"))

@bot.command(name="iniciarsorteo")
async def cmd_iniciar_sorteo(ctx, ganadores: int = 1):
    global sorteo_en_curso, sorteo_cancelado
    
    if not await check_admin(ctx):
        return
    
    if sorteo_en_curso:
        await ctx.send(embed=embeds.crear_embed_error("Ya hay sorteo"))
        return
    
    rifa = await bot.db.get_rifa_activa()
    if not rifa:
        await ctx.send(embed=embeds.crear_embed_error("No hay rifa"))
        return
    
    vendidos = await bot.db.get_boletos_vendidos(rifa['id'])
    if vendidos < ganadores:
        await ctx.send(embed=embeds.crear_embed_error(f"Solo {vendidos} vendidos"))
        return
    
    sorteo_en_curso = True
    sorteo_cancelado = False
    
    await ctx.send("🎲 Sorteo en 10 segundos...")
    await asyncio.sleep(10)
    
    if sorteo_cancelado:
        sorteo_en_curso = False
        return
    
    await ctx.send("**3...**")
    await asyncio.sleep(1)
    await ctx.send("**2...**")
    await asyncio.sleep(1)
    await ctx.send("**1...**")
    await asyncio.sleep(1)
    
    async with aiosqlite.connect('data/rifas.db') as db:
        db.row_factory = aiosqlite.Row
        cursor = await db.execute('''
            SELECT numero, comprador_id, comprador_nick FROM boletos 
            WHERE rifa_id = ? AND estado = 'pagado'
        ''', (rifa['id'],))
        boletos = await cursor.fetchall()
    
    if len(boletos) <= ganadores:
        ganadores_sel = boletos
    else:
        ganadores_sel = random.sample(boletos, ganadores)
    
    embed = discord.Embed(title="🎉 Ganadores", color=config.COLORS['success'])
    for i, b in enumerate(ganadores_sel, 1):
        embed.add_field(name=f"#{i}", value=f"#{b['numero']} - {b['comprador_nick']}", inline=False)
    
    await ctx.send(embed=embed)
    sorteo_en_curso = False

@bot.command(name="cancelarsorteo")
async def cmd_cancelar_sorteo(ctx):
    global sorteo_en_curso, sorteo_cancelado
    
    if not await check_admin(ctx):
        return
    
    if not sorteo_en_curso:
        await ctx.send(embed=embeds.crear_embed_error("No hay sorteo"))
        return
    
    sorteo_cancelado = True
    sorteo_en_curso = False
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
    
    async with aiosqlite.connect('data/rifas.db') as db:
        db.row_factory = aiosqlite.Row
        cursor = await db.execute('SELECT * FROM rifas WHERE id = ?', (id_rifa,))
        rifa = await cursor.fetchone()
        
        if not rifa:
            await ctx.send(embed=embeds.crear_embed_error("ID inválido"))
            return
        
        cursor = await db.execute('''
            SELECT numero, comprador_id, comprador_nick FROM boletos 
            WHERE rifa_id = ? AND estado = 'pagado'
        ''', (id_rifa,))
        boletos = await cursor.fetchall()
    
    if not boletos:
        await ctx.send(embed=embeds.crear_embed_error("Sin boletos"))
        return
    
    if len(boletos) < ganadores:
        await ctx.send(embed=embeds.crear_embed_error(f"Solo {len(boletos)} boletos"))
        return
    
    ganadores_sel = random.sample(boletos, ganadores)
    
    embed = discord.Embed(
        title=f"🎉 Rifa #{id_rifa} finalizada",
        description=f"Premio: {rifa['premio']}",
        color=config.COLORS['success']
    )
    
    for i, b in enumerate(ganadores_sel, 1):
        embed.add_field(name=f"Ganador {i}", value=f"#{b['numero']} - {b['comprador_nick']}", inline=False)
    
    await ctx.send(embed=embed)

@bot.command(name="vendedoradd")
async def cmd_vendedor_add(ctx, usuario: discord.Member, comision: int = 15):
    if not await check_admin(ctx):
        return
    
    async with aiosqlite.connect('data/rifas.db') as db:
        await db.execute('''
            INSERT INTO vendedores (discord_id, nombre, comision)
            VALUES (?, ?, ?)
            ON CONFLICT(discord_id) DO UPDATE SET
                comision = ?, nombre = ?
        ''', (str(usuario.id), usuario.name, comision, comision, usuario.name))
        await db.commit()
    
    try:
        rol = ctx.guild.get_role(config.ROLES['RIFAS'])
        if rol and rol not in usuario.roles:
            await usuario.add_roles(rol)
    except:
        pass
    
    await ctx.send(embed=embeds.crear_embed_exito(f"{usuario.name} es vendedor ({comision}%)"))

@bot.command(name="pagarcomisiones")
async def cmd_pagar_comisiones(ctx):
    if not await check_admin(ctx):
        return
    
    async with aiosqlite.connect('data/rifas.db') as db:
        db.row_factory = aiosqlite.Row
        cursor = await db.execute('''
            SELECT nombre, comisiones_pendientes FROM vendedores WHERE comisiones_pendientes > 0
        ''')
        vendedores = await cursor.fetchall()
    
    if not vendedores:
        await ctx.send(embed=embeds.crear_embed_info("Info", "No hay comisiones"))
        return
    
    embed = discord.Embed(title="💰 Comisiones", color=config.COLORS['primary'])
    for v in vendedores:
        embed.add_field(name=v['nombre'], value=f"${v['comisiones_pendientes']}", inline=True)
    
    await ctx.send(embed=embed)

@bot.command(name="confirmarpago")
async def cmd_confirmar_pago(ctx, vendedor: discord.Member):
    if not await check_admin(ctx):
        return
    
    async with aiosqlite.connect('data/rifas.db') as db:
        cursor = await db.execute('''
            SELECT comisiones_pendientes FROM vendedores WHERE discord_id = ?
        ''', (str(vendedor.id),))
        result = await cursor.fetchone()
        
        if not result or result[0] == 0:
            await ctx.send(embed=embeds.crear_embed_error("Sin comisiones"))
            return
        
        monto = result[0]
        
        await db.execute('''
            UPDATE vendedores SET comisiones_pendientes = 0 WHERE discord_id = ?
        ''', (str(vendedor.id),))
        await db.commit()
    
    await ctx.send(embed=embeds.crear_embed_exito(f"Pagados ${monto} a {vendedor.name}"))

@bot.command(name="reporte")
async def cmd_reporte(ctx):
    if not await check_admin(ctx):
        return
    
    rifa = await bot.db.get_rifa_activa()
    if not rifa:
        await ctx.send(embed=embeds.crear_embed_error("No hay rifa"))
        return
    
    vendidos = await bot.db.get_boletos_vendidos(rifa['id'])
    recaudado = vendidos * rifa['precio_boleto']
    
    embed = discord.Embed(
        title=f"📊 Reporte - {rifa['nombre']}",
        description=f"**Vendidos:** {vendidos}/{rifa['total_boletos']}\n"
                    f"**Recaudado:** ${recaudado}",
        color=config.COLORS['primary']
    )
    
    await ctx.send(embed=embed)

# ============================================
# COMANDOS DE CEO
# ============================================

@bot.command(name="acreditarvp")
async def cmd_acreditarvp(ctx, usuario: discord.Member, cantidad: int):
    if not await check_ceo(ctx):
        return
    
    if cantidad <= 0:
        await ctx.send(embed=embeds.crear_embed_error("Cantidad positiva"))
        return
    
    async with aiosqlite.connect('data/rifas.db') as db:
        await db.execute('''
            INSERT INTO usuarios_balance (discord_id, nombre, balance)
            VALUES (?, ?, ?)
            ON CONFLICT(discord_id) DO UPDATE SET
                balance = balance + ?
        ''', (str(usuario.id), usuario.name, cantidad, cantidad))
        await db.commit()
    
    await ctx.send(embed=embeds.crear_embed_exito(f"{cantidad} VP$ a {usuario.name}"))

@bot.command(name="retirarvp")
async def cmd_retirarvp(ctx, usuario: discord.Member, cantidad: int):
    if not await check_ceo(ctx):
        return
    
    if cantidad <= 0:
        await ctx.send(embed=embeds.crear_embed_error("Cantidad positiva"))
        return
    
    async with aiosqlite.connect('data/rifas.db') as db:
        cursor = await db.execute('''
            SELECT balance FROM usuarios_balance WHERE discord_id = ?
        ''', (str(usuario.id),))
        result = await cursor.fetchone()
        
        if not result or result[0] < cantidad:
            await ctx.send(embed=embeds.crear_embed_error("Saldo insuficiente"))
            return
        
        await db.execute('''
            UPDATE usuarios_balance SET balance = balance - ? WHERE discord_id = ?
        ''', (cantidad, str(usuario.id)))
        await db.commit()
    
    await ctx.send(embed=embeds.crear_embed_exito(f"Retirados {cantidad} VP$ de {usuario.name}"))

@bot.command(name="estadisticas")
async def cmd_estadisticas(ctx):
    if not await check_ceo(ctx):
        return
    
    async with aiosqlite.connect('data/rifas.db') as db:
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
    
    embed = discord.Embed(title="📊 Estadísticas", color=config.COLORS['primary'])
    embed.add_field(name="🎟️ Rifas", value=str(total_rifas), inline=True)
    embed.add_field(name="🎲 Boletos", value=str(total_boletos), inline=True)
    embed.add_field(name="💰 Recaudado", value=f"${total_recaudado}", inline=True)
    embed.add_field(name="👥 Clientes", value=str(total_clientes), inline=True)
    embed.add_field(name="💵 VP$", value=f"{total_vp}", inline=True)
    
    await ctx.send(embed=embed)

@bot.command(name="auditoria")
async def cmd_auditoria(ctx):
    if not await check_ceo(ctx):
        return
    
    async with aiosqlite.connect('data/rifas.db') as db:
        db.row_factory = aiosqlite.Row
        cursor = await db.execute('''
            SELECT tipo, monto, descripcion, fecha FROM transacciones
            ORDER BY fecha DESC LIMIT 10
        ''')
        transacciones = await cursor.fetchall()
    
    embed = discord.Embed(title="📋 Auditoría", color=config.COLORS['primary'])
    for t in transacciones:
        embed.add_field(
            name=f"{t['fecha'][:10]} - {t['tipo']}",
            value=f"${t['monto']} - {t['descripcion']}",
            inline=False
        )
    
    await ctx.send(embed=embed)

@bot.command(name="exportar")
async def cmd_exportar(ctx):
    if not await check_ceo(ctx):
        return
    
    fecha = datetime.now().strftime("%Y%m%d_%H%M%S")
    archivo = f"/tmp/vp_rifas_{fecha}.csv"
    
    async with aiosqlite.connect('data/rifas.db') as db:
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
    await ctx.send(embed=embeds.crear_embed_exito("Datos exportados"))

@bot.command(name="backup")
async def cmd_backup(ctx):
    if not await check_ceo(ctx):
        return
    
    fecha = datetime.now().strftime("%Y%m%d_%H%M%S")
    backup = f"/tmp/backup_{fecha}.db"
    shutil.copy2('data/rifas.db', backup)
    
    await ctx.author.send(file=discord.File(backup))
    await ctx.send(embed=embeds.crear_embed_exito("Backup creado"))

@bot.command(name="resetallsistema")
async def cmd_reset_all_sistema(ctx):
    global reset_pending
    
    if not await check_ceo(ctx):
        return
    
    embed = discord.Embed(
        title="⚠️ Reinicio total",
        description="Esto borrará TODOS los datos\nEscribe `!confirmarreset` en 30s",
        color=config.COLORS['error']
    )
    await ctx.send(embed=embed)
    
    reset_pending = {
        'usuario_id': ctx.author.id,
        'timestamp': datetime.now().timestamp()
    }

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
    
    async with aiosqlite.connect('data/rifas.db') as db:
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
        await db.commit()
    
    reset_pending = None
    await ctx.send(embed=embeds.crear_embed_exito("Sistema reiniciado"))

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
