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
import signal
import traceback
import hashlib
import time
from datetime import datetime, timedelta
import config
from src.database.database import Database
import src.utils.embeds as embeds

# ============================================
# CONFIGURACIÓN GLOBAL
# ============================================

VERSION = "3.1.0"
PREFIX = "!"
start_time = datetime.now()
reconnect_attempts = 0
MAX_RECONNECT_ATTEMPTS = 999999  # Prácticamente infinito

# Variables para sistemas
reset_pending = None
reset_user_pending = None
sorteo_en_curso = False
sorteo_cancelado = False

# Configuración de sistemas (valores por defecto)
REFERIDOS_PORCENTAJE = 10
REFERIDOS_DESCUENTO = 10
CASHBACK_PORCENTAJE = 10

# IDs de roles de fidelización
ROLES_FIDELIZACION = {
    'BRONCE': 1483720270496661515,
    'PLATA': 1483720387178139758,
    'ORO': 1483720490601418822,
    'PLATINO': 1483720672185155625,
    'DIAMANTE': 1483720783422296165,
    'MASTER': 1483721013144584192
}

# ============================================
# CONFIGURACIÓN DE LOGGING
# ============================================

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(config.LOG_PATH),
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
        
        # Iniciar tareas de fondo
        self.keep_alive_task.start()
        self.status_task.start()
        self.supervivencia_task.start()
        logger.info("✅ Tareas automáticas iniciadas")
    
    async def init_sistemas_tablas(self):
        """Inicializar tablas para los nuevos sistemas"""
        async with aiosqlite.connect(self.db.db_path) as db:
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
            
            # Insertar config por defecto si no existe
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
            
            # Insertar configuración de niveles por defecto
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
            
            # Insertar config por defecto
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
            f"Bot iniciado correctamente\n"
            f"Versión: {VERSION}\n"
            f"Sistemas: Referidos, Fidelización, Cashback\n"
            f"Servidores: {len(self.guilds)}"
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
    
    async def on_error(self, event_method, *args, **kwargs):
        logger.error(f"Error en {event_method}: {traceback.format_exc()}")
        await self.enviar_log_sistema(
            "⚠️ **ERROR DETECTADO**", 
            f"Error en {event_method}\nVer logs para más detalles"
        )
    
    async def enviar_log_sistema(self, titulo, descripcion):
        """Envía logs del sistema al canal de actualizaciones"""
        try:
            canal = self.get_channel(self.update_channel_id)
            if not canal:
                for guild in self.guilds:
                    canal = guild.get_channel(self.update_channel_id)
                    if canal:
                        break
            
            if not canal:
                logger.warning(f"No se encontró el canal {self.update_channel_id}")
                return
            
            embed = discord.Embed(
                title=titulo,
                description=descripcion,
                color=0x0099FF,
                timestamp=datetime.now()
            )
            embed.set_footer(text=f"Sistema VP Rifas v{VERSION}")
            
            await canal.send(embed=embed)
        except Exception as e:
            logger.error(f"Error enviando log de sistema: {e}")
    
    @tasks.loop(seconds=30)
    async def keep_alive_task(self):
        """Tarea que se ejecuta cada 30 segundos para mantener el bot vivo"""
        try:
            # Cambiar actividad cada 30 segundos para evitar timeouts
            activities = [
                f"{PREFIX}ayuda | {len(self.guilds)} servers",
                f"Rifas VP v{VERSION}",
                f"{PREFIX}comprar | Activo",
                f"{len(self.users)} usuarios"
            ]
            activity = random.choice(activities)
            await self.change_presence(activity=discord.Activity(type=discord.ActivityType.watching, name=activity))
        except Exception as e:
            logger.error(f"Error en keep_alive_task: {e}")
    
    @tasks.loop(minutes=5)
    async def supervivencia_task(self):
        """Tarea de supervivencia que verifica que todo funciona"""
        try:
            # Verificar conexión
            if not self.is_ready():
                logger.warning("⚠️ Bot no está ready, intentando mantener conexión...")
                return
            
            # Heartbeat cada 30 minutos
            ahora = datetime.now()
            if (ahora - self.ultimo_heartbeat).total_seconds() > 1800:  # 30 minutos
                self.ultimo_heartbeat = ahora
                uptime = datetime.now() - start_time
                horas = uptime.total_seconds() // 3600
                minutos = (uptime.total_seconds() % 3600) // 60
                
                await self.enviar_log_sistema(
                    "💓 **HEARTBEAT**", 
                    f"Bot activo por {int(horas)}h {int(minutos)}m\n"
                    f"Versión: {VERSION}\n"
                    f"Servidores: {len(self.guilds)}"
                )
        except Exception as e:
            logger.error(f"Error en supervivencia_task: {e}")
    
    @tasks.loop(minutes=60)
    async def status_task(self):
        """Tarea que se ejecuta cada hora para reportar estado detallado"""
        try:
            uptime = datetime.now() - start_time
            horas = uptime.total_seconds() // 3600
            minutos = (uptime.total_seconds() % 3600) // 60
            
            # Obtener estadísticas
            async with aiosqlite.connect(self.db.db_path) as db:
                cursor = await db.execute('SELECT COUNT(*) FROM rifas')
                total_rifas = (await cursor.fetchone())[0]
                
                cursor = await db.execute('SELECT COUNT(*) FROM boletos')
                total_boletos = (await cursor.fetchone())[0]
                
                cursor = await db.execute('SELECT COUNT(*) FROM clientes')
                total_clientes = (await cursor.fetchone())[0]
            
            await self.enviar_log_sistema(
                "📊 **REPORTE HORARIO**", 
                f"**Uptime:** {int(horas)}h {int(minutos)}m\n"
                f"**Rifas:** {total_rifas}\n"
                f"**Boletos vendidos:** {total_boletos}\n"
                f"**Clientes:** {total_clientes}"
            )
        except Exception as e:
            logger.error(f"Error en status_task: {e}")

bot = VPRifasBot()

# ============================================
# FUNCIONES AUXILIARES
# ============================================

async def enviar_log(ctx, accion, detalles):
    """Envía un log al canal de logs"""
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
    except Exception as e:
        print(f"Error enviando log: {e}")

async def verificar_canal(ctx):
    """Verifica si el comando se ejecuta en un canal de la categoría de rifas"""
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
    """Verificar si el usuario es admin (CEO o Director)"""
    if not ctx.guild:
        return False
    member = ctx.guild.get_member(ctx.author.id)
    if not member:
        return False
    return (tiene_rol(member, config.ROLES['CEO']) or 
            tiene_rol(member, config.ROLES['DIRECTOR']))

async def check_vendedor(ctx):
    """Verificar si el usuario es vendedor (Rifas o superior)"""
    if not ctx.guild:
        return False
    member = ctx.guild.get_member(ctx.author.id)
    if not member:
        return False
    return (tiene_rol(member, config.ROLES['CEO']) or 
            tiene_rol(member, config.ROLES['DIRECTOR']) or 
            tiene_rol(member, config.ROLES['RIFAS']))

async def check_ceo(ctx):
    """Verificar si el usuario es CEO"""
    if not ctx.guild:
        return False
    member = ctx.guild.get_member(ctx.author.id)
    if not member:
        return False
    return tiene_rol(member, config.ROLES['CEO'])

# ============================================
# FUNCIONES DE SISTEMAS
# ============================================

async def generar_codigo_unico(usuario_id):
    """Genera un código único de referido basado en el ID del usuario"""
    hash_obj = hashlib.md5(usuario_id.encode())
    hash_hex = hash_obj.hexdigest()[:8].upper()
    return f"VP-{hash_hex}"

async def obtener_o_crear_codigo(usuario_id, usuario_nombre):
    """Obtiene el código de referido de un usuario o crea uno nuevo"""
    async with aiosqlite.connect(bot.db.db_path) as db:
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
    """Determina el nivel de fidelización según el gasto total"""
    async with aiosqlite.connect(bot.db.db_path) as db:
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
    """Actualiza el gasto total y el nivel de fidelización de un usuario"""
    async with aiosqlite.connect(bot.db.db_path) as db:
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
    """Aplica cashback a un usuario"""
    async with aiosqlite.connect(bot.db.db_path) as db:
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
    """Obtiene el porcentaje de descuento de un usuario según su nivel"""
    async with aiosqlite.connect(bot.db.db_path) as db:
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
    """Procesa la comisión para el referidor si existe"""
    async with aiosqlite.connect(bot.db.db_path) as db:
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
        
        await db.execute('''
            INSERT INTO referidos_comisiones (referidor_id, referido_id, monto_compra, porcentaje, comision)
            VALUES (?, ?, ?, ?, ?)
        ''', (referidor_id, comprador_id, monto_compra, porcentaje, comision))
        
        await db.commit()

# ============================================
# COMANDOS DE USUARIO BÁSICOS
# ============================================

@bot.command(name="rifa")
async def cmd_rifa(ctx):
    """Ver la rifa activa actual"""
    if not await verificar_canal(ctx):
        return
    
    rifa_activa = await bot.db.get_rifa_activa()
    
    if not rifa_activa:
        await ctx.send(embed=embeds.crear_embed_error("No hay ninguna rifa activa en este momento."))
        return
    
    embed = discord.Embed(
        title=f"🎟️ {rifa_activa['nombre']}",
        description=f"**{rifa_activa['premio']}**",
        color=config.COLORS['primary']
    )
    
    embed.add_field(
        name="🏆 Premio",
        value=f"${rifa_activa['valor_premio']:,}",
        inline=True
    )
    
    embed.add_field(
        name="💰 Precio por boleto",
        value=f"${rifa_activa['precio_boleto']:,}",
        inline=True
    )
    
    embed.set_footer(text="VP Rifas • Usa !comprar [número] para participar")
    embed.timestamp = datetime.now()
    
    await ctx.send(embed=embed)

@bot.command(name="comprar")
async def cmd_comprar(ctx, numero: int):
    """Comprar un boleto específico"""
    if not await verificar_canal(ctx):
        return
    
    rifa_activa = await bot.db.get_rifa_activa()
    if not rifa_activa:
        await ctx.send(embed=embeds.crear_embed_error("No hay rifa activa"))
        return
    
    if numero < 1 or numero > rifa_activa['total_boletos']:
        await ctx.send(embed=embeds.crear_embed_error(f"Número inválido. Debe ser entre 1 y {rifa_activa['total_boletos']}"))
        return
    
    disponibles = await bot.db.get_boletos_disponibles(rifa_activa['id'])
    if numero not in disponibles:
        await ctx.send(embed=embeds.crear_embed_error(f"❌ El número {numero} ya no está disponible."))
        return
    
    async with aiosqlite.connect(bot.db.db_path) as db:
        cursor = await db.execute('''
            SELECT balance FROM usuarios_balance WHERE discord_id = ?
        ''', (str(ctx.author.id),))
        result = await cursor.fetchone()
        balance_actual = result[0] if result else 0
    
    precio_boleto = rifa_activa['precio_boleto']
    descuento = await obtener_descuento_usuario(str(ctx.author.id))
    precio_con_descuento = int(precio_boleto * (100 - descuento) / 100)
    
    if balance_actual < precio_con_descuento:
        await ctx.send(embed=embeds.crear_embed_error(
            f"❌ No tienes suficientes VP$.\n"
            f"💰 Tu balance: **{balance_actual} VP$**\n"
            f"💵 Precio del boleto: **{precio_boleto} VP$**\n"
            f"🎁 Descuento ({descuento}%): **-{precio_boleto - precio_con_descuento} VP$**\n"
            f"💰 Total a pagar: **{precio_con_descuento} VP$**"
        ))
        return
    
    nuevo_balance = balance_actual - precio_con_descuento
    
    async with aiosqlite.connect(bot.db.db_path) as db:
        await db.execute('''
            UPDATE usuarios_balance SET balance = ? WHERE discord_id = ?
        ''', (nuevo_balance, str(ctx.author.id)))
        await db.commit()
    
    exito, mensaje = await bot.db.comprar_boleto(
        rifa_activa['id'],
        numero,
        str(ctx.author.id),
        ctx.author.name,
        None
    )
    
    if exito:
        nuevo_nivel = await actualizar_fidelizacion(str(ctx.author.id), precio_con_descuento)
        cashback = await aplicar_cashback(str(ctx.author.id), precio_con_descuento)
        await procesar_comision_referido(str(ctx.author.id), precio_con_descuento)
        
        await ctx.send(embed=embeds.crear_embed_exito(
            f"✅ ¡Boleto #{numero} comprado!\n\n"
            f"💰 Monto: ${precio_boleto:,} VP$\n"
            f"🎁 Descuento: {descuento}%\n"
            f"💵 Pagado: ${precio_con_descuento:,} VP$\n"
            f"💵 Balance anterior: {balance_actual} VP$\n"
            f"💵 Balance actual: {nuevo_balance} VP$\n"
            f"💸 Cashback acumulado: {cashback} VP$\n"
            f"🏆 Nuevo nivel: {nuevo_nivel}"
        ))
        await ctx.send(f"🎟️ **{ctx.author.name}** acaba de comprar el boleto **#{numero}**")
        await enviar_log(ctx, "🎟️ COMPRA", 
                        f"{ctx.author.name} compró boleto #{numero} por {precio_con_descuento} VP$ (desc: {descuento}%)")
    else:
        async with aiosqlite.connect(bot.db.db_path) as db:
            await db.execute('''
                UPDATE usuarios_balance SET balance = ? WHERE discord_id = ?
            ''', (balance_actual, str(ctx.author.id)))
            await db.commit()
        
        await ctx.send(embed=embeds.crear_embed_error(f"❌ Error al comprar: {mensaje}"))

@bot.command(name="comprarrandom")
async def cmd_comprar_random(ctx, cantidad: int):
    """Comprar boletos aleatorios"""
    if not await verificar_canal(ctx):
        return
    
    if cantidad < 1 or cantidad > config.MAX_BOLETOS_POR_COMPRA:
        await ctx.send(embed=embeds.crear_embed_error(f"Cantidad inválida. Máximo {config.MAX_BOLETOS_POR_COMPRA}"))
        return
    
    rifa_activa = await bot.db.get_rifa_activa()
    if not rifa_activa:
        await ctx.send(embed=embeds.crear_embed_error("No hay rifa activa"))
        return
    
    disponibles = await bot.db.get_boletos_disponibles(rifa_activa['id'])
    if len(disponibles) < cantidad:
        await ctx.send(embed=embeds.crear_embed_error(f"Solo hay {len(disponibles)} boletos disponibles"))
        return
    
    precio_boleto = rifa_activa['precio_boleto']
    precio_total = precio_boleto * cantidad
    descuento = await obtener_descuento_usuario(str(ctx.author.id))
    precio_con_descuento = int(precio_total * (100 - descuento) / 100)
    
    async with aiosqlite.connect(bot.db.db_path) as db:
        cursor = await db.execute('''
            SELECT balance FROM usuarios_balance WHERE discord_id = ?
        ''', (str(ctx.author.id),))
        result = await cursor.fetchone()
        balance_actual = result[0] if result else 0
    
    if balance_actual < precio_con_descuento:
        await ctx.send(embed=embeds.crear_embed_error(
            f"❌ No tienes suficientes VP$.\n"
            f"💰 Tu balance: **{balance_actual} VP$**\n"
            f"💵 Precio total: **{precio_total} VP$**\n"
            f"🎁 Descuento ({descuento}%): **-{precio_total - precio_con_descuento} VP$**\n"
            f"💰 Total a pagar: **{precio_con_descuento} VP$**"
        ))
        return
    
    nuevo_balance = balance_actual - precio_con_descuento
    
    async with aiosqlite.connect(bot.db.db_path) as db:
        await db.execute('''
            UPDATE usuarios_balance SET balance = ? WHERE discord_id = ?
        ''', (nuevo_balance, str(ctx.author.id)))
        await db.commit()
    
    seleccionados = random.sample(disponibles, cantidad)
    
    comprados = []
    fallidos = []
    
    for num in seleccionados:
        exito, _ = await bot.db.comprar_boleto(
            rifa_activa['id'],
            num,
            str(ctx.author.id),
            ctx.author.name,
            None
        )
        if exito:
            comprados.append(num)
        else:
            fallidos.append(num)
    
    if comprados:
        if fallidos:
            monto_devolver = len(fallidos) * (precio_con_descuento // cantidad)
            nuevo_balance += monto_devolver
            
            async with aiosqlite.connect(bot.db.db_path) as db:
                await db.execute('''
                    UPDATE usuarios_balance SET balance = ? WHERE discord_id = ?
                ''', (nuevo_balance, str(ctx.author.id)))
                await db.commit()
        
        monto_pagado = precio_con_descuento - (len(fallidos) * (precio_con_descuento // cantidad))
        nuevo_nivel = await actualizar_fidelizacion(str(ctx.author.id), monto_pagado)
        cashback = await aplicar_cashback(str(ctx.author.id), monto_pagado)
        await procesar_comision_referido(str(ctx.author.id), monto_pagado)
        
        await ctx.send(embed=embeds.crear_embed_exito(
            f"✅ Boletos comprados: {', '.join(map(str, comprados))}\n"
            f"💰 Total sin descuento: ${precio_total:,} VP$\n"
            f"🎁 Descuento: {descuento}%\n"
            f"💵 Pagado: ${monto_pagado:,} VP$\n"
            f"💵 Balance anterior: {balance_actual} VP$\n"
            f"💵 Balance actual: {nuevo_balance} VP$\n"
            f"💸 Cashback acumulado: {cashback} VP$\n"
            f"🏆 Nuevo nivel: {nuevo_nivel}"
        ))
        
        await ctx.send(f"🎟️ **{ctx.author.name}** acaba de comprar {len(comprados)} boletos: {', '.join(map(str, comprados))}")
        await enviar_log(ctx, "🎟️ COMPRA MÚLTIPLE", 
                        f"{ctx.author.name} compró {len(comprados)} boletos por {monto_pagado:,} VP$ (desc: {descuento}%)")
    else:
        async with aiosqlite.connect(bot.db.db_path) as db:
            await db.execute('''
                UPDATE usuarios_balance SET balance = ? WHERE discord_id = ?
            ''', (balance_actual, str(ctx.author.id)))
            await db.commit()
        
        await ctx.send(embed=embeds.crear_embed_error("No se pudo comprar ningún boleto"))

@bot.command(name="misboletos")
async def cmd_mis_boletos(ctx):
    """Ver tus boletos en la rifa actual"""
    if not await verificar_canal(ctx):
        return
    
    rifa_activa = await bot.db.get_rifa_activa()
    if not rifa_activa:
        await ctx.send(embed=embeds.crear_embed_error("No hay rifa activa"))
        return
    
    async with aiosqlite.connect(bot.db.db_path) as db:
        db.row_factory = aiosqlite.Row
        cursor = await db.execute('''
            SELECT numero, fecha_compra, estado 
            FROM boletos 
            WHERE rifa_id = ? AND comprador_id = ?
        ''', (rifa_activa['id'], str(ctx.author.id)))
        boletos = await cursor.fetchall()
    
    if not boletos:
        await ctx.send(embed=embeds.crear_embed_error("No tienes boletos en esta rifa"))
        return
    
    embed = discord.Embed(
        title=f"🎟️ Tus boletos - Rifa #{rifa_activa['id']}",
        color=config.COLORS['primary']
    )
    
    lista = "\n".join([f"• #{b['numero']} - {b['fecha_compra'][:10]}" for b in boletos])
    embed.description = lista
    embed.set_footer(text=f"Total: {len(boletos)} boletos")
    
    await ctx.send(embed=embed)

@bot.command(name="disponibles")
async def cmd_boletos_disponibles(ctx):
    """Ver estadísticas de disponibilidad"""
    if not await verificar_canal(ctx):
        return
    
    rifa_activa = await bot.db.get_rifa_activa()
    if not rifa_activa:
        await ctx.send(embed=embeds.crear_embed_error("No hay rifa activa"))
        return
    
    disponibles = await bot.db.get_boletos_disponibles(rifa_activa['id'])
    vendidos = await bot.db.get_boletos_vendidos(rifa_activa['id'])
    total = rifa_activa['total_boletos']
    
    porcentaje_vendidos = int((vendidos / total) * 100) if total > 0 else 0
    porcentaje_disponibles = 100 - porcentaje_vendidos
    
    barras = 20
    barras_llenas = int((vendidos / total) * barras) if total > 0 else 0
    barra = "🟩" * barras_llenas + "⬜" * (barras - barras_llenas)
    
    embed = discord.Embed(
        title="🎟️ **ESTADO DE LA RIFA**",
        color=config.COLORS['info']
    )
    
    embed.add_field(name="📊 Progreso", value=barra, inline=False)
    embed.add_field(name="✅ Disponibles", value=f"**{len(disponibles)}** números", inline=True)
    embed.add_field(name="📈 Vendidos", value=f"**{vendidos}** números", inline=True)
    embed.add_field(name="🎯 Total", value=f"**{total}** números", inline=True)
    embed.add_field(name="📊 Porcentaje", value=f"Vendidos: **{porcentaje_vendidos}%**\nDisponibles: **{porcentaje_disponibles}%**", inline=False)
    embed.set_footer(text="Usa !comprar [número] para participar")
    
    await ctx.send(embed=embed)

@bot.command(name="ranking")
async def cmd_ranking(ctx):
    """Ver top compradores"""
    if not await verificar_canal(ctx):
        return
    
    async with aiosqlite.connect(bot.db.db_path) as db:
        db.row_factory = aiosqlite.Row
        cursor = await db.execute('''
            SELECT nombre, total_compras, total_gastado 
            FROM clientes 
            ORDER BY total_gastado DESC 
            LIMIT 10
        ''')
        usuarios = await cursor.fetchall()
    
    embed = embeds.crear_embed_ranking(usuarios)
    await ctx.send(embed=embed)

@bot.command(name="historial")
async def cmd_historial(ctx):
    """Ver tu historial completo de compras"""
    if not await verificar_canal(ctx):
        return
    
    async with aiosqlite.connect(bot.db.db_path) as db:
        db.row_factory = aiosqlite.Row
        cursor = await db.execute('''
            SELECT b.numero, r.nombre as rifa_nombre, b.fecha_compra, b.precio_pagado
            FROM boletos b
            JOIN rifas r ON b.rifa_id = r.id
            WHERE b.comprador_id = ?
            ORDER BY b.fecha_compra DESC
            LIMIT 20
        ''', (str(ctx.author.id),))
        boletos = await cursor.fetchall()
    
    if not boletos:
        await ctx.send(embed=embeds.crear_embed_error("No tienes historial de compras"))
        return
    
    embed = discord.Embed(title="📜 Tu historial de compras", color=config.COLORS['primary'])
    for b in boletos[:10]:
        embed.add_field(
            name=f"{b['rifa_nombre']} - #{b['numero']}",
            value=f"${b['precio_pagado']:,} - {b['fecha_compra'][:10]}",
            inline=False
        )
    
    if len(boletos) > 10:
        embed.set_footer(text=f"Mostrando 10 de {len(boletos)} compras")
    
    await ctx.send(embed=embed)

@bot.command(name="balance")
async def cmd_balance(ctx):
    """Ver tu balance de VP$"""
    if not await verificar_canal(ctx):
        return
    
    async with aiosqlite.connect(bot.db.db_path) as db:
        cursor = await db.execute('''
            SELECT balance FROM usuarios_balance WHERE discord_id = ?
        ''', (str(ctx.author.id),))
        result = await cursor.fetchone()
    
    balance = result[0] if result else 0
    
    embed = discord.Embed(
        title="💰 Tu balance de VP$",
        description=f"Tienes **{balance} VP$** disponibles",
        color=config.COLORS['primary']
    )
    
    await ctx.send(embed=embed)

@bot.command(name="topvp")
async def cmd_top_vp(ctx):
    """Ver ranking de VP$"""
    if not await verificar_canal(ctx):
        return
    
    async with aiosqlite.connect(bot.db.db_path) as db:
        db.row_factory = aiosqlite.Row
        cursor = await db.execute('''
            SELECT nombre, balance FROM usuarios_balance 
            WHERE balance > 0 
            ORDER BY balance DESC 
            LIMIT 10
        ''')
        usuarios = await cursor.fetchall()
    
    if not usuarios:
        await ctx.send(embed=embeds.crear_embed_info("ℹ️ Información", "No hay usuarios con VP$ aún"))
        return
    
    embed = discord.Embed(
        title="🏆 TOP 10 - VP$",
        color=config.COLORS['primary']
    )
    
    for i, u in enumerate(usuarios, 1):
        medalla = {1: "🥇", 2: "🥈", 3: "🥉"}.get(i, f"{i}.")
        embed.add_field(
            name=f"{medalla} {u['nombre']}",
            value=f"**{u['balance']} VP$**",
            inline=False
        )
    
    await ctx.send(embed=embed)

# ============================================
# SISTEMA DE REFERIDOS
# ============================================

@bot.command(name="codigo")
async def cmd_codigo(ctx):
    """Muestra tu código de referido único"""
    if not await verificar_canal(ctx):
        return
    
    codigo = await obtener_o_crear_codigo(str(ctx.author.id), ctx.author.name)
    
    embed = discord.Embed(
        title="🔗 **TU CÓDIGO DE REFERIDO**",
        description=f"`{codigo}`",
        color=config.COLORS['primary']
    )
    
    async with aiosqlite.connect(bot.db.db_path) as db:
        cursor = await db.execute('''
            SELECT COUNT(*), SUM(comisiones_generadas) 
            FROM referidos_relaciones 
            WHERE referidor_id = ?
        ''', (str(ctx.author.id),))
        stats = await cursor.fetchone()
        total_referidos = stats[0] if stats else 0
        total_comisiones = stats[1] if stats and stats[1] else 0
    
    embed.add_field(name="📊 Tus referidos", value=f"**{total_referidos}** personas", inline=True)
    embed.add_field(name="💰 Comisiones ganadas", value=f"**{total_comisiones} VP$**", inline=True)
    embed.add_field(
        name="🎁 Beneficios", 
        value=f"• Tú ganas: **{REFERIDOS_PORCENTAJE}%** de todas sus compras\n"
              f"• Ellos ganan: **{REFERIDOS_DESCUENTO}%** en primera compra",
        inline=False
    )
    
    embed.set_footer(text="Comparte este código con tus amigos")
    
    await ctx.send(embed=embed)

@bot.command(name="usar")
async def cmd_usar_codigo(ctx, codigo: str):
    """Usa un código de referido (antes de tu primera compra)"""
    if not await verificar_canal(ctx):
        return
    
    async with aiosqlite.connect(bot.db.db_path) as db:
        cursor = await db.execute('''
            SELECT usuario_id FROM referidos_codigos WHERE codigo = ? AND usuario_id != ?
        ''', (codigo.upper(), str(ctx.author.id)))
        referidor = await cursor.fetchone()
        
        if not referidor:
            await ctx.send(embed=embeds.crear_embed_error("❌ Código inválido o no puedes usar tu propio código"))
            return
        
        referidor_id = referidor[0]
        
        cursor = await db.execute('''
            SELECT * FROM referidos_relaciones WHERE referido_id = ?
        ''', (str(ctx.author.id),))
        existe = await cursor.fetchone()
        
        if existe:
            await ctx.send(embed=embeds.crear_embed_error("❌ Ya tienes un referidor registrado"))
            return
        
        await db.execute('''
            INSERT INTO referidos_relaciones (referido_id, referidor_id)
            VALUES (?, ?)
        ''', (str(ctx.author.id), referidor_id))
        
        await db.commit()
    
    embed = discord.Embed(
        title="✅ **CÓDIGO APLICADO**",
        description=f"Has usado el código de <@{referidor_id}>",
        color=config.COLORS['success']
    )
    embed.add_field(
        name="🎁 Beneficios", 
        value=f"• Tendrás **{REFERIDOS_DESCUENTO}% de descuento** en tu primera compra\n"
              f"• Tu referidor ganará **{REFERIDOS_PORCENTAJE}%** de tus compras futuras",
        inline=False
    )
    
    await ctx.send(embed=embed)

@bot.command(name="misreferidos")
async def cmd_mis_referidos(ctx):
    """Muestra la lista de personas que has referido"""
    if not await verificar_canal(ctx):
        return
    
    async with aiosqlite.connect(bot.db.db_path) as db:
        db.row_factory = aiosqlite.Row
        cursor = await db.execute('''
            SELECT r.referido_id, c.nombre as nombre, 
                   r.total_compras, r.total_gastado, r.comisiones_generadas,
                   r.primera_compra
            FROM referidos_relaciones r
            LEFT JOIN clientes c ON r.referido_id = c.discord_id
            WHERE r.referidor_id = ?
            ORDER BY r.fecha_registro DESC
            LIMIT 20
        ''', (str(ctx.author.id),))
        referidos = await cursor.fetchall()
        
        cursor = await db.execute('''
            SELECT SUM(comisiones_generadas) as total 
            FROM referidos_relaciones WHERE referidor_id = ?
        ''', (str(ctx.author.id),))
        total_comisiones = await cursor.fetchone()
    
    if not referidos:
        await ctx.send(embed=embeds.crear_embed_error("No has referido a nadie todavía"))
        return
    
    embed = discord.Embed(
        title="👥 **TUS REFERIDOS**",
        description=f"Total comisiones: **{total_comisiones['total'] or 0} VP$**",
        color=config.COLORS['primary']
    )
    
    for ref in referidos[:10]:
        nombre = ref['nombre'] or f"Usuario {ref['referido_id'][:5]}..."
        estado = "✅" if ref['primera_compra'] else "⏳"
        embed.add_field(
            name=f"{estado} {nombre}",
            value=f"Compras: {ref['total_compras']} | Gastado: ${ref['total_gastado']:,} | Comisiones: ${ref['comisiones_generadas']:,}",
            inline=False
        )
    
    if len(referidos) > 10:
        embed.set_footer(text=f"Mostrando 10 de {len(referidos)} referidos")
    
    await ctx.send(embed=embed)

# ============================================
# COMANDOS CEO PARA REFERIDOS
# ============================================

@bot.command(name="setrefcomision")
async def cmd_set_ref_comision(ctx, porcentaje: int):
    """[CEO] Establece el % de comisión para referidores"""
    if not await check_ceo(ctx):
        await ctx.send("❌ Solo el CEO puede usar este comando")
        return
    
    if porcentaje < 0 or porcentaje > 50:
        await ctx.send(embed=embeds.crear_embed_error("El porcentaje debe estar entre 0 y 50"))
        return
    
    global REFERIDOS_PORCENTAJE
    REFERIDOS_PORCENTAJE = porcentaje
    
    async with aiosqlite.connect(bot.db.db_path) as db:
        await db.execute('''
            UPDATE referidos_config SET porcentaje_comision = ? WHERE id = 1
        ''', (porcentaje,))
        await db.commit()
    
    embed = embeds.crear_embed_exito(f"✅ Porcentaje de comisión actualizado a **{porcentaje}%**")
    await ctx.send(embed=embed)
    await bot.enviar_log_sistema("⚙️ **CONFIG REFERIDOS**", f"Comisión cambiada a {porcentaje}% por {ctx.author.name}")

@bot.command(name="setrefdescuento")
async def cmd_set_ref_descuento(ctx, porcentaje: int):
    """[CEO] Establece el % de descuento para nuevos referidos"""
    if not await check_ceo(ctx):
        await ctx.send("❌ Solo el CEO puede usar este comando")
        return
    
    if porcentaje < 0 or porcentaje > 50:
        await ctx.send(embed=embeds.crear_embed_error("El porcentaje debe estar entre 0 y 50"))
        return
    
    global REFERIDOS_DESCUENTO
    REFERIDOS_DESCUENTO = porcentaje
    
    async with aiosqlite.connect(bot.db.db_path) as db:
        await db.execute('''
            UPDATE referidos_config SET porcentaje_descuento = ? WHERE id = 1
        ''', (porcentaje,))
        await db.commit()
    
    embed = embeds.crear_embed_exito(f"✅ Porcentaje de descuento actualizado a **{porcentaje}%**")
    await ctx.send(embed=embed)
    await bot.enviar_log_sistema("⚙️ **CONFIG REFERIDOS**", f"Descuento cambiado a {porcentaje}% por {ctx.author.name}")

@bot.command(name="refconfig")
async def cmd_ref_config(ctx):
    """[CEO] Muestra la configuración actual de referidos"""
    if not await check_ceo(ctx):
        await ctx.send("❌ Solo el CEO puede usar este comando")
        return
    
    embed = discord.Embed(
        title="⚙️ **CONFIGURACIÓN DE REFERIDOS**",
        color=config.COLORS['primary']
    )
    embed.add_field(name="💰 Comisión referidor", value=f"**{REFERIDOS_PORCENTAJE}%**", inline=True)
    embed.add_field(name="🎁 Descuento referido", value=f"**{REFERIDOS_DESCUENTO}%**", inline=True)
    
    await ctx.send(embed=embed)

# ============================================
# SISTEMA DE FIDELIZACIÓN
# ============================================

@bot.command(name="nivel")
async def cmd_nivel(ctx):
    """Muestra tu nivel de fidelización y beneficios"""
    if not await verificar_canal(ctx):
        return
    
    async with aiosqlite.connect(bot.db.db_path) as db:
        db.row_factory = aiosqlite.Row
        cursor = await db.execute('''
            SELECT f.gasto_total, f.nivel, fc.* 
            FROM fidelizacion f
            JOIN fidelizacion_config fc ON f.nivel = fc.nivel
            WHERE f.usuario_id = ?
        ''', (str(ctx.author.id),))
        data = await cursor.fetchone()
    
    if not data:
        await ctx.send(embed=embeds.crear_embed_error("No tienes historial de compras todavía"))
        return
    
    async with aiosqlite.connect(bot.db.db_path) as db:
        cursor = await db.execute('''
            SELECT nivel, gasto_minimo FROM fidelizacion_config
            WHERE gasto_minimo > ? 
            ORDER BY gasto_minimo ASC LIMIT 1
        ''', (data['gasto_total'],))
        siguiente = await cursor.fetchone()
    
    embed = discord.Embed(
        title=f"🏆 **TU NIVEL: {data['nivel']}**",
        description=f"Gasto total: **${data['gasto_total']:,} VP$**",
        color=config.COLORS['primary']
    )
    
    beneficios = []
    if data['descuento'] > 0:
        beneficios.append(f"💰 **{data['descuento']}%** de descuento en compras")
    if data['boletos_gratis_por_cada'] > 0:
        beneficios.append(f"🎟️ **{data['cantidad_boletos_gratis']}** boletos gratis cada {data['boletos_gratis_por_cada']} comprados")
    if data['acceso_anticipado_horas'] > 0:
        beneficios.append(f"⏰ Acceso anticipado **{data['acceso_anticipado_horas']}h** antes")
    if data['canal_vip']:
        beneficios.append(f"👑 Acceso a canales VIP")
    if data['rifas_exclusivas']:
        beneficios.append(f"✨ Participación en rifas exclusivas")
    
    if beneficios:
        embed.add_field(name="✅ Tus beneficios", value="\n".join(beneficios), inline=False)
    else:
        embed.add_field(name="✅ Tus beneficios", value="Sin beneficios aún. ¡Sigue comprando!", inline=False)
    
    if siguiente:
        falta = siguiente[1] - data['gasto_total']
        embed.add_field(
            name=f"🎯 Próximo nivel: {siguiente[0]}",
            value=f"Te faltan **${falta:,} VP$** para subir de nivel",
            inline=False
        )
    
    await ctx.send(embed=embed)

@bot.command(name="topgastadores")
async def cmd_top_gastadores(ctx):
    """Muestra el ranking de mayores gastadores"""
    if not await verificar_canal(ctx):
        return
    
    async with aiosqlite.connect(bot.db.db_path) as db:
        db.row_factory = aiosqlite.Row
        cursor = await db.execute('''
            SELECT f.usuario_id, c.nombre, f.gasto_total, f.nivel
            FROM fidelizacion f
            LEFT JOIN clientes c ON f.usuario_id = c.discord_id
            WHERE f.gasto_total > 0
            ORDER BY f.gasto_total DESC
            LIMIT 10
        ''')
        top = await cursor.fetchall()
    
    if not top:
        await ctx.send(embed=embeds.crear_embed_error("No hay datos todavía"))
        return
    
    embed = discord.Embed(
        title="🏆 **TOP GASTADORES**",
        color=config.COLORS['primary']
    )
    
    for i, usuario in enumerate(top, 1):
        nombre = usuario['nombre'] or f"Usuario {usuario['usuario_id'][:5]}..."
        medalla = {1: "🥇", 2: "🥈", 3: "🥉"}.get(i, f"{i}.")
        embed.add_field(
            name=f"{medalla} {nombre}",
            value=f"Gastado: **${usuario['gasto_total']:,} VP$** | Nivel: {usuario['nivel']}",
            inline=False
        )
    
    await ctx.send(embed=embed)

# ============================================
# COMANDOS CEO PARA FIDELIZACIÓN
# ============================================

@bot.command(name="setnivel")
async def cmd_set_nivel(ctx, nivel: str, campo: str, valor: str):
    """[CEO] Configura los beneficios de un nivel"""
    if not await check_ceo(ctx):
        await ctx.send("❌ Solo el CEO puede usar este comando")
        return
    
    nivel = nivel.upper()
    niveles_validos = ['BRONCE', 'PLATA', 'ORO', 'PLATINO', 'DIAMANTE', 'MASTER']
    
    if nivel not in niveles_validos:
        await ctx.send(embed=embeds.crear_embed_error(f"Nivel inválido. Usa: {', '.join(niveles_validos)}"))
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
        await ctx.send(embed=embeds.crear_embed_error(f"Campo inválido. Usa: {', '.join(campos_validos.keys())}"))
        return
    
    columna = campos_validos[campo]
    
    async with aiosqlite.connect(bot.db.db_path) as db:
        await db.execute(f'''
            UPDATE fidelizacion_config SET {columna} = ? WHERE nivel = ?
        ''', (valor, nivel))
        await db.commit()
    
    embed = embeds.crear_embed_exito(f"✅ Nivel **{nivel}**: {campo} actualizado a {valor}")
    await ctx.send(embed=embed)
    await bot.enviar_log_sistema("⚙️ **FIDELIZACIÓN**", f"{ctx.author.name} actualizó {nivel}.{campo} = {valor}")

@bot.command(name="verniveles")
async def cmd_ver_niveles(ctx):
    """[CEO] Muestra la configuración de todos los niveles"""
    if not await check_ceo(ctx):
        await ctx.send("❌ Solo el CEO puede usar este comando")
        return
    
    async with aiosqlite.connect(bot.db.db_path) as db:
        db.row_factory = aiosqlite.Row
        cursor = await db.execute('''
            SELECT * FROM fidelizacion_config ORDER BY gasto_minimo ASC
        ''')
        niveles = await cursor.fetchall()
    
    embed = discord.Embed(
        title="⚙️ **CONFIGURACIÓN DE NIVELES**",
        color=config.COLORS['primary']
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
        
        texto = f"Gasto: ${n['gasto_minimo']:,} - ${n['gasto_maximo'] or '∞':,}\n"
        texto += f"Beneficios: {' | '.join(beneficios) if beneficios else 'Ninguno'}"
        
        embed.add_field(name=f"**{n['nivel']}**", value=texto, inline=False)
    
    await ctx.send(embed=embed)

# ============================================
# SISTEMA DE CASHBACK
# ============================================

@bot.command(name="cashback")
async def cmd_cashback(ctx):
    """Muestra tu cashback acumulado"""
    if not await verificar_canal(ctx):
        return
    
    async with aiosqlite.connect(bot.db.db_path) as db:
        cursor = await db.execute('''
            SELECT cashback_acumulado FROM cashback WHERE usuario_id = ?
        ''', (str(ctx.author.id),))
        result = await cursor.fetchone()
        
        cursor = await db.execute('''
            SELECT porcentaje FROM cashback_config WHERE id = 1
        ''')
        config_cb = await cursor.fetchone()
        porcentaje = config_cb[0] if config_cb else 10
    
    cashback = result[0] if result else 0
    
    embed = discord.Embed(
        title="💰 **TU CASHBACK**",
        description=f"Acumulado: **${cashback:,} VP$**",
        color=config.COLORS['primary']
    )
    embed.add_field(name="📊 Porcentaje", value=f"**{porcentaje}%** de cada compra", inline=True)
    embed.add_field(name="📅 Día de pago", value="**LUNES**", inline=True)
    
    await ctx.send(embed=embed)

@bot.command(name="topcashback")
async def cmd_top_cashback(ctx):
    """Muestra el ranking de cashback acumulado"""
    if not await verificar_canal(ctx):
        return
    
    async with aiosqlite.connect(bot.db.db_path) as db:
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
        await ctx.send(embed=embeds.crear_embed_error("No hay cashback acumulado todavía"))
        return
    
    embed = discord.Embed(
        title="💰 **TOP CASHBACK**",
        color=config.COLORS['primary']
    )
    
    for i, usuario in enumerate(top, 1):
        nombre = usuario['nombre'] or f"Usuario {usuario['usuario_id'][:5]}..."
        medalla = {1: "🥇", 2: "🥈", 3: "🥉"}.get(i, f"{i}.")
        embed.add_field(
            name=f"{medalla} {nombre}",
            value=f"**${usuario['cashback_acumulado']:,} VP$**",
            inline=False
        )
    
    await ctx.send(embed=embed)

# ============================================
# COMANDOS CEO PARA CASHBACK
# ============================================

@bot.command(name="setcashback")
async def cmd_set_cashback(ctx, porcentaje: int):
    """[CEO] Establece el % de cashback para todas las compras"""
    if not await check_ceo(ctx):
        await ctx.send("❌ Solo el CEO puede usar este comando")
        return
    
    if porcentaje < 0 or porcentaje > 50:
        await ctx.send(embed=embeds.crear_embed_error("El porcentaje debe estar entre 0 y 50"))
        return
    
    global CASHBACK_PORCENTAJE
    CASHBACK_PORCENTAJE = porcentaje
    
    async with aiosqlite.connect(bot.db.db_path) as db:
        await db.execute('''
            UPDATE cashback_config SET porcentaje = ? WHERE id = 1
        ''', (porcentaje,))
        await db.commit()
    
    embed = embeds.crear_embed_exito(f"✅ Porcentaje de cashback actualizado a **{porcentaje}%**")
    await ctx.send(embed=embed)
    await bot.enviar_log_sistema("⚙️ **CASHBACK**", f"Porcentaje cambiado a {porcentaje}% por {ctx.author.name}")

@bot.command(name="pagarcashback")
async def cmd_pagar_cashback(ctx):
    """[CEO] Paga el cashback acumulado a todos los usuarios"""
    if not await check_ceo(ctx):
        await ctx.send("❌ Solo el CEO puede usar este comando")
        return
    
    await ctx.send("💰 **PROCESANDO PAGOS DE CASHBACK...**")
    
    async with aiosqlite.connect(bot.db.db_path) as db:
        db.row_factory = aiosqlite.Row
        cursor = await db.execute('''
            SELECT usuario_id, cashback_acumulado FROM cashback WHERE cashback_acumulado > 0
        ''')
        usuarios = await cursor.fetchall()
        
        if not usuarios:
            await ctx.send(embed=embeds.crear_embed_error("No hay cashback para pagar"))
            return
        
        total_pagado = 0
        pagados = 0
        
        for usuario in usuarios:
            monto = usuario['cashback_acumulado']
            
            await db.execute('''
                INSERT INTO usuarios_balance (discord_id, nombre, balance)
                VALUES (?, (SELECT nombre FROM clientes WHERE discord_id = ?), ?)
                ON CONFLICT(discord_id) DO UPDATE SET
                    balance = balance + ?,
                    ultima_actualizacion = CURRENT_TIMESTAMP
            ''', (usuario['usuario_id'], usuario['usuario_id'], monto, monto))
            
            await db.execute('''
                UPDATE cashback SET 
                    cashback_recibido = cashback_recibido + ?,
                    cashback_acumulado = 0,
                    ultima_actualizacion = CURRENT_TIMESTAMP
                WHERE usuario_id = ?
            ''', (monto, usuario['usuario_id']))
            
            total_pagado += monto
            pagados += 1
        
        await db.commit()
    
    embed = discord.Embed(
        title="✅ **CASHBACK PAGADO**",
        description=f"Se pagaron **${total_pagado:,} VP$** a **{pagados} usuarios**",
        color=config.COLORS['success']
    )
    await ctx.send(embed=embed)
    await bot.enviar_log_sistema("💰 **PAGO CASHBACK**", f"{ctx.author.name} pagó ${total_pagado:,} VP$ a {pagados} usuarios")

@bot.command(name="resetcashback")
async def cmd_reset_cashback(ctx):
    """[CEO] Resetea el cashback acumulado de todos los usuarios"""
    if not await check_ceo(ctx):
        await ctx.send("❌ Solo el CEO puede usar este comando")
        return
    
    embed_confirm = discord.Embed(
        title="⚠️ **CONFIRMAR RESET DE CASHBACK**",
        description="Estás a punto de **BORRAR** todo el cashback acumulado.\n"
                    "Esta acción NO SE PUEDE DESHACER.\n\n"
                    "Escribe `!confirmarresetcashback` en 30 segundos",
        color=config.COLORS['error']
    )
    await ctx.send(embed=embed_confirm)
    
    global reset_pending
    reset_pending = {
        'tipo': 'cashback',
        'usuario_id': ctx.author.id,
        'canal_id': ctx.channel.id,
        'timestamp': datetime.now().timestamp()
    }

@bot.command(name="confirmarresetcashback")
async def cmd_confirmar_reset_cashback(ctx):
    """Confirma el reset de cashback"""
    global reset_pending
    
    if not await check_ceo(ctx):
        await ctx.send("❌ Solo el CEO puede usar este comando")
        return
    
    if not reset_pending or reset_pending.get('tipo') != 'cashback' or reset_pending.get('usuario_id') != ctx.author.id:
        await ctx.send(embed=embeds.crear_embed_error("❌ No hay solicitud de reset pendiente"))
        return
    
    tiempo_actual = datetime.now().timestamp()
    if tiempo_actual - reset_pending['timestamp'] > 30:
        reset_pending = None
        await ctx.send(embed=embeds.crear_embed_error("⏰ Tiempo de confirmación expirado"))
        return
    
    async with aiosqlite.connect(bot.db.db_path) as db:
        await db.execute('UPDATE cashback SET cashback_acumulado = 0')
        await db.commit()
    
    reset_pending = None
    
    embed = embeds.crear_embed_exito("✅ Cashback resetado exitosamente")
    await ctx.send(embed=embed)
    await bot.enviar_log_sistema("🔄 **RESET CASHBACK**", f"{ctx.author.name} resetó el cashback")

# ============================================
# COMANDO VENDERRANDOM
# ============================================

@bot.command(name="venderrandom")
async def cmd_vender_random(ctx, usuario: discord.Member, cantidad: int):
    """[VENDEDOR] Vende boletos aleatorios a un usuario"""
    if not await verificar_canal(ctx):
        return
    if not await check_vendedor(ctx):
        await ctx.send("❌ No tienes permiso para usar este comando")
        return
    
    if cantidad < 1 or cantidad > config.MAX_BOLETOS_POR_COMPRA:
        await ctx.send(embed=embeds.crear_embed_error(f"Cantidad inválida. Máximo {config.MAX_BOLETOS_POR_COMPRA}"))
        return
    
    rifa_activa = await bot.db.get_rifa_activa()
    if not rifa_activa:
        await ctx.send(embed=embeds.crear_embed_error("No hay rifa activa"))
        return
    
    disponibles = await bot.db.get_boletos_disponibles(rifa_activa['id'])
    if len(disponibles) < cantidad:
        await ctx.send(embed=embeds.crear_embed_error(f"Solo hay {len(disponibles)} boletos disponibles"))
        return
    
    precio_boleto = rifa_activa['precio_boleto']
    precio_total = precio_boleto * cantidad
    
    async with aiosqlite.connect(bot.db.db_path) as db:
        cursor = await db.execute('''
            SELECT balance FROM usuarios_balance WHERE discord_id = ?
        ''', (str(usuario.id),))
        result = await cursor.fetchone()
        balance_actual = result[0] if result else 0
    
    descuento = await obtener_descuento_usuario(str(usuario.id))
    precio_con_descuento = int(precio_total * (100 - descuento) / 100)
    
    if balance_actual < precio_con_descuento:
        await ctx.send(embed=embeds.crear_embed_error(
            f"❌ {usuario.name} no tiene suficientes VP$.\n"
            f"💰 Su balance: **{balance_actual} VP$**\n"
            f"💵 Precio total ({cantidad} boletos): **{precio_total} VP$**\n"
            f"🎁 Descuento ({descuento}%): **-{precio_total - precio_con_descuento} VP$**\n"
            f"💰 Total a pagar: **{precio_con_descuento} VP$**"
        ))
        return
    
    nuevo_balance = balance_actual - precio_con_descuento
    
    seleccionados = random.sample(disponibles, cantidad)
    
    comprados = []
    
    async with aiosqlite.connect(bot.db.db_path) as db:
        await db.execute('''
            UPDATE usuarios_balance SET balance = ? WHERE discord_id = ?
        ''', (nuevo_balance, str(usuario.id)))
        
        for num in seleccionados:
            exito, _ = await bot.db.comprar_boleto(
                rifa_activa['id'],
                num,
                str(usuario.id),
                usuario.name,
                str(ctx.author.id)
            )
            if exito:
                comprados.append(num)
        
        await db.commit()
    
    if comprados:
        monto_pagado = precio_con_descuento
        await actualizar_fidelizacion(str(usuario.id), monto_pagado)
        await aplicar_cashback(str(usuario.id), monto_pagado)
        await procesar_comision_referido(str(usuario.id), monto_pagado)
        
        await ctx.send(embed=embeds.crear_embed_exito(
            f"✅ Venta realizada a {usuario.name}\n"
            f"🎟️ Boletos: {', '.join(map(str, comprados))}\n"
            f"💰 Total: ${monto_pagado:,} VP$\n"
            f"🎁 Descuento aplicado: {descuento}%"
        ))
        await ctx.send(f"💰 **{ctx.author.name}** vendió {len(comprados)} boletos a **{usuario.name}**: {', '.join(map(str, comprados))}")
        await enviar_log(ctx, "💰 VENTA MÚLTIPLE", 
                        f"{ctx.author.name} vendió {len(comprados)} boletos a {usuario.name} por {monto_pagado} VP$")
    else:
        async with aiosqlite.connect(bot.db.db_path) as db:
            await db.execute('''
                UPDATE usuarios_balance SET balance = ? WHERE discord_id = ?
            ''', (balance_actual, str(usuario.id)))
            await db.commit()
        
        await ctx.send(embed=embeds.crear_embed_error("No se pudo realizar la venta"))

# ============================================
# COMANDO DE AYUDA
# ============================================

@bot.command(name="ayuda")
async def cmd_ayuda(ctx):
    """Ver todos los comandos disponibles"""
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
    
    # Comandos básicos
    basicos = """
    `!rifa` - Ver rifa activa
    `!comprar [número]` - Comprar boleto
    `!comprarrandom [cantidad]` - Comprar aleatorios
    `!misboletos` - Ver tus boletos
    `!disponibles` - Estadísticas de disponibilidad
    `!balance` - Ver tu balance VP$
    `!ranking` - Top compradores
    `!historial` - Tu historial
    `!topvp` - Ranking VP$
    """
    embed.add_field(name="👤 **BÁSICOS**", value=basicos, inline=False)
    
    # Sistema de referidos
    referidos = """
    `!codigo` - Tu código de referido
    `!usar [código]` - Usar código de referido
    `!misreferidos` - Ver tus referidos
    """
    embed.add_field(name="🤝 **REFERIDOS**", value=referidos, inline=False)
    
    # Fidelización y cashback
    fidelizacion = """
    `!nivel` - Tu nivel y beneficios
    `!topgastadores` - Ranking de gasto
    `!cashback` - Tu cashback acumulado
    `!topcashback` - Ranking cashback
    """
    embed.add_field(name="🏆 **FIDELIZACIÓN**", value=fidelizacion, inline=False)
    
    if es_vendedor or es_director or es_ceo:
        vendedor = """
        `!vender [@usuario] [número]` - Vender boleto
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
        `!reporte` - Ver reporte
        `!alertar [mensaje]` - Alerta a todos
        """
        embed.add_field(name="🎯 **DIRECTORES**", value=director, inline=False)
    
    if es_ceo:
        ceo = """
        `!acreditarvp [@usuario] [cantidad]` - Acreditar VP$
        `!retirarvp [@usuario] [cantidad]` - Retirar VP$
        `!setrefcomision [%]` - Configurar comisión referidos
        `!setrefdescuento [%]` - Configurar descuento referidos
        `!setcashback [%]` - Configurar % cashback
        `!pagarcashback` - Pagar cashback
        `!setnivel [nivel] [campo] [valor]` - Configurar niveles
        `!verniveles` - Ver configuración niveles
        `!resetallsistema` - Reiniciar sistema
        `!auditoria` - Ver transacciones
        `!exportar` - Exportar a CSV
        `!backup` - Crear backup
        `!estadisticas` - Estadísticas globales
        """
        embed.add_field(name="👑 **CEO**", value=ceo, inline=False)
    
    embed.set_footer(text="Ejemplo: !comprar 25")
    await ctx.send(embed=embed)

# ============================================
# EJECUCIÓN DEL BOT (ANTI-CAÍDA EXTREMO)
# ============================================

def run_bot_with_reconnect():
    """Ejecuta el bot con reconexión automática INFALIBLE"""
    global reconnect_attempts
    
    print("\n" + "="*50)
    print(f"🚀 INICIANDO BOT VP RIFAS v{VERSION}")
    print("="*50 + "\n")
    
    while True:  # Bucle INFINITO de reconexión
        try:
            reconnect_attempts += 1
            print(f"📡 Intento de conexión #{reconnect_attempts}")
            bot.run(config.BOT_TOKEN, reconnect=True)
            
        except discord.errors.HTTPException as e:
            if e.status == 429:  # Rate limited
                retry_after = e.response.headers.get('Retry-After', 60)
                print(f"⚠️ Rate limited. Esperando {retry_after} segundos...")
                time.sleep(int(retry_after))
            else:
                print(f"❌ Error HTTP: {e}")
                time.sleep(10)
                
        except discord.errors.GatewayNotFound:
            print("❌ Gateway no encontrado. Reintentando en 10 segundos...")
            time.sleep(10)
            
        except discord.errors.ConnectionClosed:
            print("❌ Conexión cerrada. Reintentando en 5 segundos...")
            time.sleep(5)
            
        except discord.errors.PrivilegedIntentsRequired:
            print("❌ ERROR CRÍTICO: Intents privilegiados no activados en Discord Developer Portal")
            print("   Activa: PRESENCE INTENT, SERVER MEMBERS INTENT y MESSAGE CONTENT INTENT")
            time.sleep(30)
            
        except KeyboardInterrupt:
            print("\n👋 Bot detenido por el usuario")
            break
            
        except Exception as e:
            print(f"❌ Error inesperado: {e}")
            traceback.print_exc()
            time.sleep(10)
        
        print(f"🔄 Reintentando conexión en 5 segundos... (intento #{reconnect_attempts})")
        time.sleep(5)

if __name__ == "__main__":
    try:
        # Verificar token
        if not config.BOT_TOKEN:
            print("❌ ERROR: No se encontró BOT_TOKEN en config.py")
            sys.exit(1)
        
        # Verificar que el archivo de logs existe
        os.makedirs(os.path.dirname(config.LOG_PATH), exist_ok=True)
        
        # Iniciar bot con reconexión INFINITA
        run_bot_with_reconnect()
        
    except KeyboardInterrupt:
        print("\n👋 Bot detenido por el usuario")
    except Exception as e:
        print(f"❌ Error fatal: {e}")
        traceback.print_exc()
