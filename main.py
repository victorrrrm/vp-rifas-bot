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

# ============================================
# SERVIDOR WEB PARA API (Flask)
# ============================================

from flask import Flask, jsonify
from flask_cors import CORS
import threading

flask_app = Flask(__name__)
CORS(flask_app)

# Variables para compartir entre Flask y Discord
ultima_rifa = None
ultimos_ganadores = []
eventos_activos = {
    '2x1': False,
    'cashback_doble': False,
    'oferta_porcentaje': 0
}

@flask_app.route('/api/rifa')
def api_rifa():
    """Devuelve la rifa activa"""
    global ultima_rifa
    if ultima_rifa:
        return jsonify({
            'id': ultima_rifa['id'],
            'nombre': ultima_rifa['nombre'],
            'premio': ultima_rifa['premio'],
            'precio': ultima_rifa['precio_boleto'],
            'total_boletos': ultima_rifa['total_boletos']
        })
    return jsonify({'error': 'No hay rifa activa'}), 404

@flask_app.route('/api/ganadores')
def api_ganadores():
    """Devuelve los últimos ganadores"""
    global ultimos_ganadores
    return jsonify({'ganadores': ultimos_ganadores[:10]})

@flask_app.route('/api/eventos')
def api_eventos():
    """Devuelve los eventos activos"""
    global eventos_activos
    eventos = []
    if eventos_activos['2x1']:
        eventos.append({'icono': '🎟️', 'nombre': '2x1', 'descripcion': 'Compra 1, lleva 2 boletos'})
    if eventos_activos['cashback_doble']:
        eventos.append({'icono': '💰', 'nombre': 'Cashback Doble', 'descripcion': 'Cashback al 20%'})
    if eventos_activos['oferta_porcentaje'] > 0:
        eventos.append({'icono': '🏷️', 'nombre': 'Oferta', 'descripcion': f"{eventos_activos['oferta_porcentaje']}% de descuento"})
    return jsonify({'eventos': eventos})

def run_flask():
    flask_app.run(host='0.0.0.0', port=8080)

# Iniciar Flask en hilo separado
threading.Thread(target=run_flask, daemon=True).start()

# ============================================
# CONFIGURACIÓN GLOBAL
# ============================================

VERSION = "4.3.0"
PREFIX = "!"
start_time = datetime.now()
reconnect_attempts = 0
MAX_RECONNECT_ATTEMPTS = 999999

reset_pending = None
reset_user_pending = None
sorteo_en_curso = False
sorteo_cancelado = False

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

# IDs de roles
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
        self.actualizar_jackpot_task.start()
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
            await db.execute('''
                CREATE TABLE IF NOT EXISTS clientes (
                    discord_id TEXT PRIMARY KEY,
                    nombre TEXT NOT NULL,
                    total_compras INTEGER DEFAULT 0,
                    total_gastado INTEGER DEFAULT 0,
                    ultima_compra TIMESTAMP,
                    fecha_registro TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            ''')
            await db.execute('''
                CREATE TABLE IF NOT EXISTS transacciones (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    tipo TEXT NOT NULL,
                    boleto_id INTEGER,
                    monto INTEGER NOT NULL,
                    origen_id TEXT,
                    destino_id TEXT,
                    descripcion TEXT,
                    fecha TIMESTAMP DEFAULT CURRENT_TIMESTAMP
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
            await db.execute('''
                CREATE TABLE IF NOT EXISTS puntos_revancha (
                    usuario_id TEXT PRIMARY KEY,
                    puntos INTEGER DEFAULT 0,
                    ultima_actualizacion TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            ''')
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
        logger.info("✅ Tablas de sistemas inicializadas")
    
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
        logger.info(f"✅ Bot conectado como {self.user} (ID: {self.user.id})")
        logger.info(f"🌐 En {len(self.guilds)} servidores")
        global reconnect_attempts
        reconnect_attempts = 0
        self.reconnecting = False
        
        # Actualizar variables globales de la API
        global eventos_activos
        eventos_activos['2x1'] = evento_2x1
        eventos_activos['cashback_doble'] = evento_cashback_doble
        eventos_activos['oferta_porcentaje'] = evento_oferta_porcentaje
        
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
    global evento_cashback_doble
    async with aiosqlite.connect('data/rifas.db') as db:
        cursor = await db.execute('''
            SELECT porcentaje FROM cashback_config WHERE id = 1
        ''')
        config_cb = await cursor.fetchone()
        porcentaje = config_cb[0] if config_cb else 10
        if evento_cashback_doble:
            porcentaje = porcentaje * 2
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
    global evento_oferta_activa, evento_oferta_porcentaje
    async with aiosqlite.connect('data/rifas.db') as db:
        cursor = await db.execute('''
            SELECT f.nivel, fc.descuento 
            FROM fidelizacion f
            JOIN fidelizacion_config fc ON f.nivel = fc.nivel
            WHERE f.usuario_id = ?
        ''', (usuario_id,))
        result = await cursor.fetchone()
        descuento_base = result[1] if result else 0
        if evento_oferta_activa:
            descuento_base += evento_oferta_porcentaje
        return min(descuento_base, 50)

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

async def procesar_comision_vendedor(vendedor_id, monto_compra):
    global COMISION_VENDEDOR
    async with aiosqlite.connect('data/rifas.db') as db:
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
    async with aiosqlite.connect('data/rifas.db') as db:
        await db.execute('''
            INSERT INTO puntos_revancha (usuario_id, puntos)
            VALUES (?, ?)
            ON CONFLICT(usuario_id) DO UPDATE SET
                puntos = puntos + ?
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
    async with aiosqlite.connect('data/rifas.db') as db:
        await db.execute('''
            UPDATE jackpot SET total = ? WHERE id = 1
        ''', (jackpot_total,))
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
    """Registra un ganador en la tabla de ganadores históricos"""
    async with aiosqlite.connect('data/rifas.db') as db:
        await db.execute('''
            INSERT INTO ganadores_historicos (usuario_id, usuario_nick, premio, rifa_nombre, numero)
            VALUES (?, ?, ?, ?, ?)
        ''', (usuario_id, usuario_nick, premio, rifa_nombre, numero))
        await db.commit()
        
        # Actualizar variable global de la API
        global ultimos_ganadores
        cursor = await db.execute('''
            SELECT usuario_nick, premio, fecha FROM ganadores_historicos
            ORDER BY fecha DESC LIMIT 10
        ''')
        ultimos_ganadores = await cursor.fetchall()
        ultimos_ganadores = [dict(g) for g in ultimos_ganadores]

# ============================================
# COMANDOS DE USUARIO
# ============================================

@bot.command(name="ayuda")
async def cmd_ayuda(ctx):
    """Ver todos los comandos disponibles según tu rol"""
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
        director = """
        `!crearifa [premio] [precio] [total]` - Crear rifa
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
        `!rankingreset` - Resetear ranking de rifa
        `!topcomprador [id]` - Top compradores por ID de rifa
        `!topgastadoresreset` - Resetear top gastadores
        `!setnivel` - Configurar niveles
        `!setcomision [%]` - Configurar comisión vendedores
        `!alertar [mensaje]` - Alerta a todos
        `!rifaeliminacion [total] [premio] [valor]` - Iniciar rifa eliminación
        `!rifaeliminacionr` - Eliminar número de rifa eliminación
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
        `!setnivel` - Configurar niveles
        `!estadisticas` - Estadísticas globales
        `!auditoria` - Ver transacciones
        `!exportar` - Exportar a CSV
        `!backup` - Crear backup
        `!resetallsistema` - Reiniciar sistema
        `!version` - Versión del bot
        `!crearcodigo [codigo] [vp]` - Crear código promocional
        `!borrarcodigo [codigo]` - Borrar código promocional
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
    
    # Actualizar variable global de la API
    global ultima_rifa
    ultima_rifa = rifa_activa
    
    embed = discord.Embed(
        title=f"🎟️ {rifa_activa['nombre']}",
        description=f"**{rifa_activa['premio']}**",
        color=config.COLORS['primary']
    )
    embed.add_field(name="🏆 Premio", value=f"${rifa_activa['valor_premio']}", inline=True)
    embed.add_field(name="💰 Precio", value=f"${rifa_activa['precio_boleto']}", inline=True)
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
    descuento = await obtener_descuento_usuario(str(ctx.author.id))
    global evento_2x1
    boletos_a_pagar = cantidad
    boletos_a_recibir = cantidad
    if evento_2x1:
        boletos_a_pagar = cantidad // 2 + (cantidad % 2)
        boletos_a_recibir = cantidad
    precio_total = precio_boleto * boletos_a_pagar
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
    seleccionados = random.sample(disponibles, boletos_a_recibir)
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
async def cmd_balance(ctx, usuario: discord.Member = None):
    if not await verificar_canal(ctx):
        return
    target = usuario if usuario else ctx.author
    if usuario and not await check_admin(ctx):
        await ctx.send(embed=embeds.crear_embed_error("No tienes permiso para ver el balance de otros"))
        return
    async with aiosqlite.connect('data/rifas.db') as db:
        cursor = await db.execute('''
            SELECT balance FROM usuarios_balance WHERE discord_id = ?
        ''', (str(target.id),))
        result = await cursor.fetchone()
        balance = result[0] if result else 0
    embed = discord.Embed(
        title=f"💰 Balance de {target.name}",
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
    rifa_activa = await bot.db.get_rifa_activa()
    if not rifa_activa:
        await ctx.send(embed=embeds.crear_embed_error("No hay rifa activa"))
        return
    async with aiosqlite.connect('data/rifas.db') as db:
        db.row_factory = aiosqlite.Row
        cursor = await db.execute('''
            SELECT comprador_nick, COUNT(*) as boletos
            FROM boletos 
            WHERE rifa_id = ?
            GROUP BY comprador_id
            ORDER BY boletos DESC
            LIMIT 10
        ''', (rifa_activa['id'],))
        ranking = await cursor.fetchall()
    if not ranking:
        await ctx.send(embed=embeds.crear_embed_info("Sin datos", "Aún no hay compras en esta rifa"))
        return
    embed = discord.Embed(title="🏆 TOP COMPRADORES", color=config.COLORS['primary'])
    for i, u in enumerate(ranking, 1):
        medalla = {1: "🥇", 2: "🥈", 3: "🥉"}.get(i, f"{i}.")
        embed.add_field(name=f"{medalla} {u['comprador_nick']}", value=f"[{u['boletos']}]", inline=False)
    await ctx.send(embed=embed)

@bot.command(name="topcomprador")
async def cmd_top_comprador(ctx, id_rifa: int):
    if not await check_admin(ctx):
        await ctx.send("❌ No tienes permiso para usar este comando")
        return
    async with aiosqlite.connect('data/rifas.db') as db:
        db.row_factory = aiosqlite.Row
        cursor = await db.execute('''
            SELECT comprador_nick, COUNT(*) as boletos
            FROM boletos 
            WHERE rifa_id = ?
            GROUP BY comprador_id
            ORDER BY boletos DESC
            LIMIT 10
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
        color=config.COLORS['primary']
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
    async with aiosqlite.connect('data/rifas.db') as db:
        await db.execute('''
            UPDATE referidos_config SET porcentaje_descuento = ? WHERE id = 1
        ''', (porcentaje,))
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
    async with aiosqlite.connect('data/rifas.db') as db:
        db.row_factory = aiosqlite.Row
        cursor = await db.execute('''
            SELECT gasto_total, nivel FROM fidelizacion WHERE usuario_id = ?
        ''', (str(ctx.author.id),))
        data = await cursor.fetchone()
        if not data:
            await ctx.send(embed=embeds.crear_embed_info("Sin compras", "Aún no tienes historial"))
            return
        cursor = await db.execute('''
            SELECT * FROM fidelizacion_config WHERE nivel = ?
        ''', (data['nivel'],))
        beneficios = await cursor.fetchone()
    embed = discord.Embed(
        title=f"🏆 Nivel: {data['nivel']}",
        description=f"Gasto total: **${data['gasto_total']} VP$**",
        color=config.COLORS['primary']
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
    async with aiosqlite.connect('data/rifas.db') as db:
        await db.execute('UPDATE fidelizacion SET gasto_total = 0, nivel = "BRONCE"')
        await db.commit()
    await ctx.message.delete()
    await ctx.send(embed=embeds.crear_embed_exito("Ranking de gastadores reseteado"))

@bot.command(name="verniveles")
async def cmd_ver_niveles(ctx):
    if not await verificar_canal(ctx):
        return
    async with aiosqlite.connect('data/rifas.db') as db:
        db.row_factory = aiosqlite.Row
        cursor = await db.execute('''
            SELECT * FROM fidelizacion_config ORDER BY gasto_minimo ASC
        ''')
        niveles = await cursor.fetchall()
    embed = discord.Embed(
        title="📊 **CONFIGURACIÓN DE NIVELES**",
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
        minimo = str(n['gasto_minimo'])
        maximo = str(n['gasto_maximo']) if n['gasto_maximo'] else '∞'
        rango = f"${minimo} - ${maximo}"
        texto = f"**Rango:** {rango}\n"
        texto += f"**Beneficios:** {' | '.join(beneficios) if beneficios else 'Ninguno'}"
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
    async with aiosqlite.connect('data/rifas.db') as db:
        await db.execute(f'''
            UPDATE fidelizacion_config SET {columna} = ? WHERE nivel = ?
        ''', (valor_int, nivel))
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
    await ctx.message.delete()
    await ctx.send(embed=embeds.crear_embed_exito(f"Cashback: {porcentaje}%"))

@bot.command(name="pagarcashback")
async def cmd_pagar_cashback(ctx):
    if not await check_ceo(ctx):
        return
    await ctx.send("💰 Procesando pagos de cashback...")
    async with aiosqlite.connect('data/rifas.db') as db:
        db.row_factory = aiosqlite.Row
        cursor = await db.execute('''
            SELECT usuario_id, cashback_acumulado FROM cashback WHERE cashback_acumulado > 0
        ''')
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
                ON CONFLICT(discord_id) DO UPDATE SET
                    balance = balance + ?
            ''', (u['usuario_id'], u['usuario_id'], monto, monto))
            await db.execute('''
                INSERT INTO transacciones (tipo, monto, destino_id, descripcion)
                VALUES ('cashback', ?, ?, ?)
            ''', (monto, u['usuario_id'], f"Pago de cashback"))
            await db.execute('''
                UPDATE cashback SET cashback_acumulado = 0, cashback_recibido = cashback_recibido + ?
                WHERE usuario_id = ?
            ''', (monto, u['usuario_id']))
            total_pagado += monto
            await enviar_dm(u['usuario_id'], "💰 Cashback pagado", f"Has recibido ${monto} VP$ por cashback")
        await db.commit()
    await ctx.message.delete()
    await ctx.send(embed=embeds.crear_embed_exito(f"Pagados ${total_pagado} VP$ de cashback a {len(usuarios)} usuarios"))

@bot.command(name="resetcashback")
async def cmd_reset_cashback(ctx):
    if not await check_ceo(ctx):
        return
    async with aiosqlite.connect('data/rifas.db') as db:
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
    async with aiosqlite.connect('data/rifas.db') as db:
        cursor = await db.execute('''
            SELECT balance FROM usuarios_balance WHERE discord_id = ?
        ''', (str(usuario.id),))
        result = await cursor.fetchone()
        balance = result[0] if result else 0
    precio_boleto = rifa_activa['precio_boleto']
    descuento = await obtener_descuento_usuario(str(usuario.id))
    precio_final = int(precio_boleto * (100 - descuento) / 100)
    if balance < precio_final:
        await ctx.send(embed=embeds.crear_embed_error(f"Usuario necesita {precio_final} VP$"))
        return
    async with aiosqlite.connect('data/rifas.db') as db:
        await db.execute('''
            UPDATE usuarios_balance SET balance = ? WHERE discord_id = ?
        ''', (balance - precio_final, str(usuario.id)))
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
    await enviar_dm(str(usuario.id), "🎟️ Boleto comprado", 
                    f"Has comprado el boleto #{numero} por ${precio_final} VP$\nDescuento aplicado: {descuento}%")
    await enviar_dm(str(ctx.author.id), "💰 Venta realizada", 
                    f"Has vendido el boleto #{numero} a {usuario.name} por ${precio_final} VP$\nComisión: {COMISION_VENDEDOR}%")
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
    if len(disponibles) < cantidad:
        await ctx.send(embed=embeds.crear_embed_error(f"Solo {len(disponibles)} disponibles"))
        return
    async with aiosqlite.connect('data/rifas.db') as db:
        cursor = await db.execute('''
            SELECT balance FROM usuarios_balance WHERE discord_id = ?
        ''', (str(usuario.id),))
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
    seleccionados = random.sample(disponibles, boletos_a_recibir)
    comprados = []
    async with aiosqlite.connect('data/rifas.db') as db:
        await db.execute('''
            UPDATE usuarios_balance SET balance = ? WHERE discord_id = ?
        ''', (balance - precio_final, str(usuario.id)))
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
    await enviar_dm(str(usuario.id), "🎟️ Compra realizada", 
                    f"Has comprado {len(comprados)} boletos: {', '.join(map(str, comprados))}\n"
                    f"Total: ${precio_final}\n"
                    f"Descuento: {descuento}%\n"
                    f"Cashback acumulado: ${cashback}")
    await enviar_dm(str(ctx.author.id), "💰 Venta realizada", 
                    f"Has vendido {len(comprados)} boletos a {usuario.name} por ${precio_final}\nComisión: {COMISION_VENDEDOR}%")
    await ctx.message.delete()
    await ctx.send(embed=embeds.crear_embed_exito(f"✅ Venta realizada. Revisa tu DM."))

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
        embed.add_field(name="Comisiones pendientes", value=f"**${vendedor[0]}**", inline=False)
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
                    f"**Vendidos:** {vendidos}\n"
                    f"**Disponibles:** {len(disponibles)}",
        color=config.COLORS['info']
    )
    await ctx.send(embed=embed)

# ============================================
# COMANDOS DE ADMIN
# ============================================

@bot.command(name="crearifa")
async def cmd_crear_rifa(ctx, premio: str, precio: int, total: int):
    if not await check_admin(ctx):
        return
    nombre = f"Rifa {datetime.now().strftime('%d/%m')}"
    rifa_id = await bot.db.crear_rifa(nombre, premio, precio, precio, total)
    await reiniciar_ranking_rifa(rifa_id)
    
    # Actualizar variable global de la API
    global ultima_rifa
    rifa_activa = await bot.db.get_rifa_activa()
    if rifa_activa:
        ultima_rifa = rifa_activa
    
    embed = embeds.crear_embed_exito(f"✅ Rifa creada ID: {rifa_id}\nPremio: {premio}\nPrecio: ${precio}\nTotal: {total} boletos")
    await ctx.send(embed=embed)

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

@bot.command(name="vercomisiones")
async def cmd_ver_comisiones(ctx):
    if not await check_admin(ctx):
        return
    async with aiosqlite.connect('data/rifas.db') as db:
        db.row_factory = aiosqlite.Row
        cursor = await db.execute('''
            SELECT nombre, comisiones_pendientes FROM vendedores WHERE comisiones_pendientes > 0
        ''')
        vendedores = await cursor.fetchall()
    if not vendedores:
        await ctx.send(embed=embeds.crear_embed_info("Info", "No hay comisiones pendientes"))
        return
    embed = discord.Embed(title="💰 Comisiones pendientes", color=config.COLORS['primary'])
    for v in vendedores:
        embed.add_field(name=v['nombre'], value=f"${v['comisiones_pendientes']}", inline=True)
    await ctx.send(embed=embed)

@bot.command(name="pagarcomisiones")
async def cmd_pagar_comisiones(ctx):
    if not await check_admin(ctx):
        return
    await ctx.send("💰 Procesando pagos de comisiones...")
    async with aiosqlite.connect('data/rifas.db') as db:
        db.row_factory = aiosqlite.Row
        cursor = await db.execute('''
            SELECT discord_id, nombre, comisiones_pendientes FROM vendedores WHERE comisiones_pendientes > 0
        ''')
        vendedores = await cursor.fetchall()
        if not vendedores:
            await ctx.send(embed=embeds.crear_embed_error("No hay comisiones pendientes"))
            return
        total_pagado = 0
        for v in vendedores:
            monto = v['comisiones_pendientes']
            await db.execute('''
                INSERT INTO usuarios_balance (discord_id, nombre, balance)
                VALUES (?, ?, ?)
                ON CONFLICT(discord_id) DO UPDATE SET
                    balance = balance + ?
            ''', (v['discord_id'], v['nombre'], monto, monto))
            await db.execute('''
                INSERT INTO transacciones (tipo, monto, destino_id, descripcion)
                VALUES ('comision', ?, ?, ?)
            ''', (monto, v['discord_id'], f"Pago de comisiones"))
            await db.execute('''
                UPDATE vendedores SET comisiones_pendientes = 0, comisiones_pagadas = comisiones_pagadas + ?
                WHERE discord_id = ?
            ''', (monto, v['discord_id']))
            total_pagado += monto
            await enviar_dm(v['discord_id'], "💰 Comisiones pagadas", f"Has recibido ${monto} VP$ por tus ventas")
        await db.commit()
    await ctx.message.delete()
    await ctx.send(embed=embeds.crear_embed_exito(f"Pagadas ${total_pagado} VP$ en comisiones a {len(vendedores)} vendedores"))

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
    await ctx.message.delete()
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
    async with aiosqlite.connect('data/rifas.db') as db:
        cursor = await db.execute('''
            SELECT comprador_id, COUNT(*) as boletos
            FROM boletos
            WHERE rifa_id = ?
            GROUP BY comprador_id
        ''', (rifa['id'],))
        compradores = await cursor.fetchall()
        for comprador in compradores:
            await agregar_puntos_revancha(comprador[0], comprador[1])
    await ctx.message.delete()
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
        await enviar_dm(b['comprador_id'], "🎉 ¡FELICIDADES! GANASTE", 
                        f"Has ganado la rifa {rifa['nombre']} con el boleto #{b['numero']}\nPremio: {rifa['premio']}\nContacta a los administradores para reclamar tu premio.")
        # Registrar en ganadores históricos
        await registrar_ganador_historico(b['comprador_id'], b['comprador_nick'], rifa['premio'], rifa['nombre'], b['numero'])
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
        await enviar_dm(b['comprador_id'], "🎉 ¡FELICIDADES! GANASTE", 
                        f"Has ganado la rifa {rifa['nombre']} (ID: {id_rifa}) con el boleto #{b['numero']}\nPremio: {rifa['premio']}\nContacta a los administradores para reclamar tu premio.")
        await registrar_ganador_historico(b['comprador_id'], b['comprador_nick'], rifa['premio'], rifa['nombre'], b['numero'])
    await ctx.send(embed=embed)

@bot.command(name="vendedoradd")
async def cmd_vendedor_add(ctx, usuario: discord.Member, comision: int = 10):
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
    await ctx.message.delete()
    await ctx.send(embed=embeds.crear_embed_exito(f"{usuario.name} es vendedor ({comision}%)"))

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

@bot.command(name="alertar")
async def cmd_alertar(ctx, *, mensaje: str):
    if not await check_admin(ctx):
        return
    embed = discord.Embed(
        title="📢 ALERTA DE RIFAS",
        description=mensaje,
        color=config.COLORS['primary']
    )
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
        await db.execute('''
            INSERT INTO transacciones (tipo, monto, destino_id, descripcion)
            VALUES ('acreditar', ?, ?, ?)
        ''', (cantidad, str(usuario.id), f"Acreditación por {ctx.author.name}"))
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
        await db.execute('''
            INSERT INTO transacciones (tipo, monto, origen_id, descripcion)
            VALUES ('retirar', ?, ?, ?)
        ''', (cantidad, str(usuario.id), f"Retiro por {ctx.author.name}"))
        await db.commit()
    await enviar_dm(str(usuario.id), "💰 Retiro de VP$", f"Se te han retirado ${cantidad} VP$ de tu balance")
    await ctx.message.delete()
    await ctx.send(embed=embeds.crear_embed_exito(f"Retirados ${cantidad} VP$ de {usuario.name}"))

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
        cursor = await db.execute('SELECT SUM(cashback_acumulado) FROM cashback')
        total_cashback = (await cursor.fetchone())[0] or 0
        cursor = await db.execute('SELECT SUM(comisiones_pendientes) FROM vendedores')
        total_comisiones = (await cursor.fetchone())[0] or 0
    embed = discord.Embed(title="📊 ESTADÍSTICAS GLOBALES", color=config.COLORS['primary'])
    embed.add_field(name="🎟️ Total rifas", value=f"**{total_rifas}**", inline=True)
    embed.add_field(name="🎲 Boletos vendidos", value=f"**{total_boletos}**", inline=True)
    embed.add_field(name="💰 Total recaudado", value=f"**${total_recaudado}**", inline=True)
    embed.add_field(name="👥 Clientes registrados", value=f"**{total_clientes}**", inline=True)
    embed.add_field(name="💵 VP$ en circulación", value=f"**${total_vp}**", inline=True)
    embed.add_field(name="💸 Cashback pendiente", value=f"**${total_cashback}**", inline=True)
    embed.add_field(name="🏦 Comisiones pendientes", value=f"**${total_comisiones}**", inline=True)
    await ctx.send(embed=embed)

@bot.command(name="auditoria")
async def cmd_auditoria(ctx):
    if not await check_ceo(ctx):
        return
    async with aiosqlite.connect('data/rifas.db') as db:
        db.row_factory = aiosqlite.Row
        cursor = await db.execute('''
            SELECT tipo, monto, descripcion, fecha FROM transacciones
            ORDER BY fecha DESC LIMIT 20
        ''')
        transacciones = await cursor.fetchall()
    if not transacciones:
        await ctx.send(embed=embeds.crear_embed_info("Sin datos", "No hay transacciones registradas"))
        return
    embed = discord.Embed(title="📋 AUDITORÍA", color=config.COLORS['primary'])
    for t in transacciones[:15]:
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
    await ctx.message.delete()
    await ctx.send(embed=embeds.crear_embed_exito("✅ Datos exportados. Revisa tu DM."))

@bot.command(name="backup")
async def cmd_backup(ctx):
    if not await check_ceo(ctx):
        return
    fecha = datetime.now().strftime("%Y%m%d_%H%M%S")
    backup = f"/tmp/backup_{fecha}.db"
    shutil.copy2('data/rifas.db', backup)
    await ctx.author.send(file=discord.File(backup))
    await ctx.message.delete()
    await ctx.send(embed=embeds.crear_embed_exito("✅ Backup creado. Revisa tu DM."))

@bot.command(name="resetallsistema")
async def cmd_reset_all_sistema(ctx):
    global reset_pending
    if not await check_ceo(ctx):
        return
    embed = discord.Embed(
        title="⚠️ REINICIO TOTAL DEL SISTEMA",
        description="Esto borrará TODOS los datos.\n"
                    "Escribe `!confirmarreset` en los próximos 30 segundos.",
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
        await ctx.send(embed=embeds.crear_embed_error("Sin solicitud pendiente"))
        return
    if datetime.now().timestamp() - reset_pending['timestamp'] > 30:
        reset_pending = None
        await ctx.send(embed=embeds.crear_embed_error("Tiempo expirado"))
        return
    await ctx.send("🔄 **REINICIANDO SISTEMA...**")
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
        await db.execute("DELETE FROM codigos_promocionales")
        await db.execute("DELETE FROM codigos_canjeados")
        await db.execute("DELETE FROM puntos_revancha")
        await db.execute("DELETE FROM sqlite_sequence")
        await db.commit()
    reset_pending = None
    await ctx.message.delete()
    await ctx.send(embed=embeds.crear_embed_exito("✅ Sistema reiniciado correctamente"))

# ============================================
# COMANDOS DE PROMOCIONES
# ============================================

@bot.command(name="crearcodigo")
async def cmd_crear_codigo(ctx, codigo: str, recompensa: int):
    if not await check_ceo(ctx):
        return
    codigo = codigo.lower()
    async with aiosqlite.connect('data/rifas.db') as db:
        cursor = await db.execute('''
            SELECT * FROM codigos_promocionales WHERE codigo = ?
        ''', (codigo,))
        existe = await cursor.fetchone()
        if existe:
            await ctx.send(embed=embeds.crear_embed_error("El código ya existe"))
            return
        await db.execute('''
            INSERT INTO codigos_promocionales (codigo, recompensa, creador_id)
            VALUES (?, ?, ?)
        ''', (codigo, recompensa, str(ctx.author.id)))
        await db.commit()
    await ctx.message.delete()
    await ctx.send(embed=embeds.crear_embed_exito(f"Código `{codigo}` creado con {recompensa} VP$"))

@bot.command(name="borrarcodigo")
async def cmd_borrar_codigo(ctx, codigo: str):
    if not await check_ceo(ctx):
        return
    codigo = codigo.lower()
    async with aiosqlite.connect('data/rifas.db') as db:
        await db.execute('''
            DELETE FROM codigos_promocionales WHERE codigo = ?
        ''', (codigo,))
        await db.commit()
    await ctx.message.delete()
    await ctx.send(embed=embeds.crear_embed_exito(f"Código `{codigo}` eliminado"))

@bot.command(name="canjear")
async def cmd_canjear(ctx, codigo: str):
    if not await verificar_canal(ctx):
        return
    codigo = codigo.lower()
    async with aiosqlite.connect('data/rifas.db') as db:
        cursor = await db.execute('''
            SELECT recompensa FROM codigos_promocionales WHERE codigo = ? AND activo = 1
        ''', (codigo,))
        codigo_data = await cursor.fetchone()
        if not codigo_data:
            await ctx.send(embed=embeds.crear_embed_error("Código inválido o expirado"))
            return
        cursor = await db.execute('''
            SELECT * FROM codigos_canjeados WHERE codigo = ? AND usuario_id = ?
        ''', (codigo, str(ctx.author.id)))
        ya_canjeado = await cursor.fetchone()
        if ya_canjeado:
            await ctx.send(embed=embeds.crear_embed_error("Ya has canjeado este código"))
            return
        recompensa = codigo_data[0]
        await db.execute('''
            INSERT INTO codigos_canjeados (codigo, usuario_id)
            VALUES (?, ?)
        ''', (codigo, str(ctx.author.id)))
        await db.execute('''
            INSERT INTO usuarios_balance (discord_id, nombre, balance)
            VALUES (?, ?, ?)
            ON CONFLICT(discord_id) DO UPDATE SET
                balance = balance + ?
        ''', (str(ctx.author.id), ctx.author.name, recompensa, recompensa))
        await db.commit()
    await enviar_dm(str(ctx.author.id), "🎁 Código canjeado", f"Has canjeado el código `{codigo}` y recibido ${recompensa} VP$")
    await ctx.message.delete()
    await ctx.send(embed=embeds.crear_embed_exito(f"✅ Código canjeado. Revisa tu DM."))

# ============================================
# COMANDOS DE EVENTOS
# ============================================

@bot.command(name="2x1")
async def cmd_2x1(ctx):
    if not await check_ceo(ctx):
        return
    global evento_2x1, eventos_activos
    evento_2x1 = not evento_2x1
    eventos_activos['2x1'] = evento_2x1
    estado = "ACTIVADO" if evento_2x1 else "DESACTIVADO"
    await ctx.message.delete()
    await ctx.send(embed=embeds.crear_embed_exito(f"🎉 Evento 2x1 {estado}"))

@bot.command(name="cashbackdoble")
async def cmd_cashback_doble(ctx):
    if not await check_ceo(ctx):
        return
    global evento_cashback_doble, eventos_activos
    evento_cashback_doble = not evento_cashback_doble
    eventos_activos['cashback_doble'] = evento_cashback_doble
    estado = "ACTIVADO" if evento_cashback_doble else "DESACTIVADO"
    await ctx.message.delete()
    await ctx.send(embed=embeds.crear_embed_exito(f"🎉 Cashback doble {estado}"))

@bot.command(name="oferta")
async def cmd_oferta(ctx, porcentaje: int):
    if not await check_ceo(ctx):
        return
    global evento_oferta_activa, evento_oferta_porcentaje, eventos_activos
    if porcentaje < 0 or porcentaje > 30:
        await ctx.send(embed=embeds.crear_embed_error("0-30%"))
        return
    evento_oferta_activa = True
    evento_oferta_porcentaje = porcentaje
    eventos_activos['oferta_porcentaje'] = porcentaje
    await ctx.message.delete()
    await ctx.send(embed=embeds.crear_embed_exito(f"🎉 Oferta activada: {porcentaje}% de descuento adicional"))

@bot.command(name="ofertadesactivar")
async def cmd_oferta_desactivar(ctx):
    if not await check_ceo(ctx):
        return
    global evento_oferta_activa, evento_oferta_porcentaje, eventos_activos
    evento_oferta_activa = False
    evento_oferta_porcentaje = 0
    eventos_activos['oferta_porcentaje'] = 0
    await ctx.message.delete()
    await ctx.send(embed=embeds.crear_embed_exito("🎉 Oferta desactivada"))

# ============================================
# SISTEMA DE JACKPOT
# ============================================

@bot.command(name="jackpot")
async def cmd_jackpot(ctx, base: int, porcentaje: int, id_rifa: int):
    if not await check_ceo(ctx):
        return
    global jackpot_activo, jackpot_base, jackpot_porcentaje, jackpot_rifa_id, jackpot_total
    if base <= 0 or porcentaje <= 0:
        await ctx.send(embed=embeds.crear_embed_error("Valores deben ser positivos"))
        return
    rifa = await bot.db.get_rifa_activa()
    if not rifa or rifa['id'] != id_rifa:
        await ctx.send(embed=embeds.crear_embed_error(f"Rifa con ID {id_rifa} no está activa"))
        return
    jackpot_activo = True
    jackpot_base = base
    jackpot_porcentaje = porcentaje
    jackpot_rifa_id = id_rifa
    jackpot_total = base
    async with aiosqlite.connect('data/rifas.db') as db:
        await db.execute('''
            INSERT OR REPLACE INTO jackpot (id, rifa_id, base, porcentaje, total, activo)
            VALUES (1, ?, ?, ?, ?, 1)
        ''', (id_rifa, base, porcentaje, base))
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
    async with aiosqlite.connect('data/rifas.db') as db:
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
    async with aiosqlite.connect('data/rifas.db') as db:
        cursor = await db.execute('''
            SELECT comprador_id, comprador_nick FROM boletos 
            WHERE rifa_id = ? AND estado = 'pagado'
        ''', (jackpot_rifa_id,))
        boletos = await cursor.fetchall()
    if len(boletos) == 0:
        await ctx.send(embed=embeds.crear_embed_error("No hay boletos vendidos"))
        return
    premio_por_ganador = jackpot_total // ganadores
    ganadores_sel = random.sample(boletos, min(ganadores, len(boletos)))
    embed = discord.Embed(
        title="🎰 **JACKPOT SORTEADO** 🎰",
        description=f"Premio total: **${jackpot_total}**",
        color=config.COLORS['success']
    )
    for i, ganador in enumerate(ganadores_sel, 1):
        embed.add_field(
            name=f"🏆 Ganador #{i}",
            value=f"{ganador[1]} | ${premio_por_ganador}",
            inline=False
        )
        async with aiosqlite.connect('data/rifas.db') as db2:
            await db2.execute('''
                INSERT INTO usuarios_balance (discord_id, nombre, balance)
                VALUES (?, ?, ?)
                ON CONFLICT(discord_id) DO UPDATE SET
                    balance = balance + ?
            ''', (ganador[0], ganador[1], premio_por_ganador, premio_por_ganador))
        await enviar_dm(ganador[0], "🎰 ¡GANASTE EL JACKPOT!", f"Has ganado ${premio_por_ganador} VP$ del jackpot")
    await ctx.send(embed=embed)
    jackpot_activo = False
    jackpot_total = 0

# ============================================
# SISTEMA DE REVANCHA
# ============================================

@bot.command(name="mispuntos")
async def cmd_mis_puntos(ctx):
    if not await verificar_canal(ctx):
        return
    async with aiosqlite.connect('data/rifas.db') as db:
        cursor = await db.execute('''
            SELECT puntos FROM puntos_revancha WHERE usuario_id = ?
        ''', (str(ctx.author.id),))
        result = await cursor.fetchone()
        puntos = result[0] if result else 0
    embed = discord.Embed(
        title="🔄 Puntos de Revancha",
        description=f"Tienes **{puntos}** puntos",
        color=config.COLORS['primary']
    )
    embed.set_footer(text="¡Los puntos se acumulan por boletos perdidos!")
    await ctx.send(embed=embed)

@bot.command(name="puntosreset")
async def cmd_puntos_reset(ctx, usuario: discord.Member = None):
    if not await check_ceo(ctx):
        return
    async with aiosqlite.connect('data/rifas.db') as db:
        if usuario:
            await db.execute('''
                DELETE FROM puntos_revancha WHERE usuario_id = ?
            ''', (str(usuario.id),))
            await ctx.send(embed=embeds.crear_embed_exito(f"Puntos de {usuario.name} reseteados"))
        else:
            await db.execute('DELETE FROM puntos_revancha')
            await ctx.send(embed=embeds.crear_embed_exito("Todos los puntos de revancha reseteados"))
        await db.commit()
    await ctx.message.delete()

# ============================================
# SISTEMA DE RIFA ELIMINACIÓN
# ============================================

@bot.command(name="rifaeliminacion")
async def cmd_rifa_eliminacion(ctx, total: int, premio: str, valor: int):
    if not await check_admin(ctx):
        return
    global rifa_eliminacion_activa, rifa_eliminacion_total, rifa_eliminacion_premio
    global rifa_eliminacion_valor, rifa_eliminacion_numeros
    if total <= 0 or valor <= 0:
        await ctx.send(embed=embeds.crear_embed_error("Valores inválidos"))
        return
    rifa_eliminacion_activa = True
    rifa_eliminacion_total = total
    rifa_eliminacion_premio = premio
    rifa_eliminacion_valor = valor
    rifa_eliminacion_numeros = list(range(1, total + 1))
    embed = discord.Embed(
        title="🔪 **RIFA ELIMINACIÓN INICIADA** 🔪",
        description=f"**Premio:** {premio}\n"
                    f"**Valor por boleto:** ${valor}\n"
                    f"**Total de boletos:** {total}\n\n"
                    f"¡El último número que quede GANA!",
        color=config.COLORS['primary']
    )
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
    async with aiosqlite.connect('data/rifas.db') as db:
        cursor = await db.execute('''
            SELECT balance FROM usuarios_balance WHERE discord_id = ?
        ''', (str(ctx.author.id),))
        result = await cursor.fetchone()
        balance = result[0] if result else 0
    if balance < rifa_eliminacion_valor:
        await ctx.send(embed=embeds.crear_embed_error(f"Necesitas ${rifa_eliminacion_valor} VP$"))
        return
    nuevo_balance = balance - rifa_eliminacion_valor
    async with aiosqlite.connect('data/rifas.db') as db:
        await db.execute('''
            UPDATE usuarios_balance SET balance = ? WHERE discord_id = ?
        ''', (nuevo_balance, str(ctx.author.id)))
        await db.execute('''
            INSERT INTO transacciones (tipo, monto, origen_id, descripcion)
            VALUES ('eliminacion', ?, ?, ?)
        ''', (rifa_eliminacion_valor, str(ctx.author.id), f"Compra número {numero} en rifa eliminación"))
        await db.commit()
    rifa_eliminacion_numeros.remove(numero)
    if len(rifa_eliminacion_numeros) == 1:
        ganador_numero = rifa_eliminacion_numeros[0]
        embed = discord.Embed(
            title="🏆 **RIFA ELIMINACIÓN FINALIZADA** 🏆",
            description=f"¡El número **{ganador_numero}** sobrevivió!\n"
                        f"**Ganador:** <@{ctx.author.id}>\n"
                        f"**Premio:** {rifa_eliminacion_premio}",
            color=config.COLORS['success']
        )
        await ctx.send(embed=embed)
        await enviar_dm(str(ctx.author.id), "🏆 ¡GANASTE LA RIFA ELIMINACIÓN!", 
                        f"Has ganado la rifa eliminación con el número #{ganador_numero}\n"
                        f"Premio: {rifa_eliminacion_premio}\n"
                        f"Contacta a los administradores para reclamar tu premio.")
        rifa_eliminacion_activa = False
    else:
        await ctx.message.delete()
        await ctx.send(embed=embeds.crear_embed_exito(
            f"✅ Has comprado el número #{numero} por ${rifa_eliminacion_valor}\n"
            f"Quedan {len(rifa_eliminacion_numeros)} números en juego."
        ))
        await enviar_dm(str(ctx.author.id), "🎟️ Compra en Rifa Eliminación", 
                        f"Has comprado el número #{numero} por ${rifa_eliminacion_valor}\n"
                        f"Quedan {len(rifa_eliminacion_numeros)} números en juego.")

@bot.command(name="beliminacion")
async def cmd_ver_eliminacion(ctx):
    if not await verificar_canal(ctx):
        return
    global rifa_eliminacion_activa, rifa_eliminacion_numeros, rifa_eliminacion_total
    if not rifa_eliminacion_activa:
        await ctx.send(embed=embeds.crear_embed_error("No hay rifa eliminación activa"))
        return
    embed = discord.Embed(
        title="🔪 RIFA ELIMINACIÓN",
        description=f"**Boletos disponibles:** {len(rifa_eliminacion_numeros)}/{rifa_eliminacion_total}\n"
                    f"**Precio por boleto:** ${rifa_eliminacion_valor}",
        color=config.COLORS['info']
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
