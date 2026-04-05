import discord
from discord.ext import commands, tasks
import asyncio
import logging
import random
import csv
import shutil
import os
import sys
import traceback
import hashlib
from datetime import datetime, timedelta
import config
from src.database.database import Database
from src.database.db_pool import db_pool
import src.utils.embeds as embeds

# ============================================
# SERVIDOR WEB (Flask) - API
# ============================================
from flask import Flask, jsonify
from flask_cors import CORS
import threading

flask_app = Flask(__name__)
CORS(flask_app)

ultima_rifa = None
ultimos_ganadores = []
eventos_activos = {'2x1': False, 'cashback_doble': False, 'oferta_porcentaje': 0}

@flask_app.route('/api/rifa')
def api_rifa():
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
    return jsonify({'ganadores': ultimos_ganadores[:10]})

@flask_app.route('/api/eventos')
def api_eventos():
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

threading.Thread(target=run_flask, daemon=True).start()

# ============================================
# CONFIGURACIÓN GLOBAL
# ============================================
VERSION = config.VERSION
PREFIX = config.PREFIX
start_time = datetime.now()
reset_pending = None
sorteo_en_curso = False
sorteo_cancelado = False

# Variables de eventos (se cargarán desde BD)
evento_2x1 = False
evento_cashback_doble = False
evento_oferta_porcentaje = 0
evento_oferta_activa = False

# Porcentajes configurables
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

# Rifa eliminación
rifa_eliminacion_activa = False
rifa_eliminacion_total = 0
rifa_eliminacion_premio = ""
rifa_eliminacion_valor = 0
rifa_eliminacion_numeros = []

# ============================================
# LOGGING
# ============================================
os.makedirs('src/logs', exist_ok=True)
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
        self.reconnecting = False

    async def setup_hook(self):
        logger.info("🚀 Iniciando configuración...")
        await self.db.init_db()
        await db_pool.init()
        await self.init_sistemas_tablas()
        await self.cargar_eventos_desde_bd()
        self.keep_alive_task.start()
        self.status_task.start()
        self.actualizar_jackpot_task.start()
        logger.info("✅ Tareas automáticas iniciadas")

    async def init_sistemas_tablas(self):
        async with db_pool.connection() as conn:
            # Tabla de persistencia de eventos
            await conn.execute('''
                CREATE TABLE IF NOT EXISTS eventos_config (
                    id INTEGER PRIMARY KEY CHECK (id = 1),
                    evento_2x1 BOOLEAN DEFAULT 0,
                    cashback_doble BOOLEAN DEFAULT 0,
                    oferta_activa BOOLEAN DEFAULT 0,
                    oferta_porcentaje INTEGER DEFAULT 0
                )
            ''')
            await conn.execute("INSERT OR IGNORE INTO eventos_config (id) VALUES (1)")

            # Añadir columna puntos_asignados a rifas si no existe
            try:
                await conn.execute("ALTER TABLE rifas ADD COLUMN puntos_asignados BOOLEAN DEFAULT 0")
            except:
                pass

            # Índices para mejorar rendimiento
            await conn.execute("CREATE INDEX IF NOT EXISTS idx_boletos_rifa ON boletos(rifa_id)")
            await conn.execute("CREATE INDEX IF NOT EXISTS idx_boletos_comprador ON boletos(comprador_id)")
            await conn.execute("CREATE INDEX IF NOT EXISTS idx_transacciones_fecha ON transacciones(fecha)")
            await conn.execute("CREATE INDEX IF NOT EXISTS idx_rifas_estado ON rifas(estado)")
            await conn.execute("CREATE INDEX IF NOT EXISTS idx_fidelizacion_gasto ON fidelizacion(gasto_total)")
            await conn.execute("CREATE INDEX IF NOT EXISTS idx_boletos_vendedor ON boletos(vendedor_id)")
            await conn.execute("CREATE INDEX IF NOT EXISTS idx_boletos_rifa_numero ON boletos(rifa_id, numero)")

            await conn.commit()
        logger.info("✅ Tablas e índices actualizados")

    async def cargar_eventos_desde_bd(self):
        global evento_2x1, evento_cashback_doble, evento_oferta_activa, evento_oferta_porcentaje, eventos_activos
        row = await db_pool.fetchone("SELECT evento_2x1, cashback_doble, oferta_activa, oferta_porcentaje FROM eventos_config WHERE id = 1")
        if row:
            evento_2x1 = bool(row['evento_2x1'])
            evento_cashback_doble = bool(row['cashback_doble'])
            evento_oferta_activa = bool(row['oferta_activa'])
            evento_oferta_porcentaje = row['oferta_porcentaje']
            eventos_activos.update({
                '2x1': evento_2x1,
                'cashback_doble': evento_cashback_doble,
                'oferta_porcentaje': evento_oferta_porcentaje
            })
        logger.info(f"Eventos cargados: 2x1={evento_2x1}, cashback_doble={evento_cashback_doble}, oferta={evento_oferta_porcentaje}%")

    async def on_ready(self):
        logger.info(f"✅ Bot conectado como {self.user}")
        logger.info(f"🌐 En {len(self.guilds)} servidores")
        global ultima_rifa
        rifa_activa = await self.db.get_rifa_activa()
        if rifa_activa:
            ultima_rifa = rifa_activa
        await self.enviar_log_sistema("🟢 BOT INICIADO", f"Versión {VERSION} | Servidores: {len(self.guilds)}")

    async def on_disconnect(self):
        logger.warning("⚠️ Bot desconectado")
        await self.enviar_log_sistema("🔴 BOT DESCONECTADO", "Intentando reconectar...")

    async def on_resumed(self):
        logger.info("🔄 Bot reconectado")
        await self.enviar_log_sistema("🟢 BOT RECONECTADO", "Conexión restablecida")

    async def enviar_log_sistema(self, titulo, descripcion):
        canal = self.get_channel(config.UPDATE_CHANNEL_ID)
        if canal:
            embed = discord.Embed(title=titulo, description=descripcion, color=0x0099FF, timestamp=datetime.now())
            embed.set_footer(text=f"VP Rifas v{VERSION}")
            await canal.send(embed=embed)

    @tasks.loop(seconds=30)
    async def keep_alive_task(self):
        try:
            activity = random.choice([
                f"{PREFIX}ayuda | {len(self.guilds)} servers",
                f"Rifas VP v{VERSION}",
                f"{PREFIX}comprarrandom | Activo",
                f"{len(self.users)} usuarios"
            ])
            await self.change_presence(activity=discord.Activity(type=discord.ActivityType.watching, name=activity))
        except:
            pass

    @tasks.loop(minutes=60)
    async def status_task(self):
        uptime = datetime.now() - start_time
        horas = int(uptime.total_seconds() // 3600)
        minutos = int((uptime.total_seconds() % 3600) // 60)
        await self.enviar_log_sistema("💓 HEARTBEAT", f"Activo {horas}h {minutos}m | Versión {VERSION}")

    @tasks.loop(seconds=5)
    async def actualizar_jackpot_task(self):
        global jackpot_activo, jackpot_total
        if not jackpot_activo:
            return
        canal = self.get_channel(config.JACKPOT_CANAL_ID)
        if not canal:
            return
        embed = discord.Embed(title="🎰 JACKPOT ACTIVO", description=f"**Premio acumulado:** ${jackpot_total:,} VP$", color=0xFFD700)
        embed.add_field(name="💎 Base", value=f"${jackpot_base:,}", inline=True)
        embed.add_field(name="📊 % por boleto", value=f"{jackpot_porcentaje}%", inline=True)
        mensaje_jackpot = None
        async for msg in canal.history(limit=50):
            if msg.author == self.user and msg.pinned:
                mensaje_jackpot = msg
                break
        if mensaje_jackpot:
            await mensaje_jackpot.edit(embed=embed)
        else:
            msg = await canal.send(embed=embed)
            await msg.pin()

bot = VPRifasBot()

# ============================================
# FUNCIONES AUXILIARES
# ============================================
async def enviar_log(ctx, accion, detalles):
    canal = bot.get_channel(config.LOG_CHANNEL_ID)
    if canal:
        embed = discord.Embed(title=f"📋 {accion}", description=detalles, color=0x0099FF, timestamp=datetime.now())
        embed.add_field(name="👤 Usuario", value=ctx.author.name, inline=True)
        embed.add_field(name="📌 Canal", value=ctx.channel.name, inline=True)
        await canal.send(embed=embed)

async def enviar_dm(usuario_id, titulo, mensaje):
    try:
        usuario = await bot.fetch_user(int(usuario_id))
        embed = discord.Embed(title=titulo, description=mensaje, color=0x0099FF)
        await usuario.send(embed=embed)
    except discord.Forbidden:
        logger.warning(f"No se pudo enviar DM a {usuario_id}")
    except Exception as e:
        logger.error(f"Error enviando DM a {usuario_id}: {e}")

async def verificar_canal(ctx):
    if not ctx.guild:
        await ctx.send("❌ Solo en servidores")
        return False
    if ctx.channel.category_id != config.CATEGORIA_RIFAS:
        await ctx.send(f"❌ Este comando solo funciona en canales de la categoría de rifas")
        return False
    return True

def tiene_rol(miembro, role_id):
    return any(role.id == role_id for role in miembro.roles)

async def check_admin(ctx):
    if not ctx.guild: return False
    member = ctx.guild.get_member(ctx.author.id)
    if not member: return False
    return tiene_rol(member, config.ROLES['CEO']) or tiene_rol(member, config.ROLES['DIRECTOR'])

async def check_vendedor(ctx):
    if not ctx.guild: return False
    member = ctx.guild.get_member(ctx.author.id)
    if not member: return False
    return tiene_rol(member, config.ROLES['CEO']) or tiene_rol(member, config.ROLES['DIRECTOR']) or tiene_rol(member, config.ROLES['RIFAS'])

async def check_ceo(ctx):
    if not ctx.guild: return False
    member = ctx.guild.get_member(ctx.author.id)
    if not member: return False
    return tiene_rol(member, config.ROLES['CEO'])

async def generar_codigo_unico(usuario_id):
    hash_obj = hashlib.md5(usuario_id.encode())
    return f"VP-{hash_obj.hexdigest()[:8].upper()}"

async def obtener_o_crear_codigo(usuario_id, nombre):
    codigo = await db_pool.fetchone("SELECT codigo FROM referidos_codigos WHERE usuario_id = ?", (usuario_id,))
    if codigo:
        return codigo[0]
    codigo = await generar_codigo_unico(usuario_id)
    await db_pool.execute("INSERT INTO referidos_codigos (usuario_id, codigo) VALUES (?, ?)", (usuario_id, codigo))
    return codigo

async def obtener_nivel_por_gasto(gasto_total):
    row = await db_pool.fetchone('''
        SELECT nivel FROM fidelizacion_config
        WHERE gasto_minimo <= ? AND (gasto_maximo >= ? OR gasto_maximo IS NULL)
        ORDER BY gasto_minimo DESC LIMIT 1
    ''', (gasto_total, gasto_total))
    return row[0] if row else 'BRONCE'

async def actualizar_fidelizacion(usuario_id, monto):
    row = await db_pool.fetchone("SELECT gasto_total FROM fidelizacion WHERE usuario_id = ?", (usuario_id,))
    nuevo_gasto = (row[0] if row else 0) + monto
    if row:
        await db_pool.execute("UPDATE fidelizacion SET gasto_total = ?, fecha_actualizacion = CURRENT_TIMESTAMP WHERE usuario_id = ?", (nuevo_gasto, usuario_id))
    else:
        await db_pool.execute("INSERT INTO fidelizacion (usuario_id, gasto_total) VALUES (?, ?)", (usuario_id, nuevo_gasto))
    nuevo_nivel = await obtener_nivel_por_gasto(nuevo_gasto)
    await db_pool.execute("UPDATE fidelizacion SET nivel = ? WHERE usuario_id = ?", (nuevo_nivel, usuario_id))
    return nuevo_nivel

async def aplicar_cashback(usuario_id, monto):
    global evento_cashback_doble
    row = await db_pool.fetchone("SELECT porcentaje FROM cashback_config WHERE id = 1")
    porcentaje = row[0] if row else 10
    if evento_cashback_doble:
        porcentaje *= 2
    cashback = int(monto * porcentaje / 100)
    await db_pool.execute('''
        INSERT INTO cashback (usuario_id, cashback_acumulado) VALUES (?, ?)
        ON CONFLICT(usuario_id) DO UPDATE SET cashback_acumulado = cashback_acumulado + ?, ultima_actualizacion = CURRENT_TIMESTAMP
    ''', (usuario_id, cashback, cashback))
    return cashback

async def obtener_descuento_usuario(usuario_id):
    global evento_oferta_activa, evento_oferta_porcentaje
    row = await db_pool.fetchone('''
        SELECT fc.descuento FROM fidelizacion f
        JOIN fidelizacion_config fc ON f.nivel = fc.nivel
        WHERE f.usuario_id = ?
    ''', (usuario_id,))
    descuento = row[0] if row else 0
    if evento_oferta_activa:
        descuento += evento_oferta_porcentaje
    return min(descuento, 50)

async def procesar_comision_referido(comprador_id, monto):
    referidor = await db_pool.fetchone("SELECT referidor_id FROM referidos_relaciones WHERE referido_id = ?", (comprador_id,))
    if not referidor: return
    config_ref = await db_pool.fetchone("SELECT porcentaje_comision FROM referidos_config WHERE id = 1")
    porcentaje = config_ref[0] if config_ref else 10
    comision = int(monto * porcentaje / 100)
    await db_pool.execute('''
        INSERT INTO usuarios_balance (discord_id, nombre, balance) VALUES (?, (SELECT nombre FROM clientes WHERE discord_id = ?), ?)
        ON CONFLICT(discord_id) DO UPDATE SET balance = balance + ?
    ''', (referidor[0], referidor[0], comision, comision))
    await db_pool.execute('''
        UPDATE referidos_relaciones SET primera_compra = 1, total_compras = total_compras + 1,
        total_gastado = total_gastado + ?, comisiones_generadas = comisiones_generadas + ?
        WHERE referido_id = ?
    ''', (monto, comision, comprador_id))

async def procesar_comision_vendedor(vendedor_id, monto):
    global COMISION_VENDEDOR
    comision = int(monto * COMISION_VENDEDOR / 100)
    await db_pool.execute('''
        INSERT INTO vendedores (discord_id, nombre, comisiones_pendientes) VALUES (?, (SELECT nombre FROM clientes WHERE discord_id = ?), ?)
        ON CONFLICT(discord_id) DO UPDATE SET comisiones_pendientes = comisiones_pendientes + ?, total_ventas = total_ventas + 1
    ''', (vendedor_id, vendedor_id, comision, comision))

async def agregar_puntos_revancha(usuario_id, boletos_perdidos):
    await db_pool.execute('''
        INSERT INTO puntos_revancha (usuario_id, puntos) VALUES (?, ?)
        ON CONFLICT(usuario_id) DO UPDATE SET puntos = puntos + ?
    ''', (usuario_id, boletos_perdidos, boletos_perdidos))

async def actualizar_jackpot(monto_compra):
    global jackpot_activo, jackpot_total, jackpot_rifa_id, jackpot_porcentaje
    if not jackpot_activo: return
    rifa_activa = await bot.db.get_rifa_activa()
    if not rifa_activa or rifa_activa['id'] != jackpot_rifa_id: return
    aporte = int(monto_compra * jackpot_porcentaje / 100)
    jackpot_total += aporte
    await db_pool.execute("UPDATE jackpot SET total = ? WHERE id = 1", (jackpot_total,))

async def registrar_ganador_historico(usuario_id, usuario_nick, premio, rifa_nombre, numero):
    await db_pool.execute('''
        INSERT INTO ganadores_historicos (usuario_id, usuario_nick, premio, rifa_nombre, numero) VALUES (?, ?, ?, ?, ?)
    ''', (usuario_id, usuario_nick, premio, rifa_nombre, numero))
    global ultimos_ganadores
    rows = await db_pool.fetchall("SELECT usuario_nick, premio, fecha FROM ganadores_historicos ORDER BY fecha DESC LIMIT 10")
    ultimos_ganadores = [dict(row) for row in rows]

def calcular_precio_con_2x1(cantidad, precio_unitario, descuento_porcentaje):
    boletos_a_pagar = cantidad // 2 + (cantidad % 2)
    boletos_a_recibir = cantidad
    precio_bruto = precio_unitario * boletos_a_pagar
    precio_final = int(precio_bruto * (100 - descuento_porcentaje) / 100)
    return boletos_a_pagar, boletos_a_recibir, precio_final

# ============================================
# COMANDOS DE USUARIO
# ============================================

@bot.command(name="ayuda")
async def cmd_ayuda(ctx):
    if not await verificar_canal(ctx): return
    member = ctx.guild.get_member(ctx.author.id)
    es_ceo = tiene_rol(member, config.ROLES['CEO'])
    es_director = tiene_rol(member, config.ROLES['DIRECTOR'])
    es_vendedor = tiene_rol(member, config.ROLES['RIFAS'])

    embed = discord.Embed(title="🎟️ SISTEMA DE RIFAS VP", description=f"Prefijo: `{PREFIX}` | Versión: {VERSION}", color=config.COLORS['primary'])
    embed.add_field(name="👤 BÁSICOS", value="""`!rifa` - Ver rifa activa
`!comprarrandom [cantidad]` - Comprar boletos aleatorios
`!misboletos` - Ver tus boletos
`!balance` - Ver tu balance
`!topvp` - Ranking de VP$
`!ranking` - Top compradores de la rifa actual
`!historial` - Tu historial
`!celiminacion [número]` - Comprar en rifa eliminación
`!beliminacion` - Ver números disponibles en rifa eliminación
`!mispuntos` - Ver puntos de revancha""", inline=False)
    embed.add_field(name="🤝 REFERIDOS", value="""`!codigo` - Tu código de referido
`!usar [código]` - Usar código (solo una vez)
`!misreferidos` - Ver tus referidos""", inline=False)
    embed.add_field(name="🏆 FIDELIZACIÓN", value="""`!nivel` - Tu nivel y beneficios
`!topgastadores` - Ranking de gasto
`!cashback` - Tu cashback acumulado
`!topcashback` - Ranking cashback
`!verniveles` - Ver configuración de niveles""", inline=False)
    embed.add_field(name="🎁 PROMOCIONES", value="`!canjear [código]` - Canjear código promocional", inline=False)
    if es_vendedor or es_director or es_ceo:
        embed.add_field(name="💰 VENDEDORES", value="""`!vender [@usuario] [número]` - Vender número específico
`!venderrandom [@usuario] [cantidad]` - Vender aleatorios
`!misventas` - Ver tus ventas
`!listaboletos` - Lista de boletos""", inline=False)
    if es_director or es_ceo:
        embed.add_field(name="🎯 DIRECTORES", value="""`!crearifa [premio] [precio] [total]` - Crear rifa
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
`!topcomprador [id]` - Top compradores por ID de rifa
`!topgastadoresreset` - Resetear top gastadores
`!setnivel` - Configurar niveles
`!setcomision [%]` - Configurar comisión vendedores
`!alertar [mensaje]` - Alerta a todos
`!rifaeliminacion [total] [premio] [valor]` - Iniciar rifa eliminación
`!rifaeliminacionr` - Eliminar número de rifa eliminación""", inline=False)
    if es_ceo:
        embed.add_field(name="👑 CEO", value="""`!acreditarvp [@usuario] [cantidad]` - Acreditar VP$
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
`!canjearpuntos [cantidad]` - Canjear puntos por VP$""", inline=False)
    embed.set_footer(text="Ejemplo: !comprarrandom 3")
    await ctx.send(embed=embed)

@bot.command(name="version")
async def cmd_version(ctx):
    uptime = datetime.now() - start_time
    horas = uptime.total_seconds() // 3600
    minutos = (uptime.total_seconds() % 3600) // 60
    embed = discord.Embed(title="🤖 VP RIFAS BOT", description=f"**Versión:** `{VERSION}`\n**Estado:** 🟢 Activo\n**Uptime:** {int(horas)}h {int(minutos)}m\n**Servidores:** {len(bot.guilds)}\n**Usuarios:** {len(bot.users)}", color=config.COLORS['primary'])
    await ctx.send(embed=embed)

@bot.command(name="rifa")
async def cmd_rifa(ctx):
    if not await verificar_canal(ctx): return
    rifa = await bot.db.get_rifa_activa()
    if not rifa:
        await ctx.send(embed=embeds.crear_embed_error("No hay rifa activa"))
        return
    global ultima_rifa
    ultima_rifa = rifa
    embed = discord.Embed(title=f"🎟️ {rifa['nombre']}", description=f"**{rifa['premio']}**", color=config.COLORS['primary'])
    embed.add_field(name="🏆 Premio", value=f"${rifa['valor_premio']}", inline=True)
    embed.add_field(name="💰 Precio", value=f"${rifa['precio_boleto']}", inline=True)
    embed.set_footer(text="Usa !comprarrandom para participar")
    await ctx.send(embed=embed)

@bot.command(name="comprarrandom")
@commands.cooldown(1, 5, commands.BucketType.user)
async def cmd_comprar_random(ctx, cantidad: int = 1):
    if not await verificar_canal(ctx): return
    if cantidad < 1 or cantidad > 50:
        await ctx.send(embed=embeds.crear_embed_error("Cantidad entre 1 y 50"))
        return
    rifa = await bot.db.get_rifa_activa()
    if not rifa:
        await ctx.send(embed=embeds.crear_embed_error("No hay rifa activa"))
        return
    disponibles = await bot.db.get_boletos_disponibles(rifa['id'])
    if len(disponibles) < cantidad:
        await ctx.send(embed=embeds.crear_embed_error(f"Solo hay {len(disponibles)} boletos"))
        return
    descuento = await obtener_descuento_usuario(str(ctx.author.id))
    boletos_a_pagar, boletos_a_recibir, precio_final = calcular_precio_con_2x1(cantidad, rifa['precio_boleto'], descuento)
    balance = await db_pool.fetchone("SELECT balance FROM usuarios_balance WHERE discord_id = ?", (str(ctx.author.id),))
    balance = balance[0] if balance else 0
    if balance < precio_final:
        await ctx.send(embed=embeds.crear_embed_error(f"Necesitas {precio_final} VP$"))
        return
    seleccionados = random.sample(disponibles, boletos_a_recibir)
    async with db_pool.connection() as conn:
        await conn.execute("UPDATE usuarios_balance SET balance = ? WHERE discord_id = ?", (balance - precio_final, str(ctx.author.id)))
        for num in seleccionados:
            await conn.execute('INSERT INTO boletos (rifa_id, numero, comprador_id, comprador_nick, precio_pagado) VALUES (?, ?, ?, ?, ?)', (rifa['id'], num, str(ctx.author.id), ctx.author.name, rifa['precio_boleto']))
        await conn.commit()
    await actualizar_fidelizacion(str(ctx.author.id), precio_final)
    cashback = await aplicar_cashback(str(ctx.author.id), precio_final)
    await procesar_comision_referido(str(ctx.author.id), precio_final)
    await actualizar_jackpot(precio_final)
    await enviar_dm(str(ctx.author.id), "✅ Compra realizada", f"Has comprado {len(seleccionados)} boletos: {', '.join(map(str, seleccionados))}\nTotal: ${precio_final}\nDescuento: {descuento}%\nCashback acumulado: ${cashback}")
    await ctx.message.delete()
    await ctx.send(embed=embeds.crear_embed_exito("✅ Compra realizada! Revisa tu DM."))

@bot.command(name="misboletos")
async def cmd_mis_boletos(ctx):
    if not await verificar_canal(ctx): return
    rifa = await bot.db.get_rifa_activa()
    if not rifa:
        await ctx.send(embed=embeds.crear_embed_error("No hay rifa activa"))
        return
    rows = await db_pool.fetchall("SELECT numero FROM boletos WHERE rifa_id = ? AND comprador_id = ?", (rifa['id'], str(ctx.author.id)))
    if not rows:
        await ctx.send(embed=embeds.crear_embed_error("No tienes boletos"))
        return
    numeros = [str(r['numero']) for r in rows]
    embed = discord.Embed(title="🎟️ Tus boletos", description=f"Números: {', '.join(numeros)}", color=config.COLORS['primary'])
    await ctx.send(embed=embed)

@bot.command(name="balance")
async def cmd_balance(ctx, usuario: discord.Member = None):
    if not await verificar_canal(ctx): return
    target = usuario if usuario else ctx.author
    if usuario and not await check_admin(ctx):
        await ctx.send(embed=embeds.crear_embed_error("No tienes permiso"))
        return
    row = await db_pool.fetchone("SELECT balance FROM usuarios_balance WHERE discord_id = ?", (str(target.id),))
    balance = row[0] if row else 0
    embed = discord.Embed(title=f"💰 Balance de {target.name}", description=f"**{balance} VP$**", color=config.COLORS['primary'])
    await ctx.send(embed=embed)

@bot.command(name="topvp")
async def cmd_top_vp(ctx):
    if not await verificar_canal(ctx): return
    rows = await db_pool.fetchall("SELECT nombre, balance FROM usuarios_balance WHERE balance > 0 ORDER BY balance DESC LIMIT 10")
    if not rows:
        await ctx.send(embed=embeds.crear_embed_info("Sin datos", "No hay usuarios con VP$"))
        return
    embed = discord.Embed(title="🏆 TOP 10 VP$", color=config.COLORS['primary'])
    for i, u in enumerate(rows, 1):
        medalla = {1: "🥇", 2: "🥈", 3: "🥉"}.get(i, f"{i}.")
        embed.add_field(name=f"{medalla} {u['nombre']}", value=f"**{u['balance']} VP$**", inline=False)
    await ctx.send(embed=embed)

@bot.command(name="ranking")
async def cmd_ranking(ctx):
    if not await verificar_canal(ctx): return
    rifa = await bot.db.get_rifa_activa()
    if not rifa:
        await ctx.send(embed=embeds.crear_embed_error("No hay rifa activa"))
        return
    rows = await db_pool.fetchall('''
        SELECT comprador_nick, COUNT(*) as boletos FROM boletos
        WHERE rifa_id = ? GROUP BY comprador_id ORDER BY boletos DESC LIMIT 10
    ''', (rifa['id'],))
    if not rows:
        await ctx.send(embed=embeds.crear_embed_info("Sin datos", "Aún no hay compras"))
        return
    embed = discord.Embed(title="🏆 TOP COMPRADORES", color=config.COLORS['primary'])
    for i, u in enumerate(rows, 1):
        medalla = {1: "🥇", 2: "🥈", 3: "🥉"}.get(i, f"{i}.")
        embed.add_field(name=f"{medalla} {u['comprador_nick']}", value=f"[{u['boletos']}]", inline=False)
    await ctx.send(embed=embed)

@bot.command(name="topcomprador")
async def cmd_top_comprador(ctx, id_rifa: int):
    if not await check_admin(ctx): return
    rifa = await db_pool.fetchone("SELECT * FROM rifas WHERE id = ?", (id_rifa,))
    if not rifa:
        await ctx.send(embed=embeds.crear_embed_error(f"No existe rifa con ID {id_rifa}"))
        return
    rows = await db_pool.fetchall('''
        SELECT comprador_nick, COUNT(*) as boletos FROM boletos
        WHERE rifa_id = ? GROUP BY comprador_id ORDER BY boletos DESC LIMIT 10
    ''', (id_rifa,))
    if not rows:
        await ctx.send(embed=embeds.crear_embed_info("Sin datos", f"No hay compras en la rifa #{id_rifa}"))
        return
    embed = discord.Embed(title=f"🏆 TOP COMPRADORES - Rifa #{id_rifa}", description=f"**Premio:** {rifa['premio']}", color=config.COLORS['primary'])
    for i, u in enumerate(rows, 1):
        medalla = {1: "🥇", 2: "🥈", 3: "🥉"}.get(i, f"{i}.")
        embed.add_field(name=f"{medalla} {u['comprador_nick']}", value=f"[{u['boletos']} boletos]", inline=False)
    await ctx.send(embed=embed)

@bot.command(name="historial")
async def cmd_historial(ctx):
    if not await verificar_canal(ctx): return
    rows = await db_pool.fetchall('''
        SELECT b.numero, r.nombre as rifa, b.fecha_compra, b.precio_pagado
        FROM boletos b JOIN rifas r ON b.rifa_id = r.id
        WHERE b.comprador_id = ? ORDER BY b.fecha_compra DESC LIMIT 20
    ''', (str(ctx.author.id),))
    if not rows:
        await ctx.send(embed=embeds.crear_embed_error("Sin historial"))
        return
    embed = discord.Embed(title="📜 Tu historial", color=config.COLORS['primary'])
    for b in rows[:10]:
        embed.add_field(name=f"{b['rifa']} - #{b['numero']}", value=f"${b['precio_pagado']} - {b['fecha_compra'][:10]}", inline=False)
    await ctx.send(embed=embed)

# ============================================
# REFERIDOS
# ============================================
@bot.command(name="codigo")
async def cmd_codigo(ctx):
    if not await verificar_canal(ctx): return
    codigo = await obtener_o_crear_codigo(str(ctx.author.id), ctx.author.name)
    stats = await db_pool.fetchone("SELECT COUNT(*), SUM(comisiones_generadas) FROM referidos_relaciones WHERE referidor_id = ?", (str(ctx.author.id),))
    total_referidos = stats[0] if stats else 0
    total_comisiones = stats[1] if stats and stats[1] else 0
    embed = discord.Embed(title="🔗 Tu código de referido", description=f"`{codigo}`", color=config.COLORS['primary'])
    embed.add_field(name="📊 Referidos", value=f"**{total_referidos}**", inline=True)
    embed.add_field(name="💰 Comisiones", value=f"**{total_comisiones} VP$**", inline=True)
    await ctx.send(embed=embed)

@bot.command(name="usar")
async def cmd_usar_codigo(ctx, codigo: str):
    if not await verificar_canal(ctx): return
    ya = await db_pool.fetchone("SELECT * FROM referidos_relaciones WHERE referido_id = ?", (str(ctx.author.id),))
    if ya:
        await ctx.send(embed=embeds.crear_embed_error("Ya tienes un referidor"))
        return
    referidor = await db_pool.fetchone("SELECT usuario_id FROM referidos_codigos WHERE codigo = ? AND usuario_id != ?", (codigo.upper(), str(ctx.author.id)))
    if not referidor:
        await ctx.send(embed=embeds.crear_embed_error("Código inválido"))
        return
    await db_pool.execute("INSERT INTO referidos_relaciones (referido_id, referidor_id) VALUES (?, ?)", (str(ctx.author.id), referidor[0]))
    await ctx.send(embed=embeds.crear_embed_exito("Código aplicado correctamente"))

@bot.command(name="misreferidos")
async def cmd_mis_referidos(ctx):
    if not await verificar_canal(ctx): return
    rows = await db_pool.fetchall('''
        SELECT r.referido_id, c.nombre, r.total_compras, r.comisiones_generadas
        FROM referidos_relaciones r LEFT JOIN clientes c ON r.referido_id = c.discord_id
        WHERE r.referidor_id = ? ORDER BY r.fecha_registro DESC LIMIT 20
    ''', (str(ctx.author.id),))
    if not rows:
        await ctx.send(embed=embeds.crear_embed_error("No tienes referidos"))
        return
    embed = discord.Embed(title="👥 Tus referidos", color=config.COLORS['primary'])
    for ref in rows[:10]:
        nombre = ref['nombre'] or "Usuario"
        embed.add_field(name=f"👤 {nombre}", value=f"Compras: {ref['total_compras']} | Comisiones: ${ref['comisiones_generadas']}", inline=False)
    await ctx.send(embed=embed)

# ============================================
# FIDELIZACIÓN
# ============================================
@bot.command(name="nivel")
async def cmd_nivel(ctx):
    if not await verificar_canal(ctx): return
    data = await db_pool.fetchone("SELECT gasto_total, nivel FROM fidelizacion WHERE usuario_id = ?", (str(ctx.author.id),))
    if not data:
        await ctx.send(embed=embeds.crear_embed_info("Sin compras", "Aún no tienes historial"))
        return
    beneficios = await db_pool.fetchone("SELECT * FROM fidelizacion_config WHERE nivel = ?", (data['nivel'],))
    embed = discord.Embed(title=f"🏆 Nivel: {data['nivel']}", description=f"Gasto total: **${data['gasto_total']} VP$**", color=config.COLORS['primary'])
    if beneficios:
        texto = []
        if beneficios['descuento'] > 0: texto.append(f"💰 {beneficios['descuento']}% descuento")
        if beneficios['boletos_gratis_por_cada'] > 0: texto.append(f"🎟️ +{beneficios['cantidad_boletos_gratis']} c/{beneficios['boletos_gratis_por_cada']}")
        if beneficios['acceso_anticipado_horas'] > 0: texto.append(f"⏰ {beneficios['acceso_anticipado_horas']}h anticipación")
        if beneficios['canal_vip']: texto.append("👑 Canal VIP")
        if beneficios['rifas_exclusivas']: texto.append("✨ Rifas exclusivas")
        if texto: embed.add_field(name="✅ Beneficios", value="\n".join(texto), inline=False)
    await ctx.send(embed=embed)

@bot.command(name="topgastadores")
async def cmd_top_gastadores(ctx):
    if not await verificar_canal(ctx): return
    rows = await db_pool.fetchall('''
        SELECT c.nombre, f.gasto_total, f.nivel FROM fidelizacion f
        LEFT JOIN clientes c ON f.usuario_id = c.discord_id
        WHERE f.gasto_total > 0 ORDER BY f.gasto_total DESC LIMIT 10
    ''')
    if not rows:
        await ctx.send(embed=embeds.crear_embed_info("Sin datos", "No hay gastadores"))
        return
    embed = discord.Embed(title="🏆 TOP GASTADORES", color=config.COLORS['primary'])
    for i, u in enumerate(rows, 1):
        medalla = {1: "🥇", 2: "🥈", 3: "🥉"}.get(i, f"{i}.")
        nombre = u['nombre'] or "Usuario"
        embed.add_field(name=f"{medalla} {nombre}", value=f"Gastado: ${u['gasto_total']} | {u['nivel']}", inline=False)
    await ctx.send(embed=embed)

@bot.command(name="topgastadoresreset")
async def cmd_top_gastadores_reset(ctx):
    if not await check_ceo(ctx): return
    await db_pool.execute("UPDATE fidelizacion SET gasto_total = 0, nivel = 'BRONCE'")
    await ctx.message.delete()
    await ctx.send(embed=embeds.crear_embed_exito("Ranking de gastadores reseteado"))

@bot.command(name="verniveles")
async def cmd_ver_niveles(ctx):
    if not await verificar_canal(ctx): return
    niveles = await db_pool.fetchall("SELECT * FROM fidelizacion_config ORDER BY gasto_minimo ASC")
    embed = discord.Embed(title="📊 CONFIGURACIÓN DE NIVELES", color=config.COLORS['primary'])
    for n in niveles:
        beneficios = []
        if n['descuento'] > 0: beneficios.append(f"💰 {n['descuento']}% desc")
        if n['boletos_gratis_por_cada'] > 0: beneficios.append(f"🎟️ +{n['cantidad_boletos_gratis']} c/{n['boletos_gratis_por_cada']}")
        if n['acceso_anticipado_horas'] > 0: beneficios.append(f"⏰ {n['acceso_anticipado_horas']}h")
        if n['canal_vip']: beneficios.append("👑 VIP")
        if n['rifas_exclusivas']: beneficios.append("✨ Excl")
        rango = f"${n['gasto_minimo']} - ${n['gasto_maximo'] if n['gasto_maximo'] else '∞'}"
        texto = f"**Rango:** {rango}\n**Beneficios:** {' | '.join(beneficios) if beneficios else 'Ninguno'}"
        embed.add_field(name=f"**{n['nivel']}**", value=texto, inline=False)
    await ctx.send(embed=embed)

@bot.command(name="setnivel")
async def cmd_set_nivel(ctx, nivel: str = None, campo: str = None, valor: str = None):
    if not await check_ceo(ctx): return
    niveles_validos = ['BRONCE', 'PLATA', 'ORO', 'PLATINO', 'DIAMANTE', 'MASTER']
    if not nivel:
        await ctx.send("Uso: `!setnivel [nivel] [campo] [valor]`\nNiveles: BRONCE, PLATA, ORO, PLATINO, DIAMANTE, MASTER\nCampos: descuento, gratis_cada, gratis_cantidad, anticipo_horas, canal_vip, rifas_exclusivas")
        return
    nivel = nivel.upper()
    if nivel not in niveles_validos:
        await ctx.send(embed=embeds.crear_embed_error(f"Nivel inválido. Niveles: {', '.join(niveles_validos)}"))
        return
    campos = {'descuento': 'descuento', 'gratis_cada': 'boletos_gratis_por_cada', 'gratis_cantidad': 'cantidad_boletos_gratis', 'anticipo_horas': 'acceso_anticipado_horas', 'canal_vip': 'canal_vip', 'rifas_exclusivas': 'rifas_exclusivas'}
    if campo not in campos:
        await ctx.send(embed=embeds.crear_embed_error(f"Campo inválido. Campos: {', '.join(campos.keys())}"))
        return
    try:
        valor_int = int(valor)
    except:
        if campo in ['canal_vip', 'rifas_exclusivas']:
            valor_int = 1 if valor.lower() in ['si', 'true', '1', 'activo'] else 0
        else:
            await ctx.send(embed=embeds.crear_embed_error("Valor debe ser número"))
            return
    await db_pool.execute(f"UPDATE fidelizacion_config SET {campos[campo]} = ? WHERE nivel = ?", (valor_int, nivel))
    await ctx.message.delete()
    await ctx.send(embed=embeds.crear_embed_exito(f"Nivel {nivel}: {campo} = {valor_int}"))

# ============================================
# CASHBACK
# ============================================
@bot.command(name="cashback")
async def cmd_cashback(ctx):
    if not await verificar_canal(ctx): return
    row = await db_pool.fetchone("SELECT cashback_acumulado FROM cashback WHERE usuario_id = ?", (str(ctx.author.id),))
    cashback = row[0] if row else 0
    embed = discord.Embed(title="💰 Cashback", description=f"Acumulado: **${cashback} VP$**", color=config.COLORS['primary'])
    await ctx.send(embed=embed)

@bot.command(name="topcashback")
async def cmd_top_cashback(ctx):
    if not await verificar_canal(ctx): return
    rows = await db_pool.fetchall('''
        SELECT c.usuario_id, cl.nombre, c.cashback_acumulado FROM cashback c
        LEFT JOIN clientes cl ON c.usuario_id = cl.discord_id
        WHERE c.cashback_acumulado > 0 ORDER BY c.cashback_acumulado DESC LIMIT 10
    ''')
    if not rows:
        await ctx.send(embed=embeds.crear_embed_info("Sin datos", "No hay cashback"))
        return
    embed = discord.Embed(title="💰 TOP CASHBACK", color=config.COLORS['primary'])
    for i, u in enumerate(rows, 1):
        nombre = u['nombre'] or "Usuario"
        embed.add_field(name=f"{i}. {nombre}", value=f"**${u['cashback_acumulado']}**", inline=False)
    await ctx.send(embed=embed)

@bot.command(name="setcashback")
async def cmd_set_cashback(ctx, porcentaje: int):
    if not await check_ceo(ctx): return
    if porcentaje < 0 or porcentaje > 50:
        await ctx.send(embed=embeds.crear_embed_error("0-50%"))
        return
    global CASHBACK_PORCENTAJE
    CASHBACK_PORCENTAJE = porcentaje
    await db_pool.execute("UPDATE cashback_config SET porcentaje = ? WHERE id = 1", (porcentaje,))
    await ctx.message.delete()
    await ctx.send(embed=embeds.crear_embed_exito(f"Cashback: {porcentaje}%"))

@bot.command(name="pagarcashback")
async def cmd_pagar_cashback(ctx):
    if not await check_ceo(ctx): return
    await ctx.send("💰 Procesando pagos de cashback...")
    usuarios = await db_pool.fetchall("SELECT usuario_id, cashback_acumulado FROM cashback WHERE cashback_acumulado > 0")
    if not usuarios:
        await ctx.send(embed=embeds.crear_embed_error("No hay cashback para pagar"))
        return
    total_pagado = 0
    async with db_pool.connection() as conn:
        for u in usuarios:
            monto = u['cashback_acumulado']
            await conn.execute('''
                INSERT INTO usuarios_balance (discord_id, nombre, balance) VALUES (?, (SELECT nombre FROM clientes WHERE discord_id = ?), ?)
                ON CONFLICT(discord_id) DO UPDATE SET balance = balance + ?
            ''', (u['usuario_id'], u['usuario_id'], monto, monto))
            await conn.execute("INSERT INTO transacciones (tipo, monto, destino_id, descripcion) VALUES ('cashback', ?, ?, ?)", (monto, u['usuario_id'], "Pago de cashback"))
            await conn.execute("UPDATE cashback SET cashback_acumulado = 0, cashback_recibido = cashback_recibido + ? WHERE usuario_id = ?", (monto, u['usuario_id']))
            total_pagado += monto
            await enviar_dm(u['usuario_id'], "💰 Cashback pagado", f"Has recibido ${monto} VP$ por cashback")
        await conn.commit()
    await ctx.message.delete()
    await ctx.send(embed=embeds.crear_embed_exito(f"Pagados ${total_pagado} VP$ de cashback a {len(usuarios)} usuarios"))

@bot.command(name="resetcashback")
async def cmd_reset_cashback(ctx):
    if not await check_ceo(ctx): return
    await db_pool.execute("UPDATE cashback SET cashback_acumulado = 0")
    await ctx.message.delete()
    await ctx.send(embed=embeds.crear_embed_exito("Cashback reseteados"))

# ============================================
# VENDEDORES
# ============================================
@bot.command(name="vender")
async def cmd_vender(ctx, usuario: discord.Member, numero: int):
    if not await verificar_canal(ctx): return
    if not await check_vendedor(ctx):
        await ctx.send(embed=embeds.crear_embed_error("Sin permiso"))
        return
    rifa = await bot.db.get_rifa_activa()
    if not rifa:
        await ctx.send(embed=embeds.crear_embed_error("No hay rifa"))
        return
    if numero < 1 or numero > rifa['total_boletos']:
        await ctx.send(embed=embeds.crear_embed_error(f"Número entre 1-{rifa['total_boletos']}"))
        return
    disponibles = await bot.db.get_boletos_disponibles(rifa['id'])
    if numero not in disponibles:
        await ctx.send(embed=embeds.crear_embed_error(f"Número {numero} no disponible"))
        return
    balance_row = await db_pool.fetchone("SELECT balance FROM usuarios_balance WHERE discord_id = ?", (str(usuario.id),))
    balance = balance_row[0] if balance_row else 0
    descuento = await obtener_descuento_usuario(str(usuario.id))
    precio_final = int(rifa['precio_boleto'] * (100 - descuento) / 100)
    if balance < precio_final:
        await ctx.send(embed=embeds.crear_embed_error(f"Usuario necesita {precio_final} VP$"))
        return
    async with db_pool.connection() as conn:
        await conn.execute("UPDATE usuarios_balance SET balance = ? WHERE discord_id = ?", (balance - precio_final, str(usuario.id)))
        await conn.execute('INSERT INTO boletos (rifa_id, numero, comprador_id, comprador_nick, vendedor_id, precio_pagado) VALUES (?, ?, ?, ?, ?, ?)',
                           (rifa['id'], numero, str(usuario.id), usuario.name, str(ctx.author.id), rifa['precio_boleto']))
        await conn.commit()
    await actualizar_fidelizacion(str(usuario.id), precio_final)
    await aplicar_cashback(str(usuario.id), precio_final)
    await procesar_comision_referido(str(usuario.id), precio_final)
    await procesar_comision_vendedor(str(ctx.author.id), precio_final)
    await actualizar_jackpot(precio_final)
    await enviar_dm(str(usuario.id), "🎟️ Boleto comprado", f"Has comprado el boleto #{numero} por ${precio_final} VP$")
    await enviar_dm(str(ctx.author.id), "💰 Venta realizada", f"Has vendido el boleto #{numero} a {usuario.name} por ${precio_final} VP$")
    await ctx.message.delete()
    await ctx.send(embed=embeds.crear_embed_exito("✅ Venta realizada. Revisa tu DM."))

@bot.command(name="venderrandom")
async def cmd_vender_random(ctx, usuario: discord.Member, cantidad: int = 1):
    if not await verificar_canal(ctx): return
    if not await check_vendedor(ctx):
        await ctx.send(embed=embeds.crear_embed_error("Sin permiso"))
        return
    if cantidad < 1 or cantidad > 50:
        await ctx.send(embed=embeds.crear_embed_error("1-50 boletos"))
        return
    rifa = await bot.db.get_rifa_activa()
    if not rifa:
        await ctx.send(embed=embeds.crear_embed_error("No hay rifa"))
        return
    disponibles = await bot.db.get_boletos_disponibles(rifa['id'])
    if len(disponibles) < cantidad:
        await ctx.send(embed=embeds.crear_embed_error(f"Solo {len(disponibles)} disponibles"))
        return
    balance_row = await db_pool.fetchone("SELECT balance FROM usuarios_balance WHERE discord_id = ?", (str(usuario.id),))
    balance = balance_row[0] if balance_row else 0
    descuento = await obtener_descuento_usuario(str(usuario.id))
    boletos_a_pagar, boletos_a_recibir, precio_final = calcular_precio_con_2x1(cantidad, rifa['precio_boleto'], descuento)
    if balance < precio_final:
        await ctx.send(embed=embeds.crear_embed_error(f"Usuario necesita {precio_final} VP$"))
        return
    seleccionados = random.sample(disponibles, boletos_a_recibir)
    async with db_pool.connection() as conn:
        await conn.execute("UPDATE usuarios_balance SET balance = ? WHERE discord_id = ?", (balance - precio_final, str(usuario.id)))
        for num in seleccionados:
            await conn.execute('INSERT INTO boletos (rifa_id, numero, comprador_id, comprador_nick, vendedor_id, precio_pagado) VALUES (?, ?, ?, ?, ?, ?)',
                               (rifa['id'], num, str(usuario.id), usuario.name, str(ctx.author.id), rifa['precio_boleto']))
        await conn.commit()
    await actualizar_fidelizacion(str(usuario.id), precio_final)
    cashback = await aplicar_cashback(str(usuario.id), precio_final)
    await procesar_comision_referido(str(usuario.id), precio_final)
    await procesar_comision_vendedor(str(ctx.author.id), precio_final)
    await actualizar_jackpot(precio_final)
    await enviar_dm(str(usuario.id), "🎟️ Compra realizada", f"Has comprado {len(seleccionados)} boletos: {', '.join(map(str, seleccionados))}\nTotal: ${precio_final}\nDescuento: {descuento}%\nCashback: ${cashback}")
    await enviar_dm(str(ctx.author.id), "💰 Venta realizada", f"Has vendido {len(seleccionados)} boletos a {usuario.name} por ${precio_final}")
    await ctx.message.delete()
    await ctx.send(embed=embeds.crear_embed_exito("✅ Venta realizada. Revisa tu DM."))

@bot.command(name="misventas")
async def cmd_mis_ventas(ctx):
    if not await verificar_canal(ctx): return
    if not await check_vendedor(ctx):
        await ctx.send(embed=embeds.crear_embed_error("Sin permiso"))
        return
    ventas = await db_pool.fetchall('''
        SELECT b.numero, r.nombre as rifa, b.comprador_nick, b.precio_pagado, b.fecha_compra
        FROM boletos b JOIN rifas r ON b.rifa_id = r.id
        WHERE b.vendedor_id = ? ORDER BY b.fecha_compra DESC LIMIT 20
    ''', (str(ctx.author.id),))
    vendedor = await db_pool.fetchone("SELECT comisiones_pendientes FROM vendedores WHERE discord_id = ?", (str(ctx.author.id),))
    embed = discord.Embed(title="💰 Tus ventas", color=config.COLORS['primary'])
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
    if not await verificar_canal(ctx): return
    if not await check_vendedor(ctx):
        await ctx.send(embed=embeds.crear_embed_error("Sin permiso"))
        return
    rifa = await bot.db.get_rifa_activa()
    if not rifa:
        await ctx.send(embed=embeds.crear_embed_error("No hay rifa"))
        return
    disponibles = await bot.db.get_boletos_disponibles(rifa['id'])
    vendidos = await bot.db.get_boletos_vendidos(rifa['id'])
    embed = discord.Embed(title=f"📋 Boletos - {rifa['nombre']}", description=f"**Total:** {rifa['total_boletos']}\n**Vendidos:** {vendidos}\n**Disponibles:** {len(disponibles)}", color=config.COLORS['info'])
    await ctx.send(embed=embed)

# ============================================
# ADMIN / DIRECTOR
# ============================================
@bot.command(name="crearifa")
async def cmd_crear_rifa(ctx, premio: str, precio: int, total: int):
    if not await check_admin(ctx): return
    nombre = f"Rifa {datetime.now().strftime('%d/%m')}"
    rifa_id = await bot.db.crear_rifa(nombre, premio, precio, precio, total)
    global ultima_rifa
    rifa_activa = await bot.db.get_rifa_activa()
    if rifa_activa:
        ultima_rifa = rifa_activa
    embed = embeds.crear_embed_exito(f"✅ Rifa creada ID: {rifa_id}\nPremio: {premio}\nPrecio: ${precio}\nTotal: {total} boletos")
    await ctx.send(embed=embed)

@bot.command(name="setcomision")
async def cmd_set_comision(ctx, porcentaje: int):
    if not await check_admin(ctx): return
    if porcentaje < 0 or porcentaje > 30:
        await ctx.send(embed=embeds.crear_embed_error("0-30%"))
        return
    global COMISION_VENDEDOR
    COMISION_VENDEDOR = porcentaje
    await ctx.message.delete()
    await ctx.send(embed=embeds.crear_embed_exito(f"Comisión vendedores: {porcentaje}%"))

@bot.command(name="vercomisiones")
async def cmd_ver_comisiones(ctx):
    if not await check_admin(ctx): return
    vendedores = await db_pool.fetchall("SELECT nombre, comisiones_pendientes FROM vendedores WHERE comisiones_pendientes > 0")
    if not vendedores:
        await ctx.send(embed=embeds.crear_embed_info("Info", "No hay comisiones pendientes"))
        return
    embed = discord.Embed(title="💰 Comisiones pendientes", color=config.COLORS['primary'])
    for v in vendedores:
        embed.add_field(name=v['nombre'], value=f"${v['comisiones_pendientes']}", inline=True)
    await ctx.send(embed=embed)

@bot.command(name="pagarcomisiones")
async def cmd_pagar_comisiones(ctx):
    if not await check_admin(ctx): return
    await ctx.send("💰 Procesando pagos de comisiones...")
    vendedores = await db_pool.fetchall("SELECT discord_id, nombre, comisiones_pendientes FROM vendedores WHERE comisiones_pendientes > 0")
    if not vendedores:
        await ctx.send(embed=embeds.crear_embed_error("No hay comisiones pendientes"))
        return
    total_pagado = 0
    async with db_pool.connection() as conn:
        for v in vendedores:
            monto = v['comisiones_pendientes']
            await conn.execute('''
                INSERT INTO usuarios_balance (discord_id, nombre, balance) VALUES (?, ?, ?)
                ON CONFLICT(discord_id) DO UPDATE SET balance = balance + ?
            ''', (v['discord_id'], v['nombre'], monto, monto))
            await conn.execute("INSERT INTO transacciones (tipo, monto, destino_id, descripcion) VALUES ('comision', ?, ?, ?)", (monto, v['discord_id'], "Pago de comisiones"))
            await conn.execute("UPDATE vendedores SET comisiones_pendientes = 0, comisiones_pagadas = comisiones_pagadas + ? WHERE discord_id = ?", (monto, v['discord_id']))
            total_pagado += monto
            await enviar_dm(v['discord_id'], "💰 Comisiones pagadas", f"Has recibido ${monto} VP$ por tus ventas")
        await conn.commit()
    await ctx.message.delete()
    await ctx.send(embed=embeds.crear_embed_exito(f"Pagadas ${total_pagado} VP$ en comisiones a {len(vendedores)} vendedores"))

@bot.command(name="aumentarnumeros")
async def cmd_aumentar_numeros(ctx, cantidad: int):
    if not await check_admin(ctx): return
    rifa = await bot.db.get_rifa_activa()
    if not rifa:
        await ctx.send(embed=embeds.crear_embed_error("No hay rifa"))
        return
    if cantidad <= 0:
        await ctx.send(embed=embeds.crear_embed_error("Cantidad positiva"))
        return
    nuevo_total = rifa['total_boletos'] + cantidad
    await db_pool.execute("UPDATE rifas SET total_boletos = ? WHERE id = ?", (nuevo_total, rifa['id']))
    await ctx.message.delete()
    await ctx.send(embed=embeds.crear_embed_exito(f"✅ Total: {nuevo_total} boletos"))

@bot.command(name="cerrarifa")
async def cmd_cerrar_rifa(ctx):
    if not await check_admin(ctx): return
    rifa = await bot.db.get_rifa_activa()
    if not rifa:
        await ctx.send(embed=embeds.crear_embed_error("No hay rifa activa"))
        return
    if rifa['estado'] != 'activa':
        await ctx.send(embed=embeds.crear_embed_error("La rifa ya está cerrada o finalizada"))
        return
    # Verificar si ya se asignaron puntos
    row = await db_pool.fetchone("SELECT puntos_asignados FROM rifas WHERE id = ?", (rifa['id'],))
    if row and row['puntos_asignados']:
        await ctx.send(embed=embeds.crear_embed_error("Los puntos de revancha ya fueron asignados para esta rifa"))
        return
    # Cambiar estado
    await bot.db.cerrar_rifa(rifa['id'])
    # Asignar puntos revancha
    compradores = await db_pool.fetchall("SELECT comprador_id, COUNT(*) as boletos FROM boletos WHERE rifa_id = ? AND estado = 'pagado' GROUP BY comprador_id", (rifa['id'],))
    async with db_pool.connection() as conn:
        for c in compradores:
            await conn.execute('''
                INSERT INTO puntos_revancha (usuario_id, puntos) VALUES (?, ?)
                ON CONFLICT(usuario_id) DO UPDATE SET puntos = puntos + ?
            ''', (c['comprador_id'], c['boletos'], c['boletos']))
        await conn.execute("UPDATE rifas SET puntos_asignados = 1 WHERE id = ?", (rifa['id'],))
    await ctx.message.delete()
    await ctx.send(embed=embeds.crear_embed_exito("✅ Rifa cerrada y puntos de revancha asignados"))

@bot.command(name="iniciarsorteo")
async def cmd_iniciar_sorteo(ctx, ganadores: int = 1):
    global sorteo_en_curso, sorteo_cancelado
    if not await check_admin(ctx): return
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
    boletos = await db_pool.fetchall("SELECT numero, comprador_id, comprador_nick FROM boletos WHERE rifa_id = ? AND estado = 'pagado'", (rifa['id'],))
    if len(boletos) <= ganadores:
        ganadores_sel = boletos
    else:
        ganadores_sel = random.sample(boletos, ganadores)
    embed = discord.Embed(title="🎉 Ganadores", color=config.COLORS['success'])
    for i, b in enumerate(ganadores_sel, 1):
        embed.add_field(name=f"#{i}", value=f"#{b['numero']} - {b['comprador_nick']}", inline=False)
        await enviar_dm(b['comprador_id'], "🎉 ¡FELICIDADES! GANASTE", f"Has ganado la rifa {rifa['nombre']} con el boleto #{b['numero']}\nPremio: {rifa['premio']}")
        await registrar_ganador_historico(b['comprador_id'], b['comprador_nick'], rifa['premio'], rifa['nombre'], b['numero'])
    await ctx.send(embed=embed)
    sorteo_en_curso = False

@bot.command(name="cancelarsorteo")
async def cmd_cancelar_sorteo(ctx):
    global sorteo_en_curso, sorteo_cancelado
    if not await check_admin(ctx): return
    if not sorteo_en_curso:
        await ctx.send(embed=embeds.crear_embed_error("No hay sorteo"))
        return
    sorteo_cancelado = True
    sorteo_en_curso = False
    await ctx.message.delete()
    await ctx.send(embed=embeds.crear_embed_exito("Sorteo cancelado"))

@bot.command(name="finalizarrifa")
async def cmd_finalizar_rifa(ctx, id_rifa: int = None, ganadores: int = 1):
    if not await check_admin(ctx): return
    if id_rifa is None:
        rifa = await bot.db.get_rifa_activa()
        if not rifa:
            await ctx.send(embed=embeds.crear_embed_error("Especifica ID"))
            return
        id_rifa = rifa['id']
    rifa = await db_pool.fetchone("SELECT * FROM rifas WHERE id = ?", (id_rifa,))
    if not rifa:
        await ctx.send(embed=embeds.crear_embed_error("ID inválido"))
        return
    boletos = await db_pool.fetchall("SELECT numero, comprador_id, comprador_nick FROM boletos WHERE rifa_id = ? AND estado = 'pagado'", (id_rifa,))
    if not boletos:
        await ctx.send(embed=embeds.crear_embed_error("Sin boletos"))
        return
    if len(boletos) < ganadores:
        await ctx.send(embed=embeds.crear_embed_error(f"Solo {len(boletos)} boletos"))
        return
    ganadores_sel = random.sample(boletos, ganadores)
    embed = discord.Embed(title=f"🎉 Rifa #{id_rifa} finalizada", description=f"Premio: {rifa['premio']}", color=config.COLORS['success'])
    for i, b in enumerate(ganadores_sel, 1):
        embed.add_field(name=f"Ganador {i}", value=f"#{b['numero']} - {b['comprador_nick']}", inline=False)
        await enviar_dm(b['comprador_id'], "🎉 ¡FELICIDADES! GANASTE", f"Has ganado la rifa {rifa['nombre']} (ID: {id_rifa}) con el boleto #{b['numero']}\nPremio: {rifa['premio']}")
        await registrar_ganador_historico(b['comprador_id'], b['comprador_nick'], rifa['premio'], rifa['nombre'], b['numero'])
    await ctx.send(embed=embed)

@bot.command(name="vendedoradd")
async def cmd_vendedor_add(ctx, usuario: discord.Member, comision: int = 10):
    if not await check_admin(ctx): return
    await db_pool.execute('''
        INSERT INTO vendedores (discord_id, nombre, comision) VALUES (?, ?, ?)
        ON CONFLICT(discord_id) DO UPDATE SET comision = ?, nombre = ?
    ''', (str(usuario.id), usuario.name, comision, comision, usuario.name))
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
    if not await check_admin(ctx): return
    rifa = await bot.db.get_rifa_activa()
    if not rifa:
        await ctx.send(embed=embeds.crear_embed_error("No hay rifa"))
        return
    vendidos = await bot.db.get_boletos_vendidos(rifa['id'])
    recaudado = vendidos * rifa['precio_boleto']
    embed = discord.Embed(title=f"📊 Reporte - {rifa['nombre']}", description=f"**Vendidos:** {vendidos}/{rifa['total_boletos']}\n**Recaudado:** ${recaudado}", color=config.COLORS['primary'])
    await ctx.send(embed=embed)

@bot.command(name="alertar")
@commands.cooldown(1, 300, commands.BucketType.guild)
async def cmd_alertar(ctx, *, mensaje: str):
    if not await check_admin(ctx): return
    embed = discord.Embed(title="📢 ALERTA DE RIFAS", description=mensaje, color=config.COLORS['primary'])
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
# CEO
# ============================================
@bot.command(name="acreditarvp")
async def cmd_acreditarvp(ctx, usuario: discord.Member, cantidad: int):
    if not await check_ceo(ctx): return
    if cantidad <= 0:
        await ctx.send(embed=embeds.crear_embed_error("Cantidad positiva"))
        return
    await db_pool.execute('''
        INSERT INTO usuarios_balance (discord_id, nombre, balance) VALUES (?, ?, ?)
        ON CONFLICT(discord_id) DO UPDATE SET balance = balance + ?
    ''', (str(usuario.id), usuario.name, cantidad, cantidad))
    await db_pool.execute("INSERT INTO transacciones (tipo, monto, destino_id, descripcion) VALUES ('acreditar', ?, ?, ?)", (cantidad, str(usuario.id), f"Acreditación por {ctx.author.name}"))
    await enviar_dm(str(usuario.id), "💰 Acreditación de VP$", f"Se te han acreditado ${cantidad} VP$ en tu balance")
    await ctx.message.delete()
    await ctx.send(embed=embeds.crear_embed_exito(f"Acreditados ${cantidad} VP$ a {usuario.name}"))

@bot.command(name="retirarvp")
async def cmd_retirarvp(ctx, usuario: discord.Member, cantidad: int):
    if not await check_ceo(ctx): return
    if cantidad <= 0:
        await ctx.send(embed=embeds.crear_embed_error("Cantidad positiva"))
        return
    row = await db_pool.fetchone("SELECT balance FROM usuarios_balance WHERE discord_id = ?", (str(usuario.id),))
    if not row or row[0] < cantidad:
        await ctx.send(embed=embeds.crear_embed_error("Saldo insuficiente"))
        return
    await db_pool.execute("UPDATE usuarios_balance SET balance = balance - ? WHERE discord_id = ?", (cantidad, str(usuario.id)))
    await db_pool.execute("INSERT INTO transacciones (tipo, monto, origen_id, descripcion) VALUES ('retirar', ?, ?, ?)", (cantidad, str(usuario.id), f"Retiro por {ctx.author.name}"))
    await enviar_dm(str(usuario.id), "💰 Retiro de VP$", f"Se te han retirado ${cantidad} VP$ de tu balance")
    await ctx.message.delete()
    await ctx.send(embed=embeds.crear_embed_exito(f"Retirados ${cantidad} VP$ de {usuario.name}"))

@bot.command(name="estadisticas")
async def cmd_estadisticas(ctx):
    if not await check_ceo(ctx): return
    total_rifas = (await db_pool.fetchone("SELECT COUNT(*) FROM rifas"))[0]
    total_boletos = (await db_pool.fetchone("SELECT COUNT(*) FROM boletos"))[0]
    total_recaudado = (await db_pool.fetchone("SELECT SUM(precio_pagado) FROM boletos"))[0] or 0
    total_clientes = (await db_pool.fetchone("SELECT COUNT(*) FROM clientes"))[0]
    total_vp = (await db_pool.fetchone("SELECT SUM(balance) FROM usuarios_balance"))[0] or 0
    total_cashback = (await db_pool.fetchone("SELECT SUM(cashback_acumulado) FROM cashback"))[0] or 0
    total_comisiones = (await db_pool.fetchone("SELECT SUM(comisiones_pendientes) FROM vendedores"))[0] or 0
    embed = discord.Embed(title="📊 ESTADÍSTICAS GLOBALES", color=config.COLORS['primary'])
    embed.add_field(name="🎟️ Total rifas", value=f"**{total_rifas}**", inline=True)
    embed.add_field(name="🎲 Boletos vendidos", value=f"**{total_boletos}**", inline=True)
    embed.add_field(name="💰 Total recaudado", value=f"**${total_recaudado}**", inline=True)
    embed.add_field(name="👥 Clientes", value=f"**{total_clientes}**", inline=True)
    embed.add_field(name="💵 VP$ en circulación", value=f"**${total_vp}**", inline=True)
    embed.add_field(name="💸 Cashback pendiente", value=f"**${total_cashback}**", inline=True)
    embed.add_field(name="🏦 Comisiones pendientes", value=f"**${total_comisiones}**", inline=True)
    await ctx.send(embed=embed)

@bot.command(name="auditoria")
async def cmd_auditoria(ctx):
    if not await check_ceo(ctx): return
    transacciones = await db_pool.fetchall("SELECT tipo, monto, descripcion, fecha FROM transacciones ORDER BY fecha DESC LIMIT 20")
    if not transacciones:
        await ctx.send(embed=embeds.crear_embed_info("Sin datos", "No hay transacciones"))
        return
    embed = discord.Embed(title="📋 AUDITORÍA", color=config.COLORS['primary'])
    for t in transacciones[:15]:
        embed.add_field(name=f"{t['fecha'][:10]} - {t['tipo']}", value=f"${t['monto']} - {t['descripcion']}", inline=False)
    await ctx.send(embed=embed)

@bot.command(name="exportar")
async def cmd_exportar(ctx):
    if not await check_ceo(ctx): return
    fecha = datetime.now().strftime("%Y%m%d_%H%M%S")
    archivo = f"/tmp/vp_rifas_{fecha}.csv"
    boletos = await db_pool.fetchall('''
        SELECT b.numero, r.nombre as rifa, b.comprador_nick, b.precio_pagado, b.fecha_compra
        FROM boletos b JOIN rifas r ON b.rifa_id = r.id ORDER BY b.fecha_compra DESC
    ''')
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
    if not await check_ceo(ctx): return
    fecha = datetime.now().strftime("%Y%m%d_%H%M%S")
    backup = f"/tmp/backup_{fecha}.db"
    shutil.copy2(config.DB_PATH, backup)
    await ctx.author.send(file=discord.File(backup))
    await ctx.message.delete()
    await ctx.send(embed=embeds.crear_embed_exito("✅ Backup creado. Revisa tu DM."))

@bot.command(name="resetallsistema")
async def cmd_reset_all_sistema(ctx):
    global reset_pending
    if not await check_ceo(ctx): return
    embed = discord.Embed(title="⚠️ REINICIO TOTAL DEL SISTEMA", description="Esto borrará TODOS los datos.\nEscribe `!confirmarreset` en 30 segundos.", color=config.COLORS['error'])
    await ctx.send(embed=embed)
    reset_pending = {'usuario_id': ctx.author.id, 'timestamp': datetime.now().timestamp()}

@bot.command(name="confirmarreset")
async def cmd_confirmar_reset(ctx):
    global reset_pending
    if not await check_ceo(ctx): return
    if not reset_pending or reset_pending.get('usuario_id') != ctx.author.id:
        await ctx.send(embed=embeds.crear_embed_error("Sin solicitud pendiente"))
        return
    if datetime.now().timestamp() - reset_pending['timestamp'] > 30:
        reset_pending = None
        await ctx.send(embed=embeds.crear_embed_error("Tiempo expirado"))
        return
    await ctx.send("🔄 REINICIANDO SISTEMA...")
    async with db_pool.connection() as conn:
        await conn.execute("DELETE FROM transacciones")
        await conn.execute("DELETE FROM boletos")
        await conn.execute("DELETE FROM vendedores")
        await conn.execute("DELETE FROM clientes")
        await conn.execute("DELETE FROM usuarios_balance")
        await conn.execute("DELETE FROM rifas")
        await conn.execute("DELETE FROM referidos_codigos")
        await conn.execute("DELETE FROM referidos_relaciones")
        await conn.execute("DELETE FROM fidelizacion")
        await conn.execute("DELETE FROM cashback")
        await conn.execute("DELETE FROM codigos_promocionales")
        await conn.execute("DELETE FROM codigos_canjeados")
        await conn.execute("DELETE FROM puntos_revancha")
        await conn.execute("DELETE FROM sqlite_sequence")
        await conn.commit()
    reset_pending = None
    await ctx.message.delete()
    await ctx.send(embed=embeds.crear_embed_exito("✅ Sistema reiniciado correctamente"))

# ============================================
# PROMOCIONES
# ============================================
@bot.command(name="crearcodigo")
async def cmd_crear_codigo(ctx, codigo: str, recompensa: int):
    if not await check_ceo(ctx): return
    codigo = codigo.lower()
    existe = await db_pool.fetchone("SELECT * FROM codigos_promocionales WHERE codigo = ?", (codigo,))
    if existe:
        await ctx.send(embed=embeds.crear_embed_error("El código ya existe"))
        return
    await db_pool.execute("INSERT INTO codigos_promocionales (codigo, recompensa, creador_id) VALUES (?, ?, ?)", (codigo, recompensa, str(ctx.author.id)))
    await ctx.message.delete()
    await ctx.send(embed=embeds.crear_embed_exito(f"Código `{codigo}` creado con {recompensa} VP$"))

@bot.command(name="borrarcodigo")
async def cmd_borrar_codigo(ctx, codigo: str):
    if not await check_ceo(ctx): return
    codigo = codigo.lower()
    await db_pool.execute("DELETE FROM codigos_promocionales WHERE codigo = ?", (codigo,))
    await ctx.message.delete()
    await ctx.send(embed=embeds.crear_embed_exito(f"Código `{codigo}` eliminado"))

@bot.command(name="canjear")
async def cmd_canjear(ctx, codigo: str):
    if not await verificar_canal(ctx): return
    codigo = codigo.lower()
    codigo_data = await db_pool.fetchone("SELECT recompensa FROM codigos_promocionales WHERE codigo = ? AND activo = 1", (codigo,))
    if not codigo_data:
        await ctx.send(embed=embeds.crear_embed_error("Código inválido o expirado"))
        return
    ya = await db_pool.fetchone("SELECT * FROM codigos_canjeados WHERE codigo = ? AND usuario_id = ?", (codigo, str(ctx.author.id)))
    if ya:
        await ctx.send(embed=embeds.crear_embed_error("Ya has canjeado este código"))
        return
    recompensa = codigo_data[0]
    await db_pool.execute("INSERT INTO codigos_canjeados (codigo, usuario_id) VALUES (?, ?)", (codigo, str(ctx.author.id)))
    await db_pool.execute('''
        INSERT INTO usuarios_balance (discord_id, nombre, balance) VALUES (?, ?, ?)
        ON CONFLICT(discord_id) DO UPDATE SET balance = balance + ?
    ''', (str(ctx.author.id), ctx.author.name, recompensa, recompensa))
    await enviar_dm(str(ctx.author.id), "🎁 Código canjeado", f"Has canjeado el código `{codigo}` y recibido ${recompensa} VP$")
    await ctx.message.delete()
    await ctx.send(embed=embeds.crear_embed_exito("✅ Código canjeado. Revisa tu DM."))

# ============================================
# EVENTOS
# ============================================
@bot.command(name="2x1")
async def cmd_2x1(ctx):
    if not await check_ceo(ctx): return
    global evento_2x1, eventos_activos
    evento_2x1 = not evento_2x1
    eventos_activos['2x1'] = evento_2x1
    await db_pool.execute("UPDATE eventos_config SET evento_2x1 = ? WHERE id = 1", (evento_2x1,))
    estado = "ACTIVADO" if evento_2x1 else "DESACTIVADO"
    await ctx.message.delete()
    await ctx.send(embed=embeds.crear_embed_exito(f"🎉 Evento 2x1 {estado}"))

@bot.command(name="cashbackdoble")
async def cmd_cashback_doble(ctx):
    if not await check_ceo(ctx): return
    global evento_cashback_doble, eventos_activos
    evento_cashback_doble = not evento_cashback_doble
    eventos_activos['cashback_doble'] = evento_cashback_doble
    await db_pool.execute("UPDATE eventos_config SET cashback_doble = ? WHERE id = 1", (evento_cashback_doble,))
    estado = "ACTIVADO" if evento_cashback_doble else "DESACTIVADO"
    await ctx.message.delete()
    await ctx.send(embed=embeds.crear_embed_exito(f"🎉 Cashback doble {estado}"))

@bot.command(name="oferta")
async def cmd_oferta(ctx, porcentaje: int):
    if not await check_ceo(ctx): return
    if porcentaje < 0 or porcentaje > 30:
        await ctx.send(embed=embeds.crear_embed_error("0-30%"))
        return
    global evento_oferta_activa, evento_oferta_porcentaje, eventos_activos
    evento_oferta_activa = True
    evento_oferta_porcentaje = porcentaje
    eventos_activos['oferta_porcentaje'] = porcentaje
    await db_pool.execute("UPDATE eventos_config SET oferta_activa = 1, oferta_porcentaje = ? WHERE id = 1", (porcentaje,))
    await ctx.message.delete()
    await ctx.send(embed=embeds.crear_embed_exito(f"🎉 Oferta activada: {porcentaje}% de descuento adicional"))

@bot.command(name="ofertadesactivar")
async def cmd_oferta_desactivar(ctx):
    if not await check_ceo(ctx): return
    global evento_oferta_activa, evento_oferta_porcentaje, eventos_activos
    evento_oferta_activa = False
    evento_oferta_porcentaje = 0
    eventos_activos['oferta_porcentaje'] = 0
    await db_pool.execute("UPDATE eventos_config SET oferta_activa = 0, oferta_porcentaje = 0 WHERE id = 1")
    await ctx.message.delete()
    await ctx.send(embed=embeds.crear_embed_exito("🎉 Oferta desactivada"))

# ============================================
# JACKPOT
# ============================================
@bot.command(name="jackpot")
async def cmd_jackpot(ctx, base: int, porcentaje: int, id_rifa: int):
    if not await check_ceo(ctx): return
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
    await db_pool.execute("INSERT OR REPLACE INTO jackpot (id, rifa_id, base, porcentaje, total, activo) VALUES (1, ?, ?, ?, ?, 1)", (id_rifa, base, porcentaje, base))
    await ctx.message.delete()
    await ctx.send(embed=embeds.crear_embed_exito(f"🎰 Jackpot activado!\nBase: ${base}\n{porcentaje}% de cada compra"))

@bot.command(name="jackpotreset")
async def cmd_jackpot_reset(ctx):
    if not await check_ceo(ctx): return
    global jackpot_activo, jackpot_total, jackpot_base, jackpot_porcentaje, jackpot_rifa_id
    jackpot_activo = False
    jackpot_total = 0
    jackpot_base = 0
    jackpot_porcentaje = 0
    jackpot_rifa_id = 0
    await db_pool.execute("UPDATE jackpot SET activo = 0 WHERE id = 1")
    await ctx.message.delete()
    await ctx.send(embed=embeds.crear_embed_exito("🎰 Jackpot resetado"))

@bot.command(name="jackpotsortear")
async def cmd_jackpot_sortear(ctx, ganadores: int = 1):
    if not await check_ceo(ctx): return
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
    boletos = await db_pool.fetchall("SELECT comprador_id, comprador_nick FROM boletos WHERE rifa_id = ? AND estado = 'pagado'", (jackpot_rifa_id,))
    if len(boletos) == 0:
        await ctx.send(embed=embeds.crear_embed_error("No hay boletos vendidos"))
        return
    premio_por_ganador = jackpot_total // ganadores
    ganadores_sel = random.sample(boletos, min(ganadores, len(boletos)))
    embed = discord.Embed(title="🎰 JACKPOT SORTEADO", description=f"Premio total: **${jackpot_total}**", color=config.COLORS['success'])
    for i, g in enumerate(ganadores_sel, 1):
        embed.add_field(name=f"🏆 Ganador #{i}", value=f"{g['comprador_nick']} | ${premio_por_ganador}", inline=False)
        await db_pool.execute('''
            INSERT INTO usuarios_balance (discord_id, nombre, balance) VALUES (?, ?, ?)
            ON CONFLICT(discord_id) DO UPDATE SET balance = balance + ?
        ''', (g['comprador_id'], g['comprador_nick'], premio_por_ganador, premio_por_ganador))
        await enviar_dm(g['comprador_id'], "🎰 ¡GANASTE EL JACKPOT!", f"Has ganado ${premio_por_ganador} VP$ del jackpot")
    await ctx.send(embed=embed)
    jackpot_activo = False
    jackpot_total = 0

# ============================================
# RIFA ELIMINACIÓN
# ============================================
@bot.command(name="rifaeliminacion")
async def cmd_rifa_eliminacion(ctx, total: int, premio: str, valor: int):
    if not await check_admin(ctx): return
    global rifa_eliminacion_activa, rifa_eliminacion_total, rifa_eliminacion_premio, rifa_eliminacion_valor, rifa_eliminacion_numeros
    if total <= 0 or valor <= 0:
        await ctx.send(embed=embeds.crear_embed_error("Valores inválidos"))
        return
    rifa_eliminacion_activa = True
    rifa_eliminacion_total = total
    rifa_eliminacion_premio = premio
    rifa_eliminacion_valor = valor
    rifa_eliminacion_numeros = list(range(1, total + 1))
    embed = discord.Embed(title="🔪 RIFA ELIMINACIÓN INICIADA", description=f"Premio: {premio}\nValor: ${valor}\nTotal: {total} boletos\n¡El último número que quede GANA!", color=config.COLORS['primary'])
    await ctx.send(embed=embed)

@bot.command(name="rifaeliminacionr")
async def cmd_rifa_eliminacion_eliminar(ctx, numero: int):
    if not await check_admin(ctx): return
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
    if not await verificar_canal(ctx): return
    global rifa_eliminacion_activa, rifa_eliminacion_numeros, rifa_eliminacion_valor
    if not rifa_eliminacion_activa:
        await ctx.send(embed=embeds.crear_embed_error("No hay rifa eliminación activa"))
        return
    if numero not in rifa_eliminacion_numeros:
        await ctx.send(embed=embeds.crear_embed_error(f"Número {numero} no disponible"))
        return
    balance = await db_pool.fetchone("SELECT balance FROM usuarios_balance WHERE discord_id = ?", (str(ctx.author.id),))
    balance = balance[0] if balance else 0
    if balance < rifa_eliminacion_valor:
        await ctx.send(embed=embeds.crear_embed_error(f"Necesitas ${rifa_eliminacion_valor} VP$"))
        return
    nuevo_balance = balance - rifa_eliminacion_valor
    async with db_pool.connection() as conn:
        await conn.execute("UPDATE usuarios_balance SET balance = ? WHERE discord_id = ?", (nuevo_balance, str(ctx.author.id)))
        await conn.execute("INSERT INTO transacciones (tipo, monto, origen_id, descripcion) VALUES ('eliminacion', ?, ?, ?)", (rifa_eliminacion_valor, str(ctx.author.id), f"Compra número {numero} en rifa eliminación"))
        await conn.commit()
    rifa_eliminacion_numeros.remove(numero)
    if len(rifa_eliminacion_numeros) == 1:
        ganador_numero = rifa_eliminacion_numeros[0]
        embed = discord.Embed(title="🏆 RIFA ELIMINACIÓN FINALIZADA", description=f"¡El número **{ganador_numero}** sobrevivió!\n**Ganador:** <@{ctx.author.id}>\n**Premio:** {rifa_eliminacion_premio}", color=config.COLORS['success'])
        await ctx.send(embed=embed)
        await enviar_dm(str(ctx.author.id), "🏆 ¡GANASTE LA RIFA ELIMINACIÓN!", f"Has ganado la rifa eliminación con el número #{ganador_numero}\nPremio: {rifa_eliminacion_premio}")
        rifa_eliminacion_activa = False
    else:
        await ctx.message.delete()
        await ctx.send(embed=embeds.crear_embed_exito(f"✅ Has comprado el número #{numero} por ${rifa_eliminacion_valor}\nQuedan {len(rifa_eliminacion_numeros)} números."))
        await enviar_dm(str(ctx.author.id), "🎟️ Compra en Rifa Eliminación", f"Has comprado el número #{numero} por ${rifa_eliminacion_valor}")

@bot.command(name="beliminacion")
async def cmd_ver_eliminacion(ctx):
    if not await verificar_canal(ctx): return
    global rifa_eliminacion_activa, rifa_eliminacion_numeros, rifa_eliminacion_total
    if not rifa_eliminacion_activa:
        await ctx.send(embed=embeds.crear_embed_error("No hay rifa eliminación activa"))
        return
    embed = discord.Embed(title="🔪 RIFA ELIMINACIÓN", description=f"**Boletos disponibles:** {len(rifa_eliminacion_numeros)}/{rifa_eliminacion_total}\n**Precio:** ${rifa_eliminacion_valor}", color=config.COLORS['info'])
    await ctx.send(embed=embed)

# ============================================
# PUNTOS REVANCHA
# ============================================
@bot.command(name="mispuntos")
async def cmd_mis_puntos(ctx):
    if not await verificar_canal(ctx): return
    row = await db_pool.fetchone("SELECT puntos FROM puntos_revancha WHERE usuario_id = ?", (str(ctx.author.id),))
    puntos = row[0] if row else 0
    embed = discord.Embed(title="🔄 Puntos de Revancha", description=f"Tienes **{puntos}** puntos", color=config.COLORS['primary'])
    embed.set_footer(text="Se acumulan por boletos perdidos")
    await ctx.send(embed=embed)

@bot.command(name="puntosreset")
async def cmd_puntos_reset(ctx, usuario: discord.Member = None):
    if not await check_ceo(ctx): return
    if usuario:
        await db_pool.execute("DELETE FROM puntos_revancha WHERE usuario_id = ?", (str(usuario.id),))
        await ctx.send(embed=embeds.crear_embed_exito(f"Puntos de {usuario.name} reseteados"))
    else:
        await db_pool.execute("DELETE FROM puntos_revancha")
        await ctx.send(embed=embeds.crear_embed_exito("Todos los puntos reseteados"))
    await ctx.message.delete()

@bot.command(name="canjearpuntos")
async def cmd_canjear_puntos(ctx, puntos: int):
    if not await verificar_canal(ctx): return
    if puntos <= 0:
        await ctx.send(embed=embeds.crear_embed_error("Cantidad positiva"))
        return
    row = await db_pool.fetchone("SELECT puntos FROM puntos_revancha WHERE usuario_id = ?", (str(ctx.author.id),))
    if not row or row['puntos'] < puntos:
        await ctx.send(embed=embeds.crear_embed_error("No tienes suficientes puntos"))
        return
    vp = puntos * config.PUNTOS_REVANCHA_TASA
    async with db_pool.connection() as conn:
        await conn.execute("UPDATE puntos_revancha SET puntos = puntos - ? WHERE usuario_id = ?", (puntos, str(ctx.author.id)))
        await conn.execute('''
            INSERT INTO usuarios_balance (discord_id, nombre, balance) VALUES (?, ?, ?)
            ON CONFLICT(discord_id) DO UPDATE SET balance = balance + ?
        ''', (str(ctx.author.id), ctx.author.name, vp, vp))
        await conn.execute("INSERT INTO transacciones (tipo, monto, destino_id, descripcion) VALUES ('canje_puntos', ?, ?, ?)", (vp, str(ctx.author.id), f"Canje de {puntos} puntos"))
    await ctx.send(embed=embeds.crear_embed_exito(f"Canjeaste {puntos} puntos por {vp} VP$"))

# ============================================
# MANEJADOR DE ERRORES GLOBAL
# ============================================
@bot.event
async def on_command_error(ctx, error):
    if isinstance(error, commands.CommandOnCooldown):
        await ctx.send(f"⏳ Espera {error.retry_after:.1f} segundos antes de usar este comando.")
    elif isinstance(error, commands.MissingPermissions):
        await ctx.send("❌ No tienes permiso para usar este comando.")
    else:
        logger.error(f"Error en comando {ctx.command}: {error}")
        await ctx.send("❌ Ocurrió un error inesperado. Contacta al administrador.")

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
