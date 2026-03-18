import discord
import config

def crear_embed_rifa(rifa, vendidos, disponibles):
    embed = discord.Embed(
        title=f"🎟️ {rifa['nombre']}",
        description=rifa['premio'],
        color=config.COLORS['primary']
    )
    return embed

def crear_embed_error(mensaje):
    return discord.Embed(
        title="❌ Error",
        description=mensaje,
        color=config.COLORS['error']
    )

def crear_embed_exito(mensaje):
    return discord.Embed(
        title="✅ Éxito",
        description=mensaje,
        color=config.COLORS['success']
    )

def crear_embed_info(titulo, mensaje):
    return discord.Embed(
        title=titulo,
        description=mensaje,
        color=config.COLORS['info']
    )
