import discord
import config
from datetime import datetime

def crear_embed_rifa(rifa, vendidos, disponibles):
    """Embed principal de rifa"""
    
    total = rifa['total_boletos']
    porcentaje = int((vendidos / total) * 10) if total > 0 else 0
    barra = "█" * porcentaje + "░" * (10 - porcentaje)
    
    embed = discord.Embed(
        title=f"🎟️ {rifa['nombre']}",
        description=f"**{rifa['premio']}**",
        color=config.COLORS['primary']
    )
    
    recaudado = vendidos * rifa['precio_boleto']
    meta = total * rifa['precio_boleto']
    
    embed.add_field(
        name="🏆 Premio",
        value=f"${rifa['valor_premio']:,}",
        inline=True
    )
    
    embed.add_field(
        name="💰 Precio",
        value=f"${rifa['precio_boleto']:,}",
        inline=True
    )
    
    embed.add_field(
        name="📊 Progreso",
        value=f"{vendidos}/{total} boletos\n{barra}",
        inline=False
    )
    
    embed.add_field(
        name="💵 Recaudado",
        value=f"${recaudado:,} / ${meta:,}",
        inline=True
    )
    
    embed.add_field(
        name="🎲 Disponibles",
        value=f"**{len(disponibles)}** números",
        inline=True
    )
    
    if rifa['fecha_cierre']:
        try:
            timestamp = int(datetime.fromisoformat(rifa['fecha_cierre']).timestamp())
            embed.add_field(
                name="⏰ Cierre",
                value=f"<t:{timestamp}:R>",
                inline=True
            )
        except:
            pass
    
    embed.set_footer(text="VP Rifas • Usa !comprar [número] para participar")
    embed.timestamp = datetime.now()
    
    return embed

def crear_embed_error(mensaje):
    """Embed para errores"""
    return discord.Embed(
        title="❌ Error",
        description=mensaje,
        color=config.COLORS['error']
    )

def crear_embed_exito(mensaje):
    """Embed para operaciones exitosas"""
    return discord.Embed(
        title="✅ Operación exitosa",
        description=mensaje,
        color=config.COLORS['success']
    )

def crear_embed_info(titulo, mensaje):
    """Embed informativo"""
    return discord.Embed(
        title=titulo,
        description=mensaje,
        color=config.COLORS['info']
    )

def crear_embed_ranking(usuarios):
    """Embed con ranking"""
    embed = discord.Embed(
        title="🏆 Ranking de compradores",
        color=config.COLORS['primary']
    )
    
    for i, usuario in enumerate(usuarios[:10], 1):
        medalla = {1: "🥇", 2: "🥈", 3: "🥉"}.get(i, f"{i}.")
        embed.add_field(
            name=f"{medalla} {usuario['nombre']}",
            value=f"{usuario['total_compras']} boletos | ${usuario['total_gastado']:,}",
            inline=False
        )
    
    return embed
