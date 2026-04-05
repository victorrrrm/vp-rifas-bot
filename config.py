# config.py
import os
from dotenv import load_dotenv

load_dotenv()

# Token del bot
BOT_TOKEN = os.getenv('BOT_TOKEN')

# Rutas
DB_PATH = os.getenv('DB_PATH', 'data/rifas.db')
LOG_PATH = os.getenv('LOG_PATH', 'src/logs/bot.log')

# Prefijo y versión
PREFIX = "!"
VERSION = "4.3.1"

# IDs de categorías y canales (cámbialos si son diferentes en tu servidor)
CATEGORIA_RIFAS = int(os.getenv('CATEGORIA_RIFAS', 1482835014604296283))
JACKPOT_CANAL_ID = int(os.getenv('JACKPOT_CANAL_ID', 1486253228499931228))
RIFA_ELIMINACION_CANAL_ID = int(os.getenv('RIFA_ELIMINACION_CANAL_ID', 1486257489560342548))
UPDATE_CHANNEL_ID = int(os.getenv('UPDATE_CHANNEL_ID', 1483378335831560202))
LOG_CHANNEL_ID = int(os.getenv('LOG_CHANNEL_ID', 1482849207290429461))

# IDs de roles (deben coincidir con los que tienes en Railway)
ROLES = {
    'CEO': int(os.getenv('CEO_ROLE_ID', 1016130577595891713)),
    'DIRECTOR': int(os.getenv('DIRECTOR_ROLE_ID', 1473799754457677967)),
    'RIFAS': int(os.getenv('RIFAS_ROLE_ID', 1476836273493643438)),
    'MIEMBRO': int(os.getenv('MIEMBRO_ROLE_ID', 1442736806234816603))
}

# Colores para embeds
COLORS = {
    'primary': 0xFFD700,
    'success': 0x00FF00,
    'error': 0xFF0000,
    'info': 0x0099FF
}

# Tasa de conversión puntos revancha → VP$
PUNTOS_REVANCHA_TASA = int(os.getenv('PUNTOS_REVANCHA_TASA', 10))
