import os
from dotenv import load_dotenv

load_dotenv()

BOT_TOKEN = os.getenv('BOT_TOKEN')

ROLES = {
    'CEO': int(os.getenv('CEO_ROLE_ID', 0)),
    'DIRECTOR': int(os.getenv('DIRECTOR_ROLE_ID', 0)),
    'RIFAS': int(os.getenv('RIFAS_ROLE_ID', 0)),
    'MIEMBRO': int(os.getenv('MIEMBRO_ROLE_ID', 0))
}

COLORS = {
    'primary': 0xFFD700,
    'success': 0x00FF00,
    'error': 0xFF0000,
    'info': 0x0099FF
}

CATEGORIA_RIFAS = int(os.getenv('CATEGORIA_RIFAS', 0))

if not BOT_TOKEN:
    raise ValueError("❌ ERROR: No se encontró BOT_TOKEN")
