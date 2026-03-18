import os
from dotenv import load_dotenv

# Cargar variables de entorno
load_dotenv()

# Token del bot
BOT_TOKEN = os.getenv('BOT_TOKEN')

# IDs de roles
ROLES = {
    'CEO': int(os.getenv('CEO_ROLE_ID', 0)),
    'DIRECTOR': int(os.getenv('DIRECTOR_ROLE_ID', 0)),
    'RIFAS': int(os.getenv('RIFAS_ROLE_ID', 0)),
    'MIEMBRO': int(os.getenv('MIEMBRO_ROLE_ID', 0))
}

# Colores (valores fijos en hexadecimal directo)
COLORS = {
    'primary': 0xFFD700,      # Dorado
    'success': 0x00FF00,       # Verde
    'error': 0xFF0000,         # Rojo
    'info': 0x0099FF           # Azul
}

# Rutas
DB_PATH = 'data/rifas.db'
LOG_PATH = 'src/logs/bot.log'

# Configuración de rifas
DEFAULT_COMISION = int(os.getenv('DEFAULT_COMISION', 15))
MAX_BOLETOS_POR_COMPRA = int(os.getenv('MAX_BOLETOS_POR_COMPRA', 50))
TIEMPO_APARTADO_HORAS = int(os.getenv('TIEMPO_APARTADO_HORAS', 24))


# IDs de categorías
CATEGORIA_RIFAS = 1482835014604296283


# Verificar token
if not BOT_TOKEN:
    raise ValueError("❌ ERROR: No se encontró BOT_TOKEN en .env")

print("✅ Configuración cargada correctamente")
print(f"📊 Colores: {COLORS}")
