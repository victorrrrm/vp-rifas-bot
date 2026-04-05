import os
from dotenv import load_dotenv

# Cargar variables de entorno
load_dotenv()

# ============================================
# TOKEN DEL BOT
# ============================================

BOT_TOKEN = os.getenv('BOT_TOKEN')

# ============================================
# IDS DE ROLES
# ============================================

ROLES = {
    'CEO': int(os.getenv('CEO_ROLE_ID', 1016130577595891713)),
    'DIRECTOR': int(os.getenv('DIRECTOR_ROLE_ID', 1473799754457677967)),
    'RIFAS': int(os.getenv('RIFAS_ROLE_ID', 1476836273493643438)),
    'MIEMBRO': int(os.getenv('MIEMBRO_ROLE_ID', 1442736806234816603))
}

# ============================================
# IDS DE ROLES DE FIDELIZACIÓN
# ============================================

ROLES_FIDELIZACION = {
    'BRONCE': 1483720270496661515,
    'PLATA': 1483720387178139758,
    'ORO': 1483720490601418822,
    'PLATINO': 1483720672185155625,
    'DIAMANTE': 1483720783422296165,
    'MASTER': 1483721013144584192
}

# ============================================
# IDS DE ROLES DE FRANQUICIAS
# ============================================

ROLES_FRANQUICIA = {
    1: 1489834608747876453,
    2: 1489834653849358436,
    3: 1489834697281245324,
    4: 1489834736569552896,
    5: 1489834781138096219
}

# ============================================
# IDS DE CANALES DE FRANQUICIAS
# ============================================

CANALES_FRANQUICIA = {
    1: 1489817105225486457,
    2: 1489818797186486292,
    3: 1489818846645981224,
    4: 1489818896327377006,
    5: 1489818943471357993
}

# ============================================
# IDS DE ROLES DE DISTRIBUIDORES
# ============================================

ROLES_DISTRIBUIDORES = {
    'A': 1489828306801922221,
    'B': 1489828037703766288,
    'C': 1489827878152437870,
    'D': 1489827780181884998,
    'E': 1489827683629137940,
    'F': 1489827505715281980
}

# ============================================
# ID DE CATEGORÍA DE RIFAS
# ============================================

CATEGORIA_RIFAS = 1482835014604296283

# ============================================
# ID DE CATEGORÍA DE FRANQUICIAS
# ============================================

CATEGORIA_FRANQUICIAS = 1489816515774517278

# ============================================
# ID DEL CANAL DE LOGS DEL SISTEMA
# ============================================

LOG_CHANNEL_ID = 1483378335831560202

# ============================================
# ID DEL CANAL DE LOGS DE ACCIONES
# ============================================

ACTION_LOG_CHANNEL_ID = 1482849207290429461

# ============================================
# ID DEL CANAL DE JACKPOT
# ============================================

JACKPOT_CHANNEL_ID = 1486253228499931228

# ============================================
# ID DEL CANAL DE RIFA ELIMINACIÓN
# ============================================

ELIMINACION_CHANNEL_ID = 1486257489560342548

# ============================================
# COLORES PARA EMBEDS
# ============================================

COLORS = {
    'primary': 0xFFD700,      # Dorado
    'success': 0x00FF00,       # Verde
    'error': 0xFF0000,         # Rojo
    'info': 0x0099FF,          # Azul
    'warning': 0xFFA500        # Naranja
}

# ============================================
# CONFIGURACIÓN DE RIFAS
# ============================================

DEFAULT_COMISION = 15
MAX_BOLETOS_POR_COMPRA = 50
TIEMPO_APARTADO_HORAS = 24

# ============================================
# RUTAS DE ARCHIVOS
# ============================================

# Detectar si estamos en Railway o local
if os.path.exists('/app/data'):
    DB_PATH = '/app/data/rifas.db'
    LOG_PATH = '/app/data/bot.log'
    BACKUP_PATH = '/app/data/backups'
    VOLUME_MOUNTED = True
else:
    DB_PATH = 'data/rifas.db'
    LOG_PATH = 'src/logs/bot.log'
    BACKUP_PATH = 'backups'
    VOLUME_MOUNTED = False

# Crear carpetas necesarias
os.makedirs(os.path.dirname(DB_PATH) if os.path.dirname(DB_PATH) else 'data', exist_ok=True)
os.makedirs('src/logs', exist_ok=True)
os.makedirs(BACKUP_PATH, exist_ok=True)

# ============================================
# VERIFICACIÓN DEL TOKEN
# ============================================

if not BOT_TOKEN:
    raise ValueError("❌ ERROR: No se encontró BOT_TOKEN en .env")

# ============================================
# MENSAJE DE INICIO
# ============================================

print("✅ Configuración cargada correctamente")
print(f"📊 Colores: {COLORS}")
print(f"💾 Volumen persistente: {'✅ Activado' if VOLUME_MOUNTED else '❌ No detectado'}")
print(f"📁 Base de datos: {DB_PATH}")
