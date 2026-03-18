#!/data/data/com.termux/files/usr/bin/bash

# Mostrar notificación permanente
termux-notification --id "bot_vp" --title "🤖 VP Rifas Bot" --content "Bot activo - No cerrar" --ongoing

# Iniciar el bot
cd /data/data/com.termux/files/home/vp-rifas-bot
./inicio_persistente.sh
