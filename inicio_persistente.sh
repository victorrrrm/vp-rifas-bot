#!/data/data/com.termux/files/usr/bin/bash

# Mantener el teléfono despierto
termux-wake-lock

# Aumentar prioridad
renice -n -20 $$ > /dev/null 2>&1

# Matar instancias anteriores
pkill -f ultra_persistente 2>/dev/null
tmux kill-session -t bot 2>/dev/null

# Iniciar watchdog en segundo plano con prioridad
cd /data/data/com.termux/files/home/vp-rifas-bot
nohup nice -n -20 ./ultra_persistente.sh > watchdog.log 2>&1 &

echo "✅ Bot iniciado con prioridad máxima"
echo "📊 Ver logs: tail -f ~/vp-rifas-bot/watchdog.log"
