#!/data/data/com.termux/files/usr/bin/bash

BOT_DIR="/data/data/com.termux/files/home/vp-rifas-bot"
LOG_FILE="$BOT_DIR/watchdog.log"

# Adquirir wake lock permanente (evita suspensión)
termux-wake-lock

# Aumentar prioridad del proceso al máximo
echo "=== INICIANDO WATCHDOG CON PRIORIDAD MÁXIMA ===" | tee -a $LOG_FILE
date | tee -a $LOG_FILE

# Dar prioridad máxima a este script
renice -n -20 $$ > /dev/null 2>&1

while true; do
    # Verificar si la sesión de tmux existe
    if ! tmux has-session -t bot 2>/dev/null; then
        echo "$(date): Bot no encontrado, iniciando..." | tee -a $LOG_FILE
        cd $BOT_DIR
        
        # Iniciar tmux con prioridad máxima
        tmux new-session -d -s bot 'nice -n -20 python main.py'
        sleep 5
    else
        # Verificar que el proceso realmente está corriendo
        if ! tmux list-panes -t bot -F "#{pane_pid}" | xargs -I {} ps -p {} > /dev/null 2>&1; then
            echo "$(date): Proceso del bot muerto, reiniciando..." | tee -a $LOG_FILE
            tmux kill-session -t bot 2>/dev/null
            cd $BOT_DIR
            tmux new-session -d -s bot 'nice -n -20 python main.py'
        fi
    fi
    
    # Mantener el wake lock activo
    termux-wake-lock
    
    sleep 15  # Verificar cada 15 segundos (más frecuente)
done
