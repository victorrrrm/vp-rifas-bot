#!/data/data/com.termux/files/usr/bin/bash
termux-wake-lock
termux-notification --id "bot_vp" --title "🤖 VP Rifas Bot" --content "Bot activo - No cerrar" --ongoing
renice -n -20 $$ > /dev/null 2>&1
pkill -f ultra_persistente 2>/dev/null
tmux kill-session -t bot 2>/dev/null
nohup nice -n -20 ./ultra_persistente.sh > watchdog.log 2>&1 &
echo "✅ BOT ACTIVADO - Ya puedes cerrar Termux"
echo "📊 Para ver logs: tail -f ~/vp-rifas-bot/watchdog.log"
