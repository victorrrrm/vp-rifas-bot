#!/data/data/com.termux/files/usr/bin/bash
cd ~/vp-rifas-bot
while true; do
    echo "=== INICIANDO BOT VP RIFAS ==="
    date
    python main.py
    echo "=== BOT CAÍDO. REINICIANDO EN 5 SEGUNDOS ==="
    sleep 5
done
