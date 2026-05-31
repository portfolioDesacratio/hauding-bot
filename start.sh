#!/bin/bash
# Запуск локально (polling) — просто и надёжно
# Поддерживает .env файл

if [ -f .env ]; then
    set -a
    source .env
    set +a
fi

mkdir -p data

if [ -z "$BOT_TOKEN" ]; then
    echo "❌ Задай BOT_TOKEN в .env файле"
    echo "   cp .env.example .env"
    exit 1
fi

export DATA_DIR="$(pwd)/data"

# Проверка установки
python3 -c "import telegram" 2>/dev/null || {
    echo "📦 Устанавливаю зависимости..."
    pip install -r requirements.txt
}

screen -dmS collector python3 bot.py 2>/dev/null

if [ $? -eq 0 ]; then
    echo "✅ Бот запущен в screen-сессии 'collector'"
    echo "   Отключиться: Ctrl+A, D"
    echo "   Вернуться:   screen -r collector"
    echo "   Логи:        screen -r collector"
else
    echo "⚠️  screen не найден, запускаю напрямую..."
    python3 bot.py
fi
