# === trade_executor.py ===
# Фоновый обработчик торговых сигналов и запуск стратегий

import time
import psycopg2
import os
from datetime import datetime

# Параметры подключения к PostgreSQL (из переменных окружения)
PG_HOST = os.environ.get("PG_HOST")
PG_PORT = os.environ.get("PG_PORT", 5432)
PG_NAME = os.environ.get("PG_NAME")
PG_USER = os.environ.get("PG_USER")
PG_PASSWORD = os.environ.get("PG_PASSWORD")

# Основной цикл фонового воркера
def run_executor():
    print("🚀 Trade Executor запущен", flush=True)
    while True:
        try:
            # Подключение к базе
            conn = psycopg2.connect(
                dbname=PG_NAME,
                user=PG_USER,
                password=PG_PASSWORD,
                host=PG_HOST,
                port=PG_PORT
            )
            cur = conn.cursor()

            # 📥 Чтение последних сигналов типа action
            cur.execute("""
                SELECT timestamp, symbol, action
                FROM signals
                WHERE action IN ('BUY', 'SELL')
                ORDER BY timestamp DESC
                LIMIT 10
            """)
            signals = cur.fetchall()
            conn.close()

            for ts, symbol, action in signals:
                print(f"[{ts}] 🛰️ {action} {symbol}", flush=True)

                # 🔧 Здесь будет запуск логики стратегии (например, channel_vilarso)
                # TODO: реализовать запуск стратегии

        except Exception as e:
            print("❌ Ошибка в trade_executor:", e, flush=True)

        time.sleep(10)  # ⏱ Проверка каждые 10 секунд

if __name__ == "__main__":
    run_executor()