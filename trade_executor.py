# === trade_executor.py ===
# Фоновый обработчик торговых сигналов и запуск стратегий

import time
import psycopg2
import os
from datetime import datetime, timedelta

# Параметры подключения к PostgreSQL
PG_HOST = os.environ.get("PG_HOST")
PG_PORT = os.environ.get("PG_PORT", 5432)
PG_NAME = os.environ.get("PG_NAME")
PG_USER = os.environ.get("PG_USER")
PG_PASSWORD = os.environ.get("PG_PASSWORD")

# === СТРАТЕГИЯ: channel_vilarso ===
# Обработка сигнала action по условиям стратегии

def run_channel_vilarso(symbol, action, signal_time):
    print(f"🧠 Запуск стратегии channel_vilarso для {symbol} @ {signal_time}", flush=True)

    # 🔎 Проверка: если по этому тикеру уже есть открытая сделка
    if check_open_trade_exists(symbol):
        print(f"⛔ Уже есть активная сделка по {symbol}, сигнал проигнорирован", flush=True)
        return

    # 🔎 Определение текущей 5-минутной свечи
    interval_start = signal_time.replace(minute=(signal_time.minute // 5) * 5, second=0, microsecond=0)
    interval_end = interval_start + timedelta(minutes=5)

    # 🔍 Проверка на наличие сигнала control ДО action в той же свече
    if action.startswith("BUY"):
        required_control = "BUYZONE"
    else:
        required_control = "SELLZONE"

    if not check_control_signal(symbol, required_control, interval_start, interval_end, signal_time):
        print(f"⛔ Нет сигнала {required_control} ДО {action} в интервале свечи — вход отклонён", flush=True)
        return

    # 📛 Проверка разрешений
    if not check_trade_permission(symbol):
        print(f"⛔ Торговля по тикеру {symbol} запрещена", flush=True)
        return

    if not check_strategy_permission("channel_vilarso"):
        print(f"⛔ Стратегия channel_vilarso отключена", flush=True)
        return

    # 📉 Проверка maxtradevolume
    if not check_volume_limit("channel_vilarso"):
        print(f"⛔ Превышен лимит объёма по стратегии channel_vilarso", flush=True)
        return

    # 📈 Проверка направления канала
    direction = get_channel_direction(symbol)
    if not check_direction_allowed(direction, action):
        print(f"⛔ Недопустимое направление канала ({direction})", flush=True)
        return

    # 📏 Проверка ширины канала vs ATR
    if not check_channel_width_vs_atr(symbol):
        print(f"⛔ Канал слишком узкий — не соответствует 3*ATR", flush=True)
        return

    # ✅ Все условия выполнены → открываем сделку
    execute_trade(symbol, action, "channel_vilarso")
    print(f"✅ Сделка по стратегии channel_vilarso открыта для {symbol}", flush=True)


# Основной цикл фонового воркера
def run_executor():
    print("🚀 Trade Executor запущен", flush=True)
    while True:
        try:
            conn = psycopg2.connect(
                dbname=PG_NAME,
                user=PG_USER,
                password=PG_PASSWORD,
                host=PG_HOST,
                port=PG_PORT
            )
            cur = conn.cursor()

            # 📥 Получение последних сигналов типа action
            cur.execute("""
                SELECT timestamp, symbol, action
                FROM signals
                WHERE type = 'action'
                  AND timestamp >= now() - interval '1 minute'
                ORDER BY timestamp DESC
            """)
            signals = cur.fetchall()
            conn.close()

            if not signals:
                print("⏱ Нет свежих сигналов (type='action')", flush=True)

            for ts, symbol, action in signals:
                print(f"[{ts}] 🛰️ {action} {symbol}", flush=True)

                # Запуск стратегии channel_vilarso
                run_channel_vilarso(symbol, action, ts)

        except Exception as e:
            print("❌ Ошибка в trade_executor:", e, flush=True)

        time.sleep(10)

if __name__ == "__main__":
    run_executor()