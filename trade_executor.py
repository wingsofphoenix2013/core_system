# === trade_executor.py ===
# Фоновый обработчик торговых сигналов и запуск стратегий

import time
import psycopg2
import os
from datetime import datetime, timedelta

PG_HOST = os.environ.get("PG_HOST")
PG_PORT = os.environ.get("PG_PORT", 5432)
PG_NAME = os.environ.get("PG_NAME")
PG_USER = os.environ.get("PG_USER")
PG_PASSWORD = os.environ.get("PG_PASSWORD")

# === СТРАТЕГИЯ: channel_vilarso ===
def run_channel_vilarso(symbol, action, signal_time):
    print(f"🧠 Запуск стратегии channel_vilarso для {symbol} @ {signal_time}", flush=True)

    if check_open_trade_exists(symbol):
        print(f"⛔ Уже есть активная сделка по {symbol}", flush=True)
        return

    interval_start = signal_time.replace(minute=(signal_time.minute // 5) * 5, second=0, microsecond=0)
    interval_end = interval_start + timedelta(minutes=5)

    required_control = "BUYZONE" if action.startswith("BUY") else "SELLZONE"
    if not check_control_signal(symbol, required_control, interval_start, interval_end, signal_time):
        print(f"⛔ Нет сигнала {required_control} ДО {action}", flush=True)
        return

    if not check_trade_permission(symbol):
        print(f"⛔ Торговля по тикеру {symbol} запрещена", flush=True)
        return

    if not check_strategy_permission("channel_vilarso"):
        print(f"⛔ Стратегия channel_vilarso отключена", flush=True)
        return

    if not check_volume_limit("channel_vilarso"):
        print(f"⛔ Превышен лимит объёма по стратегии", flush=True)
        return

    direction = get_channel_direction(symbol)
    if not check_direction_allowed(direction, action):
        print(f"⛔ Направление канала {direction} не разрешено", flush=True)
        return

    if not check_channel_width_vs_atr(symbol):
        print(f"⛔ Ширина канала не проходит условие >= 3*ATR", flush=True)
        return

    execute_trade(symbol, action, "channel_vilarso")
    print(f"✅ Сделка открыта по {symbol}", flush=True)

# === Заглушки всех вспомогательных функций ===
def check_open_trade_exists(symbol): return False
def check_control_signal(symbol, control, start, end, action_time): return True
def check_trade_permission(symbol): return True
def check_strategy_permission(strategy): return True
def check_volume_limit(strategy): return True
def get_channel_direction(symbol): return "восходящий ↗️"
def check_direction_allowed(direction, action): return True
def check_channel_width_vs_atr(symbol): return True
def execute_trade(symbol, action, strategy): pass

# === Основной цикл воркера ===
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

            cur.execute("""
                SELECT timestamp, symbol, action
                FROM signals
                WHERE type = 'action'
                  AND processed = false
                  AND timestamp >= now() - interval '1 minute'
                ORDER BY timestamp DESC
            """)
            signals = cur.fetchall()

            for ts, symbol, action in signals:
                print(f"[{ts}] 🛰️ {action} {symbol}", flush=True)
                run_channel_vilarso(symbol, action, ts)

                # ✅ Помечаем сигнал как обработанный
                cur.execute("""
                    UPDATE signals
                    SET processed = true
                    WHERE symbol = %s AND action = %s AND timestamp = %s
                """, (symbol, action, ts))

            conn.commit()
            conn.close()

            if not signals:
                print("⏱ Нет свежих сигналов (type='action')", flush=True)

        except Exception as e:
            print("❌ Ошибка в trade_executor:", e, flush=True)

        time.sleep(10)

if __name__ == "__main__":
    run_executor()