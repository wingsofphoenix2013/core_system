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

# === СТРАТЕГИЯ: channel_vilarso обновленая  ===
def run_channel_vilarso(symbol, action, signal_time, strategy_id):
    print(f"🧠 Запуск стратегии channel_vilarso для {symbol} @ {signal_time}", flush=True)
    global entrylog
    entrylog = []

    if check_open_trade_exists(symbol):
        return

    interval_start = signal_time.replace(minute=(signal_time.minute // 5) * 5, second=0, microsecond=0)
    interval_end = interval_start + timedelta(minutes=5)

    required_control = "BUYZONE" if action.startswith("BUY") else "SELLZONE"
    if not check_control_signal(symbol, required_control, interval_start, interval_end, signal_time):
        return

    if not check_trade_permission(symbol, strategy_id):
        return

    if not check_strategy_permission("channel_vilarso"):
        return

    if not check_volume_limit("channel_vilarso"):
        return

    direction = get_channel_direction(symbol)
    if not check_direction_allowed(direction, action):
        return

    if not check_channel_width_vs_atr(symbol):
        return

    execute_trade(symbol, action, "channel_vilarso")
    print(f"✅ Сделка открыта по {symbol}", flush=True)

# === Проверка: есть ли активная сделка по тикеру ===
def check_open_trade_exists(symbol):
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
            SELECT 1
            FROM trades
            WHERE symbol = %s AND status = 'open'
            LIMIT 1
        """, (symbol,))
        exists = cur.fetchone()
        conn.close()

        if exists:
            msg = f"⛔ Уже есть активная сделка по {symbol}"
        else:
            msg = f"✅ Нет активной сделки по {symbol}"

        print(msg, flush=True)
        entrylog.append(msg)
        return bool(exists)
    except Exception as e:
        msg = f"❌ Ошибка check_open_trade_exists: {e}"
        print(msg, flush=True)
        entrylog.append(msg)
        return True
# === Проверка: был ли сигнал BUYZONE/SELLZONE ранее в пределах свечи ===
def check_control_signal(symbol, control, start, end, action_time):
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
            SELECT timestamp FROM signals
            WHERE symbol = %s AND type = 'control' AND action = %s
              AND timestamp >= %s AND timestamp < %s
              AND timestamp < %s
            ORDER BY timestamp DESC
            LIMIT 1
        """, (symbol, control, start, end, action_time))
        row = cur.fetchone()
        conn.close()

        if row:
            msg = f"✅ Найден сигнал {control} в интервале {start}–{end}, до {action_time}"
            print(msg, flush=True)
            entrylog.append(msg)
            return True
        else:
            msg = f"❌ Сигнал {control} не найден в интервале {start}–{end} до {action_time}"
            print(msg, flush=True)
            entrylog.append(msg)
            return False
    except Exception as e:
        msg = f"❌ Ошибка check_control_signal: {e}"
        print(msg, flush=True)
        entrylog.append(msg)
        return False
# === Проверка: разрешена ли торговля по тикеру и стратегии ===
def check_trade_permission(symbol, strategy_id):
    try:
        conn = psycopg2.connect(
            dbname=PG_NAME,
            user=PG_USER,
            password=PG_PASSWORD,
            host=PG_HOST,
            port=PG_PORT
        )
        cur = conn.cursor()

        cur.execute("SELECT tradepermission FROM symbols WHERE name = %s", (symbol,))
        symbol_row = cur.fetchone()

        cur.execute("SELECT tradepermission FROM strategy WHERE id = %s", (strategy_id,))
        strategy_row = cur.fetchone()

        conn.close()

        if not symbol_row:
            msg = f"❌ Тикер {symbol} не найден в таблице symbols"
            print(msg, flush=True)
            entrylog.append(msg)
            return False

        if not strategy_row:
            msg = f"❌ Стратегия {strategy_id} не найдена в таблице strategy"
            print(msg, flush=True)
            entrylog.append(msg)
            return False

        if symbol_row[0] != 'enabled':
            msg = f"❌ Торговля по тикеру {symbol} запрещена"
            print(msg, flush=True)
            entrylog.append(msg)
            return False

        if strategy_row[0] != 'enabled':
            msg = f"❌ Стратегия {strategy_id} отключена"
            print(msg, flush=True)
            entrylog.append(msg)
            return False

        msg = f"✅ Торговля по тикеру {symbol} и стратегии {strategy_id} разрешена"
        print(msg, flush=True)
        entrylog.append(msg)
        return True

    except Exception as e:
        msg = f"❌ Ошибка check_trade_permission: {e}"
        print(msg, flush=True)
        entrylog.append(msg)
        return False
# === Заглушки остальных функций ===
def check_volume_limit(strategy): entrylog.append("✅ volume check ok"); return True
def get_channel_direction(symbol): return "восходящий ↗️"
def check_direction_allowed(direction, action): entrylog.append("✅ направление канала допустимо"); return True
def check_channel_width_vs_atr(symbol): entrylog.append("✅ ширина канала >= 3*ATR"); return True
def execute_trade(symbol, action, strategy): entrylog.append("✅ trade executed")

# === Основной цикл воркера с фильтрацией сигналов по стратегиям ===
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

            # Получаем все необработанные сигналы
            cur.execute("""
                SELECT id, timestamp, symbol, action, type
                FROM signals
                WHERE processed = false
                  AND timestamp >= now() - interval '1 minute'
                ORDER BY timestamp DESC
            """)
            signals = cur.fetchall()

            # Получаем все активные стратегии
            cur.execute("""
                SELECT id, name, uses_buyorder, uses_sellorder, uses_buyzone, uses_sellzone, uses_info
                FROM strategy
                WHERE tradepermission = 'enabled'
            """)
            strategies = cur.fetchall()

            for signal_id, ts, symbol, action, signal_type in signals:
                print(f"[{ts}] 🛰️ {action} {symbol} ({signal_type})", flush=True)

                for strategy in strategies:
                    strategy_id, name, use_buy, use_sell, use_zone_buy, use_zone_sell, use_info = strategy

                    # Обрабатываются только сигналы типа action
                    if signal_type != 'action':
                        continue

                    if action == 'BUYORDER' and use_buy:
                        run_channel_vilarso(symbol, action, ts, strategy_id)

                    elif action == 'SELLORDER' and use_sell:
                        run_channel_vilarso(symbol, action, ts, strategy_id)

                    elif action == 'BUYZONE' and use_zone_buy:
                        run_channel_vilarso(symbol, action, ts, strategy_id)

                    elif action == 'SELLZONE' and use_zone_sell:
                        run_channel_vilarso(symbol, action, ts, strategy_id)

                    elif action == 'INFO' and use_info:
                        run_channel_vilarso(symbol, action, ts, strategy_id)

                # Сигнал помечается как обработанный один раз
                cur.execute("""
                    UPDATE signals
                    SET processed = true
                    WHERE id = %s
                """, (signal_id,))

            conn.commit()
            conn.close()

            if not signals:
                print("⏱ Нет свежих сигналов", flush=True)

        except Exception as e:
            print("❌ Ошибка в trade_executor:", e, flush=True)

        time.sleep(10)

if __name__ == "__main__":
    run_executor()
