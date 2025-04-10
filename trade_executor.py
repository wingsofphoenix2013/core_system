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
    global entrylog
    entrylog = []

    if check_open_trade_exists(symbol):
        return

    interval_start = signal_time.replace(minute=(signal_time.minute // 5) * 5, second=0, microsecond=0)
    interval_end = interval_start + timedelta(minutes=5)

    required_control = "BUYZONE" if action.startswith("BUY") else "SELLZONE"
    if not check_control_signal(symbol, required_control, interval_start, interval_end, signal_time):
        return

    if not check_trade_permission(symbol):
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
# === Расчёт канала по логике TV (49 свечей + latest_price) ===
def calculate_channel(symbol, latest_price, conn=None):
    try:
        import numpy as np
        import math
        import psycopg2

        if conn is None:
            conn = psycopg2.connect(
                dbname=PG_NAME,
                user=PG_USER,
                password=PG_PASSWORD,
                host=PG_HOST,
                port=PG_PORT
            )
        cur = conn.cursor()
        cur.execute("""
            SELECT timestamp, close
            FROM candles_5m
            WHERE symbol = %s
            ORDER BY timestamp DESC
            LIMIT 49
        """, (symbol,))
        rows = cur.fetchall()

        if not rows or len(rows) < 49:
            print(f"❌ Недостаточно данных для расчёта канала по {symbol}", flush=True)
            return None

        rows.reverse()
        closes = [row[1] for row in rows]
        closes.append(latest_price)

        X = np.arange(len(closes))
        Y = np.array(closes)

        avgX = np.mean(X)
        avgY = np.mean(Y)
        covXY = np.mean((X - avgX) * (Y - avgY))
        varX = np.mean((X - avgX) ** 2)
        slope = covXY / varX
        intercept = avgY - slope * avgX

        expected = slope * X + intercept
        stdDev = np.std(Y - expected)
        width = 2 * stdDev
        mid = expected[-1]

        width_percent = (width / mid) * 100 if mid != 0 else 0

        return {
            "slope": slope,
            "width_percent": width_percent,
            "mid": mid,
            "stdDev": stdDev,
        }

    except Exception as e:
        print(f"❌ Ошибка calculate_channel: {e}", flush=True)
        return None
# === Получение ATR по тикеру из таблицы candles_5m ===
def get_atr(symbol, period=14):
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
            SELECT timestamp, open, high, low, close
            FROM candles_5m
            WHERE symbol = %s
            ORDER BY timestamp DESC
            LIMIT %s
        """, (symbol, period + 1))
        rows = cur.fetchall()
        conn.close()

        if len(rows) < period + 1:
            msg = f"❌ Недостаточно данных для расчёта ATR({period}) по {symbol}"
            print(msg, flush=True)
            entrylog.append(msg)
            return None

        # Преобразуем в обратный порядок (от старых к новым)
        rows = list(reversed(rows))

        trs = []
        for i in range(1, len(rows)):
            _, _, high, low, close = rows[i]
            _, prev_open, _, _, prev_close = rows[i - 1]

            tr = max(
                high - low,
                abs(high - prev_close),
                abs(low - prev_close)
            )
            trs.append(tr)

        atr = sum(trs) / period
        msg = f"✅ ATR({period}) по {symbol}: {atr:.6f}"
        print(msg, flush=True)
        entrylog.append(msg)
        return atr

    except Exception as e:
        msg = f"❌ Ошибка get_atr: {e}"
        print(msg, flush=True)
        entrylog.append(msg)
        return None

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
# === Проверка: разрешена ли торговля по тикеру ===
def check_trade_permission(symbol):
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
            SELECT tradepermission
            FROM symbols
            WHERE name = %s
        """, (symbol,))
        row = cur.fetchone()
        conn.close()

        if not row:
            msg = f"❌ Тикер {symbol} не найден в таблице symbols"
            print(msg, flush=True)
            entrylog.append(msg)
            return False

        if row[0] != 'enabled':
            msg = f"❌ Торговля по тикеру {symbol} запрещена (tradepermission = '{row[0]}')"
            print(msg, flush=True)
            entrylog.append(msg)
            return False

        msg = f"✅ Торговля по тикеру {symbol} разрешена"
        print(msg, flush=True)
        entrylog.append(msg)
        return True

    except Exception as e:
        msg = f"❌ Ошибка check_trade_permission: {e}"
        print(msg, flush=True)
        entrylog.append(msg)
        return False
# === Проверка: разрешена ли стратегия ===
def check_strategy_permission(strategy_name):
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
            SELECT tradepermission
            FROM strategy
            WHERE name = %s
        """, (strategy_name,))
        row = cur.fetchone()
        conn.close()

        if not row:
            msg = f"❌ Стратегия '{strategy_name}' не найдена в таблице strategy"
            print(msg, flush=True)
            entrylog.append(msg)
            return False

        if row[0] != 'enabled':
            msg = f"❌ Стратегия '{strategy_name}' отключена (tradepermission = '{row[0]}')"
            print(msg, flush=True)
            entrylog.append(msg)
            return False

        msg = f"✅ Стратегия '{strategy_name}' разрешена"
        print(msg, flush=True)
        entrylog.append(msg)
        return True

    except Exception as e:
        msg = f"❌ Ошибка check_strategy_permission: {e}"
        print(msg, flush=True)
        entrylog.append(msg)
        return False
# === Проверка: не превышен ли лимит maxtradevolume по стратегии ===
def check_volume_limit(strategy_name):
    try:
        conn = psycopg2.connect(
            dbname=PG_NAME,
            user=PG_USER,
            password=PG_PASSWORD,
            host=PG_HOST,
            port=PG_PORT
        )
        cur = conn.cursor()

        # Получаем лимит из таблицы strategy
        cur.execute("""
            SELECT maxtradevolume
            FROM strategy
            WHERE name = %s
        """, (strategy_name,))
        row = cur.fetchone()

        if not row:
            msg = f"❌ Стратегия '{strategy_name}' не найдена в таблице strategy"
            print(msg, flush=True)
            entrylog.append(msg)
            conn.close()
            return False

        max_volume = row[0]

        # Считаем суммарный размер всех открытых сделок по этой стратегии
        cur.execute("""
            SELECT COALESCE(SUM(size), 0)
            FROM trades
            WHERE strategy = %s AND status = 'open'
        """, (strategy_name,))
        current_volume = cur.fetchone()[0]
        conn.close()

        if current_volume < max_volume:
            msg = f"✅ Текущий объём {current_volume} не превышает лимит {max_volume}"
            print(msg, flush=True)
            entrylog.append(msg)
            return True
        else:
            msg = f"❌ Лимит maxtradevolume превышен: {current_volume} из {max_volume}"
            print(msg, flush=True)
            entrylog.append(msg)
            return False

    except Exception as e:
        msg = f"❌ Ошибка check_volume_limit: {e}"
        print(msg, flush=True)
        entrylog.append(msg)
        return False
# === Получение направления канала на основе расчёта регрессии ===
def get_channel_direction(symbol):
    try:
        # Берём цену закрытия последней свечи
        conn = psycopg2.connect(
            dbname=PG_NAME,
            user=PG_USER,
            password=PG_PASSWORD,
            host=PG_HOST,
            port=PG_PORT
        )
        cur = conn.cursor()
        cur.execute("""
            SELECT close
            FROM candles_5m
            WHERE symbol = %s
            ORDER BY timestamp DESC
            LIMIT 1
        """, (symbol,))
        row = cur.fetchone()
        conn.close()

        if not row:
            msg = f"❌ Нет цены закрытия для {symbol}"
            print(msg, flush=True)
            entrylog.append(msg)
            return "неизвестно"

        latest_price = row[0]
        result = calculate_channel(symbol, latest_price)

        if not result:
            return "неизвестно"

        slope = result["slope"]

        if slope > 0.01:
            direction = "восходящий ↗️"
        elif slope < -0.01:
            direction = "нисходящий ↘️"
        else:
            direction = "флет ➡️"

        msg = f"✅ Направление канала для {symbol}: {direction} (наклон = {slope:.4f})"
        print(msg, flush=True)
        entrylog.append(msg)
        return direction

    except Exception as e:
        msg = f"❌ Ошибка get_channel_direction: {e}"
        print(msg, flush=True)
        entrylog.append(msg)
        return "неизвестно"
# === Проверка: ширина канала ≥ 3 × ATR на момент входа ===
def check_channel_width_vs_atr(symbol):
    try:
        atr = get_atr(symbol)
        if atr is None:
            return False

        conn = psycopg2.connect(
            dbname=PG_NAME,
            user=PG_USER,
            password=PG_PASSWORD,
            host=PG_HOST,
            port=PG_PORT
        )
        cur = conn.cursor()
        cur.execute("""
            SELECT close
            FROM candles_5m
            WHERE symbol = %s
            ORDER BY timestamp DESC
            LIMIT 1
        """, (symbol,))
        row = cur.fetchone()
        conn.close()

        if not row:
            msg = f"❌ Нет цены закрытия для {symbol}"
            print(msg, flush=True)
            entrylog.append(msg)
            return False

        latest_price = row[0]
        result = calculate_channel(symbol, latest_price)

        if not result:
            return False

        width_percent = result["width_percent"]
        slope = result["slope"]
        mid = result["mid"]
        atr_percent = (atr / mid) * 100 if mid else 0
        threshold = 3 * atr_percent

        if width_percent >= threshold:
            msg = f"✅ Ширина канала {width_percent:.2f}% ≥ 3×ATR ({threshold:.2f}%)"
            result_ok = True
        else:
            msg = f"❌ Ширина канала {width_percent:.2f}% < 3×ATR ({threshold:.2f}%)"
            result_ok = False

        print(msg, flush=True)
        entrylog.append(msg)
        return result_ok

    except Exception as e:
        msg = f"❌ Ошибка check_channel_width_vs_atr: {e}"
        print(msg, flush=True)
        entrylog.append(msg)
        return False
# === Заглушки остальных функций ===
def execute_trade(symbol, action, strategy): entrylog.append("✅ trade executed")
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
