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
# === Получение направления канала по тикеру ===
def get_channel_direction(symbol):
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
            SELECT slope
            FROM live_channels
            WHERE symbol = %s
        """, (symbol,))
        row = cur.fetchone()
        conn.close()

        if not row:
            msg = f"❌ Нет данных о канале для {symbol}"
            print(msg, flush=True)
            entrylog.append(msg)
            return "неизвестно"

        slope = row[0]

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
# === Проверка: допустимо ли направление канала для данного сигнала ===
def check_direction_allowed(direction, action):
    try:
        if action == "BUYORDER":
            allowed = direction in ("восходящий ↗️", "флет ➡️")
        elif action == "SELLORDER":
            allowed = direction in ("нисходящий ↘️", "флет ➡️")
        else:
            msg = f"❌ Неизвестный тип сигнала: {action}"
            print(msg, flush=True)
            entrylog.append(msg)
            return False

        if allowed:
            msg = f"✅ Направление канала допустимо: {direction} для сигнала {action}"
        else:
            msg = f"❌ Недопустимое направление канала: {direction} для сигнала {action}"

        print(msg, flush=True)
        entrylog.append(msg)
        return allowed

    except Exception as e:
        msg = f"❌ Ошибка check_direction_allowed: {e}"
        print(msg, flush=True)
        entrylog.append(msg)
        return False
# === Проверка: ширина канала должна быть ≥ 3 × ATR (% от цены) ===
def check_channel_width_vs_atr(symbol):
    try:
        atr = get_atr(symbol, period=14)
        if atr is None:
            return False

        # Получаем ширину канала и цену закрытия последней свечи
        conn = psycopg2.connect(
            dbname=PG_NAME,
            user=PG_USER,
            password=PG_PASSWORD,
            host=PG_HOST,
            port=PG_PORT
        )
        cur = conn.cursor()

        cur.execute("""
            SELECT width_percent
            FROM live_channels
            WHERE symbol = %s
        """, (symbol,))
        width_row = cur.fetchone()

        cur.execute("""
            SELECT close
            FROM candles_5m
            WHERE symbol = %s
            ORDER BY timestamp DESC
            LIMIT 1
        """, (symbol,))
        close_row = cur.fetchone()

        conn.close()

        if not width_row or not close_row:
            msg = f"❌ Нет данных ширины канала или цены закрытия по {symbol}"
            print(msg, flush=True)
            entrylog.append(msg)
            return False

        width = width_row[0]
        close_price = close_row[0]

        atr_percent = (atr / close_price) * 100
        threshold = 3 * atr_percent

        if width >= threshold:
            msg = f"✅ Ширина канала {width:.2f}% ≥ 3×ATR ({threshold:.2f}%)"
            result = True
        else:
            msg = f"❌ Ширина канала {width:.2f}% < 3×ATR ({threshold:.2f}%)"
            result = False

        print(msg, flush=True)
        entrylog.append(msg)
        return result

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
