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
# === ПОТОК @trade для получения онлайн-цен Binance ===
import threading
import websocket
import json

latest_price = {}

def load_symbols_from_db():
    try:
        conn = psycopg2.connect(
            dbname=PG_NAME,
            user=PG_USER,
            password=PG_PASSWORD,
            host=PG_HOST,
            port=PG_PORT
        )
        cur = conn.cursor()
        cur.execute("SELECT name FROM symbols")
        rows = cur.fetchall()
        conn.close()
        return [row[0].lower() for row in rows]
    except Exception as e:
        print(f"❌ Ошибка при загрузке символов: {e}", flush=True)
        return []

def start_trade_stream():
    def run():
        subscribed = set()
        ws = None

        while True:
            try:
                symbols = load_symbols_from_db()
                new_symbols = [s for s in symbols if s not in subscribed]

                if new_symbols or ws is None:
                    if ws:
                        ws.close()
                        time.sleep(1)

                    streams = "/".join([f"{s}@trade" for s in symbols])
                    url = f"wss://stream.binance.com:9443/stream?streams={streams}"
                    print(f"🔌 Подключение к потоку: {url}", flush=True)

                    def on_message(wsapp, message):
                        try:
                            data = json.loads(message)
                            stream = data.get("stream", "")
                            if not stream.endswith("@trade"):
                                return
                            symbol = stream.replace("@trade", "").upper()
                            price = float(data["data"]["p"])
                            latest_price[symbol] = price
                        except Exception as e:
                            print(f"Ошибка обработки сообщения: {e}", flush=True)

                    def on_error(wsapp, error):
                        print(f"WebSocket error: {error}", flush=True)

                    def on_close(wsapp, close_status_code, close_msg):
                        print("🔌 WebSocket закрыт, переподключение...", flush=True)

                    def on_open(wsapp):
                        print("🟢 WebSocket открыт", flush=True)

                    ws = websocket.WebSocketApp(url, on_message=on_message,
                                                on_error=on_error, on_close=on_close, on_open=on_open)

                    threading.Thread(target=ws.run_forever, daemon=True).start()
                    subscribed = set(symbols)

                time.sleep(30)

            except Exception as e:
                print(f"Ошибка в trade-потоке: {e}", flush=True)
                time.sleep(10)

    threading.Thread(target=run, daemon=True).start()
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
# === Расчёт SL и TP уровней стратегии channel_vilarso с логированием ===
def calculate_sl_tp(entry_price, atr, direction="long"):
    """
    Расчёт SL и 6 TP уровней в соответствии со стратегией channel_vilarso
    :param entry_price: цена входа
    :param atr: значение ATR на момент входа
    :param direction: "long" или "short"
    :return: (stop_loss_price, take_profits: list)
    """
    take_profits = []

    if direction == "long":
        stop_loss_price = entry_price - 1.5 * atr
        entrylog.append(f"✅ SL: {stop_loss_price:.5f}")

        take_profits.append({"tp_percent": 50, "tp_price": entry_price + 2.0 * atr, "new_sl": entry_price})
        take_profits.append({"tp_percent": 10, "tp_price": entry_price + 2.2 * atr, "new_sl": entry_price + 1.0 * atr})
        take_profits.append({"tp_percent": 10, "tp_price": entry_price + 2.4 * atr, "new_sl": entry_price + 1.2 * atr})
        take_profits.append({"tp_percent": 10, "tp_price": entry_price + 2.6 * atr, "new_sl": entry_price + 1.4 * atr})
        take_profits.append({"tp_percent": 10, "tp_price": entry_price + 2.8 * atr, "new_sl": entry_price + 1.6 * atr})
        take_profits.append({"tp_percent": 10, "tp_price": entry_price + 3.0 * atr, "new_sl": entry_price + 1.8 * atr})
    else:  # short
        stop_loss_price = entry_price + 1.5 * atr
        entrylog.append(f"✅ SL: {stop_loss_price:.5f}")

        take_profits.append({"tp_percent": 50, "tp_price": entry_price - 2.0 * atr, "new_sl": entry_price})
        take_profits.append({"tp_percent": 10, "tp_price": entry_price - 2.2 * atr, "new_sl": entry_price - 1.0 * atr})
        take_profits.append({"tp_percent": 10, "tp_price": entry_price - 2.4 * atr, "new_sl": entry_price - 1.2 * atr})
        take_profits.append({"tp_percent": 10, "tp_price": entry_price - 2.6 * atr, "new_sl": entry_price - 1.4 * atr})
        take_profits.append({"tp_percent": 10, "tp_price": entry_price - 2.8 * atr, "new_sl": entry_price - 1.6 * atr})
        take_profits.append({"tp_percent": 10, "tp_price": entry_price - 3.0 * atr, "new_sl": entry_price - 1.8 * atr})

    # Логируем TP уровни
    for i, tp in enumerate(take_profits, start=1):
        entrylog.append(f"✅ TP{i}: {tp['tp_price']:.5f} ({tp['tp_percent']}%), SL → {tp['new_sl']:.5f}")

    return stop_loss_price, take_profits
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
# === Открытие сделки и сохранение SL/TP в trades_sltp ===
def execute_trade(symbol, action, strategy):
    try:
        entry_price = latest_price.get(symbol)
        if entry_price is None:
            msg = f"❌ Нет online-цены для {symbol} в latest_price"
            print(msg, flush=True)
            entrylog.append(msg)
            return

        atr = get_atr(symbol)
        if atr is None:
            return

        direction = "long" if action == "BUYORDER" else "short"

        conn = psycopg2.connect(
            dbname=PG_NAME,
            user=PG_USER,
            password=PG_PASSWORD,
            host=PG_HOST,
            port=PG_PORT
        )
        cur = conn.cursor()

        cur.execute("""
            SELECT size, leverage
            FROM strategy
            WHERE name = %s
        """, (strategy,))
        strat = cur.fetchone()
        if not strat:
            msg = f"❌ Стратегия {strategy} не найдена"
            print(msg, flush=True)
            entrylog.append(msg)
            conn.close()
            return

        size, leverage = strat
        position = size / entry_price  # количество монет в позиции

        sl_price, tp_list = calculate_sl_tp(entry_price, atr, direction)

        # Вставка сделки
        cur.execute("""
            INSERT INTO trades (symbol, side, entry_time, entry_price, size, position, leverage, strategy, status, entrylog)
            VALUES (%s, %s, now(), %s, %s, %s, %s, %s, 'open', %s)
            RETURNING id
        """, (
            symbol,
            direction,
            entry_price,
            size,
            position,
            leverage,
            strategy,
            "\n".join(entrylog)
        ))
        trade_id = cur.fetchone()[0]

        # Сохраняем SL
        cur.execute("""
            INSERT INTO trades_sltp (trade_id, type, step, target_price, exit_percent, new_stop_loss)
            VALUES (%s, 'sl', 0, %s, 100, NULL)
        """, (trade_id, sl_price))

        # Сохраняем TP уровни
        for i, tp in enumerate(tp_list, start=1):
            cur.execute("""
                INSERT INTO trades_sltp (trade_id, type, step, target_price, exit_percent, new_stop_loss)
                VALUES (%s, 'tp', %s, %s, %s, %s)
            """, (
                trade_id,
                i,
                tp["tp_price"],
                tp["tp_percent"],
                tp["new_sl"]
            ))

        conn.commit()
        conn.close()

        entrylog.append(f"✅ Сделка открыта по цене {entry_price:.5f}, trade_id={trade_id}")
        print(f"✅ Сделка открыта по {symbol} (trade_id={trade_id})", flush=True)

    except Exception as e:
        msg = f"❌ Ошибка execute_trade: {e}"
        print(msg, flush=True)
        entrylog.append(msg)
# === Мониторинг открытых сделок и расчёт PnL на основе position ===
def monitor_active_trades():
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

            # Получаем все открытые сделки
            cur.execute("""
                SELECT id, symbol, side, entry_price, position
                FROM trades
                WHERE status = 'open'
            """)
            trades = cur.fetchall()

            for trade_id, symbol, side, entry_price, total_position in trades:
                price = latest_price.get(symbol)
                if price is None:
                    continue

                # Получаем все активные уровни SL/TP
                cur.execute("""
                    SELECT id, type, step, target_price, exit_percent, new_stop_loss
                    FROM trades_sltp
                    WHERE trade_id = %s
                    ORDER BY step
                """, (trade_id,))
                levels = cur.fetchall()

                for level in levels:
                    lvl_id, lvl_type, step, target_price, exit_percent, new_sl = level

                    # Проверка срабатывания уровня
                    triggered = (
                        price >= target_price if side == "long" and lvl_type == "tp" else
                        price <= target_price if side == "short" and lvl_type == "tp" else
                        price <= target_price if side == "long" and lvl_type == "sl" else
                        price >= target_price if side == "short" and lvl_type == "sl" else
                        False
                    )

                    if triggered:
                        reason = f"{lvl_type}-hit-{step}" if lvl_type == "tp" else "sl-hit"
                        print(f"⚡️ Сработал уровень {lvl_type.upper()} step={step} по {symbol}: цена {price}, цель {target_price}", flush=True)

                        # Расчёт фактического количества монет и суммы выхода
                        actual_position = total_position * (exit_percent / 100)
                        exit_amount = actual_position * price

                        # Расчёт PnL
                        if side == "long":
                            gross_pnl = (price - entry_price) * actual_position
                        else:
                            gross_pnl = (entry_price - price) * actual_position

                        entry_fee = entry_price * 0.0005 * actual_position
                        exit_fee = price * 0.0002 * actual_position
                        net_pnl = gross_pnl - entry_fee - exit_fee

                        # Вставка в trade_exits
                        cur.execute("""
                            INSERT INTO trade_exits (trade_id, price, size, position, percent, reason, planned, pnl)
                            VALUES (%s, %s, %s, %s, %s, %s, false, %s)
                        """, (
                            trade_id,
                            price,
                            exit_amount,
                            actual_position,
                            exit_percent,
                            reason,
                            net_pnl
                        ))

                        # Удаление сработавшего уровня
                        cur.execute("DELETE FROM trades_sltp WHERE id = %s", (lvl_id,))

                        # Закрытие сделки при SL или TP6
                        if lvl_type == "sl" or step == 6:
                            cur.execute("""
                                SELECT SUM(pnl)
                                FROM trade_exits
                                WHERE trade_id = %s
                            """, (trade_id,))
                            pnl_total = cur.fetchone()[0] or 0

                            cur.execute("""
                                UPDATE trades
                                SET status = 'closed',
                                    exit_time = now(),
                                    exit_price = %s,
                                    pnl = %s
                                WHERE id = %s
                            """, (price, pnl_total, trade_id))

                        # Перенос SL, если задан новый уровень
                        if lvl_type == "tp" and new_sl is not None:
                            cur.execute("""
                                DELETE FROM trades_sltp
                                WHERE trade_id = %s AND type = 'sl'
                            """, (trade_id,))
                            cur.execute("""
                                INSERT INTO trades_sltp (trade_id, type, step, target_price, exit_percent, new_stop_loss)
                                VALUES (%s, 'sl', 0, %s, 100, NULL)
                            """, (trade_id, new_sl))

                        break  # обработан один уровень → ждём следующую цену

            conn.commit()
            conn.close()

        except Exception as e:
            print(f"❌ Ошибка в мониторинге сделок: {e}", flush=True)

        time.sleep(3)
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

        except Exception as e:
            print("❌ Ошибка в trade_executor:", e, flush=True)

        time.sleep(10)

if __name__ == "__main__":
    start_trade_stream()
    threading.Thread(target=monitor_active_trades, daemon=True).start()
    run_executor()
