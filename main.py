# Робот сеточной торговли (Grid Bot) для QUIK через QuikPy.
# Автор: Vitaliy Novozhilov
# Выставляет лимитные заявки по сетке вокруг базовой цены,
# обновляет базовую цену при исполнении уровней и хранит её в файле state.txt.
# Параметры робота задаются в файле settings.py.
import gc
import ast
import os
import time
import traceback
import math
from pathlib import Path
from lifecycle import WaitStatus
from decimal import Decimal

import settings as _settings
from transport import BridgeDisconnected, RemoteError, TransactionUncertain
from datetime import datetime

from QuikPy import QuikPy
from settings import CLASS, SECCODE, BASE_ASSET_CODE, ACCOUNT, CLIENT_CODE, LOT_PER_LEVEL, LEVELS, GRID_STEP, MAX_LOTS_TOTAL, START_TIME, END_TIME, POLL_MS, REQUESTS_PORT, CALLBACKS_PORT, RECONNECT_POLL_SEC, RECONNECT_DELAY_SEC, STABILIZATION_DELAY_SEC


# Глобальные переменные состояния робота
# - trans_id: счётчик транзакций для QUIK
# - position: текущая позиция по инструменту (в лотах)
# - base_price: текущая базовая цена сетки
# - prev_position: предыдущая позиция (для отслеживания изменений)
# - count_exept: счётчик подряд возникших исключений в основном цикле
# - first_start: флаг первого запуска при непустой позиции для выравнивания
# - file_name: файл для хранения базовой цены между перезапусками
# - _cached_step: кэш шага цены инструмента (загружается один раз)
trans_id = 1
position = 0
base_price = 0
prev_position = 0
count_exept = 0
first_start = True
BOT_ID = getattr(_settings, 'BOT_ID', 'bot_1')
STATE_DIR = Path(__file__).resolve().parent / 'runtime' / BOT_ID
file_name = str(STATE_DIR / 'state.txt')
_cached_step = None
GC_INTERVAL_CYCLES = 300  # Периодичность очистки мусора (в циклах, ~5 мин при POLL_MS=1)
grid_order_numbers = set()  # Only orders from the current grid; bounded by 2 * LEVELS.
grid_build_in_progress = False
_wait_status = WaitStatus()
FUNDS_RETRY_SECONDS = 60
funds_retry_after = {}  # At most two directions; monotonic deadlines.
pending_grid_orders = []  # Rejected levels of the current grid only, at most 2 * LEVELS.


class CancellationBlocked(RemoteError):
    """Definitive KILL refusal whose target still needs reconciliation."""


def wait_for_inactive_order(qp, order_num):
    """Read-only waiting: no replacement grid and no repeated KILL_ORDER."""
    last_log = None
    waiting = False
    while True:
        reason = 'статус заявки неизвестен'
        try:
            if not qp.connected:
                from gateway_control import ensure_gateway
                ensure_gateway(REQUESTS_PORT, CALLBACKS_PORT)
                qp.Reconnect()
            order = qp.GetOrderByClassNumber(CLASS, order_num)['data']
            valid = (isinstance(order, dict) and str(order.get('order_num')) == str(order_num)
                     and order.get('class_code') == CLASS and order.get('sec_code') == SECCODE
                     and order.get('account', order.get('trdaccid')) == ACCOUNT
                     and 'flags' in order)
            # Read the actual position as fills can continue during the wait.
            qty = get_current_position(qp)
            if valid and not int(order['flags']) & 1:
                if waiting:
                    print(f'{datetime.now():%Y-%m-%d %H:%M:%S} Ожидание отмены завершено: '
                          f'заявка {order_num} неактивна, позиция {qty}.')
                return
            reason = f'заявка активна, остаток {order.get("balance")}, позиция {qty}' if valid else reason
        except (RemoteError, ConnectionError, TimeoutError, OSError) as exc:
            reason = f'нет подтверждённых данных: {exc}'
        now = time.monotonic()
        if last_log is None or now - last_log >= 60:
            print(f'{datetime.now():%Y-%m-%d %H:%M:%S} Ожидание отмены {order_num}: '
                  f'{reason}. Новая сетка заблокирована; робот продолжает наблюдение.')
            last_log = now
        waiting = True
        time.sleep(2)


def is_funds_rejection(exc):
    """Only a definitive broker refusal with no created order is recoverable here."""
    reply = exc.response.get('data')
    if not isinstance(reply, dict):
        # Compatibility with an already running gateway that serializes the reply.
        try:
            reply = ast.literal_eval(exc.response.get('error', ''))
        except (ValueError, SyntaxError, TypeError):
            return False
    if not isinstance(reply, dict):
        return False
    message = str(reply.get('result_msg', '')).lower()
    return (reply.get('status') in (4, 6) and str(reply.get('order_num', 0)) == '0'
            and ('[gw][332]' in message or 'нехватка средств' in message))


def retry_pending_grid(qp):
    """Retry missing levels without cancelling accepted orders or exceeding risk limits."""
    if not pending_grid_orders or all(time.monotonic() < funds_retry_after.get(side, 0)
                                      for _, _, side in pending_grid_orders):
        return
    for price, volume, side in tuple(pending_grid_orders):
        if time.monotonic() < funds_retry_after.get(side, 0):
            continue
        qp.RefreshOrders(strict=True)
        if completed_grid_level(qp) is not None:
            return  # The normal loop must process the execution before repairs.
        orders = [o for o in qp.GetOrders(CLASS, SECCODE)['data'] if int(o['flags']) & 1]
        qty = get_current_position(qp)
        budgets = {'B': max(0, MAX_LOTS_TOTAL - qty), 'S': max(0, MAX_LOTS_TOTAL + qty)}
        occupied = set()
        for order in orders:
            order_side = 'S' if int(order['flags']) & 4 else 'B'
            budgets[order_side] -= int(order['balance'])
            occupied.add((Decimal(str(order['price'])), order_side))
        item = (price, volume, side)
        if (Decimal(str(price)), side) in occupied:
            pending_grid_orders.remove(item)
            continue
        if budgets[side] < volume:
            continue
        if not check_quik_connection(qp) or not check_time(qp) or not check_session_status(qp):
            raise RuntimeError('Торговля недоступна во время сверки сетки')
        reply = send_limit_order(qp, price, volume, side)
        if isinstance(reply, dict) and reply.get('order_num'):
            grid_order_numbers.add(str(reply['order_num']))
            pending_grid_orders.remove(item)
            print(f'{datetime.now():%Y-%m-%d %H:%M:%S} Восстановлен уровень: {side}, {volume} лотов по {price}')



def get_price_step(qp):
    global _cached_step
    if _cached_step is None:
        data = qp.GetParamEx(CLASS, SECCODE, 'SEC_PRICE_STEP')['data']
        value = float(data['param_value'])
        if int(data.get('result', 0)) != 1 or not math.isfinite(value) or value <= 0:
            raise ValueError('QUIK did not return a valid tick size')
        _cached_step = value
    return _cached_step


def get_current_price(qp):
    data = qp.GetParamEx(CLASS, SECCODE, 'LAST')['data']
    value = float(data['param_value'])
    if int(data.get('result', 0)) != 1 or not math.isfinite(value):
        raise ValueError('QUIK did not return a valid LAST price')
    return value  # Zero and negative spread prices are valid.


# def get_price_by_grid(price):
#     return round(price / GRID_STEP + 0.5) * GRID_STEP


def get_current_position(qp):
    holdings = qp.GetFuturesHoldings()['data']
    matches = [item for item in holdings
               if item.get('sec_code') == BASE_ASSET_CODE
               and item.get('trdaccid', item.get('account')) == ACCOUNT]
    if len(matches) > 1:
        raise ValueError('Ambiguous position: multiple records for the account and working leg')
    return int(matches[0]['totalnet']) if matches else 0


def write_in_file(price):
    if not math.isfinite(float(price)):
        raise ValueError('Cannot save an invalid base price')
    STATE_DIR.mkdir(parents=True, exist_ok=True)
    target = Path(file_name)
    temporary = target.with_suffix('.tmp')
    with temporary.open('w', encoding='utf-8') as stream:
        stream.write(f'{price}\n{SECCODE}\n{ACCOUNT}\n{BOT_ID}\n{BASE_ASSET_CODE}\n')
        stream.flush()
        os.fsync(stream.fileno())
    os.replace(temporary, target)


def read_from_file():
    target = Path(file_name)
    if not target.exists():
        return None
    try:
        lines = target.read_text(encoding='utf-8').splitlines()
        if len(lines) != 5 or lines[1:] != [SECCODE, ACCOUNT, BOT_ID, BASE_ASSET_CODE]:
            return None
        value = float(lines[0])
        return value if math.isfinite(value) else None
    except (ValueError, OSError):
        return None


def get_last_trade_price(qp):
    """Возвращает цену последней сделки по инструменту."""
    # Берём последний элемент из списка сделок и приводим цену к float
    trades = qp.GetTrades(CLASS, SECCODE)['data']
    if not trades or len(trades) == 0:
        return None  # Нет сделок
    last_trade = trades[-1]
    last_trade_price = float(last_trade['price'])
    return last_trade_price
        

def check_position_change_and_update(cur_pos):
    """Проверяет изменение позиции и обновляет глобальную переменную.

    Параметры:
        cur_pos (int | None): текущая позиция.

    Возвращает:
        bool: True, если позиция изменилась и обновлена; иначе False.
    """
    global position
    if position != cur_pos:
        position = cur_pos
        return True
    return False


def read_tracked_grid_order(qp, number):
    """Verify a tracked order without changing the grid or its base price."""
    order = qp.GetOrderByClassNumber(CLASS, number)['data']
    if order is None:
        order = qp.GetConfirmedUnfilledCancellation(number)['data']
    if not isinstance(order, dict):
        detail = 'QUIK не вернул заявку'
    elif str(order.get('order_num')) != number:
        detail = f'QUIK вернул другой номер: {order.get("order_num")}'
    elif (order.get('class_code') != CLASS or order.get('sec_code') != SECCODE
          or order.get('account', order.get('trdaccid')) != ACCOUNT):
        detail = 'класс, инструмент или счёт не совпадают'
    else:
        try:
            int(order['flags'])
            balance = float(order['balance'])
            if not math.isfinite(balance) or balance < 0:
                raise ValueError('invalid balance')
        except (KeyError, TypeError, ValueError, OverflowError):
            detail = 'недоступны корректные статус или остаток'
        else:
            return order
    raise TransactionUncertain(dict(error=f'Cannot verify grid order {number}; '
                                    f'{detail}; replacement stopped'))


def verify_tracked_grid_orders(qp):
    """Check missing tracked orders before reporting runtime recovery."""
    if not grid_order_numbers:
        return
    active = {str(number) for number in find_active_orders(qp)}
    for number in sorted(grid_order_numbers):
        if number not in active:
            read_tracked_grid_order(qp, number)


def completed_grid_level(qp):
    """Detect a fully filled own level even when opposite fills offset position."""
    if not grid_order_numbers:
        return None
    active = {str(number) for number in find_active_orders(qp)}
    completed = []
    for number in tuple(grid_order_numbers):
        if number in active:
            continue  # A partial fill retains its active order and remaining quantity.
        order = read_tracked_grid_order(qp, number)
        flags = int(order['flags'])
        if flags & 1:
            continue  # A stale active snapshot is not proof of execution.
        if not flags & 2 and float(order['balance']) == 0:
            completed.append(order)
        else:
            grid_order_numbers.remove(number)  # Cancelled orders are not filled levels.
    if not completed:
        return None
    # Prefer the latest actual trade when it belongs to a completed level.
    # A partial trade of another order must not overwrite this level's price.
    trades = qp.GetTrades(CLASS, SECCODE)['data']
    if isinstance(trades, list) and trades:
        trade = trades[-1]
        for order in completed:
            if str(trade.get('order_num')) == str(order['order_num']):
                return dict(order, price=trade['price'])
    # The connector retains only the latest trade; fall back to the own level.
    return max(completed, key=lambda order: int(order['order_num']))


def check_levels_executed(cur_pos, qp, filled_order=None):
    """Определяет, выполнен ли очередной уровень сетки.

    При изменении позиции на `LOT_PER_LEVEL` от последнего учтённого уровня обновляет базовую цену
    по последней сделке, обновляет `prev_position` и сохраняет цену в файл.
    При закрытии позиции (cur_pos == 0) обновляет базовую цену по текущей рыночной цене.

    Параметры:
        cur_pos (int): текущая позиция.
        qp: экземпляр QuikPy.

    Возвращает:
        bool: True, если уровень считается выполненным; иначе False.
    """
    global first_price_buy, first_price_sell, base_price, prev_position
    # Проверяем, изменилась ли позиция с последней проверки
    if cur_pos == prev_position and filled_order is None:
        return False  # Позиция не изменилась — уровень не исполнен
    # На старте может быть остаток от частичного исполнения или прежнего
    # объёма уровня. Считаем исполненный объём от последней принятой позиции,
    # не требуя кратности всей позиции. Частичные исполнения накапливаются.
    if (filled_order is not None or cur_pos == 0 or abs(cur_pos - prev_position) >= LOT_PER_LEVEL) and abs(cur_pos) <= MAX_LOTS_TOTAL:
        if filled_order is not None:
            base_price = float(filled_order['price'])
            prev_position = cur_pos
            write_in_file(base_price)
            print(f'{datetime.now():%Y-%m-%d %H:%M:%S} Уровень исполнен: заявка '
                  f'{filled_order["order_num"]}. Новая базовая цена: {base_price}')
            if cur_pos == 0:
                time.sleep(2)
            return True
        # Если позиция закрылась — обновляем базовую цену по цене последней сделки
        if cur_pos == 0:
            trade_price = get_last_trade_price(qp)
            base_price = trade_price if trade_price is not None else base_price
            print(f"\n{datetime.now().strftime('%Y-%m-%d %H:%M:%S')} Позиция закрыта. Новая базовая цена: {base_price}")
            time.sleep(2)  # Задержка 2 секунды после закрытия позиции
        elif cur_pos > 0:
            trade_price = get_last_trade_price(qp)
            if trade_price is not None:
                base_price = trade_price
                print(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')} Уровень исполнен. Новая базовая цена: {base_price}")
        elif cur_pos < 0:
            trade_price = get_last_trade_price(qp)
            if trade_price is not None:
                base_price = trade_price
                print(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')} Уровень исполнен. Новая базовая цена: {base_price}")
        # Фиксируем новую базовую цену и сохраняем её на диск
        prev_position = cur_pos
        write_in_file(base_price)
        return True
    return False


# Глобальный флаг: было ли уже сообщение о переключении на локальное время
_fallback_time_printed = False


def check_time(qp):
    """Проверяет, находится ли текущее время в торговом окне.
    Приоритет: Серверное время QUIK -> Локальное время ПК.
    """
    global _fallback_time_printed
    current_time = None
    used_server_time = False

    # 1. Пробуем получить серверное время
    try:
        res = qp.GetInfoParam('SERVERTIME')
        if res and 'data' in res and res['data']:
            server_time = str(res['data']).strip()
            parts = server_time.split(':')
            if len(parts) >= 2:
                h = f"{int(parts[0]):02d}"
                m = f"{int(parts[1]):02d}"
                current_time = f"{h}:{m}"
                used_server_time = True
                _fallback_time_printed = False  # Сброс при восстановлении
    except Exception:
        pass

    # 2. Если не вышло, берем локальное время компьютера
    if current_time is None:
        current_time = datetime.now().strftime('%H:%M')

    # Логирование при переключении на локальное время (только один раз)
    if not used_server_time and not _fallback_time_printed:
        print(f"Серверное время недоступно, используем локальное: {current_time}")
        _fallback_time_printed = True

    return START_TIME <= current_time < END_TIME


def check_quik_connection(qp):
    """Проверяет соединение QUIK с сервером.

    Возвращает:
        bool: True, если QUIK подключен к серверу; False, если отключен.
    """
    try:
        result = qp.IsConnected()
        if not result or 'data' not in result:
            return False
        return result['data'] == 1
    except (KeyError, ValueError, TypeError):
        return False


def check_session_status(qp):
    """Проверяет статус торговой сессии QUIK через параметр TRADINGPHASE.

    Возвращает:
        bool: True, если торги идут (сессия открыта); False, если клиринг/перерыв/закрытие.

    Примечание:
        Для спредов FUTSPREAD параметр TRADINGPHASE может возвращать:
        - param_image: 'открыта' или 'закрыта'
        - param_value: '1.000000' (может быть и при открытой, и при закрытой сессии)
        
        Поэтому проверяем param_image, а не param_value.
    """
    try:
        result = qp.GetParamEx(CLASS, SECCODE, 'TRADINGPHASE')
        if result and 'data' in result and int(result['data'].get('result', 0)) == 1:
            param_image = result['data'].get('param_image', '')
            # Проверяем по изображению параметра (более надёжно для спредов)
            if param_image == 'открыта':
                return True
            elif param_image == 'закрыта':
                return False
            # Если param_image не определён, пробуем проверить по param_value
            param_value = result['data'].get('param_value')
            if param_value:
                trading_phase = int(float(param_value))
                # Торгуем в фазе 1 (открытие) и 2 (основная сессия)
                return trading_phase in (1, 2)
        # Если параметр недоступен, ждём подтверждения открытия сессии
        return False
    except (KeyError, ValueError, TypeError):
        return False


def send_limit_order(qp, price, volume, side):
    """Отправляет лимитную заявку в QUIK.

    Параметры:
        qp: экземпляр QuikPy.
        price (int | float): цена заявки.
        volume (int): объём в лотах.
        side (str): направление операции ('B' — покупка, 'S' — продажа).
    """
    global trans_id
    if time.monotonic() < funds_retry_after.get(side, 0):
        return None
    trans_id += 1
    # Округляем цену до 6 знаков после запятой для избежания проблем с float
    price = round(float(price), 6)
    # Форматируем цену: убираем лишние нули после запятой (249.0 -> 249, 0.230000 -> 0.23)
    tick = Decimal(str(get_price_step(qp)))
    if Decimal(str(price)) % tick != 0:
        raise ValueError('Order price is not aligned to the instrument tick')
    price_str = format(price, '.6f').rstrip('0').rstrip('.') or '0'
    # Формируем словарь транзакции. Все значения должны быть строками
    transaction = {  # Все значения должны передаваться в виде строк
        'TRANS_ID': str(trans_id),  # Номер транзакции задается клиентом
        'CLIENT_CODE': CLIENT_CODE,  # Код клиента. Для фьючерсов его нет
        'ACCOUNT': ACCOUNT,  # Счет
        'ACTION': 'NEW_ORDER',  # Тип заявки: Новая лимитная/рыночная заявка
        'CLASSCODE': CLASS,  # Код площадки
        'SECCODE': SECCODE,  # Код тикера
        'OPERATION': side,  # B = покупка, S = продажа
        'PRICE': price_str,  # Цена исполнения. Для рыночных фьючерсных заявок наихудшая цена в зависимости от направления. Для остальных рыночных заявок цена = 0
        'QUANTITY': str(volume),  # Кол-во в лотах
        'TYPE': 'L'}  # L = лимитная заявка (по умолчанию), M = рыночная заявка
    try:
        qp.SendTransaction(transaction)
        return qp.WaitTransaction(transaction['TRANS_ID'])['data']
    except TransactionUncertain:
        raise  # Never retry an order with an unknown outcome.
    except RemoteError as exc:
        if not is_funds_rejection(exc):
            raise
        funds_retry_after[side] = time.monotonic() + FUNDS_RETRY_SECONDS
        print(f'{datetime.now():%Y-%m-%d %H:%M:%S} QUIK отклонил заявку: недостаточно средств; '
              f'{side}, {volume} лотов по {price}. Повтор не ранее чем через {FUNDS_RETRY_SECONDS} сек.')
        return None


def cancel_order(qp, order_num):
    """Отменяет заявку по её номеру.

    Параметры:
        qp: экземпляр QuikPy.
        order_num (int | str): номер заявки (ORDER_KEY).
    """
    global trans_id
    trans_id += 1
    # Формируем транзакцию на снятие заявки
    transaction = {
        'TRANS_ID': str(trans_id),  # Номер транзакции задается клиентом
        'ACTION': 'KILL_ORDER',  # Тип заявки: Удаление существующей заявки
        'CLASSCODE': CLASS,  # Код площадки
        'SECCODE': SECCODE,  # Код тикера
        'ORDER_KEY': str(order_num)}  # Номер заявки
    try:
        qp.SendTransaction(transaction)
        qp.WaitTransaction(transaction['TRANS_ID'])
    except TransactionUncertain:
        raise  # An unknown transaction result must not be retried or ignored.
    except RemoteError:
        # An order may fill after the active snapshot but before KILL_ORDER.
        # A rejection alone never proves that it is safe to replace the grid.
        # The rejection can arrive before QUIK's order table reflects the fill.
        # Poll this exact order briefly; never resubmit KILL_ORDER.
        deadline = time.monotonic() + 3
        for attempt in range(16):
            order = qp.GetOrderByClassNumber(CLASS, order_num)['data']
            if (not isinstance(order, dict) or str(order.get('order_num')) != str(order_num)
                    or order.get('class_code') != CLASS or order.get('sec_code') != SECCODE
                    or order.get('account', order.get('trdaccid')) != ACCOUNT
                    or 'flags' not in order):
                raise CancellationBlocked(dict(error=f'Cancellation rejected; cannot verify order {order_num}'))
            if not int(order['flags']) & 1:
                break
            if time.monotonic() >= deadline or attempt == 15:
                print(f'{datetime.now():%Y-%m-%d %H:%M:%S} Отмена заявки {order_num} отклонена; '
                      'после ожидания заявка всё ещё активна. Перестройка остановлена.')
                raise CancellationBlocked(dict(error=f'Order {order_num} remains active after cancellation refusal'))
            time.sleep(0.2)
        qp.RefreshOrders()
        print(f'{datetime.now():%Y-%m-%d %H:%M:%S} Отмена заявки {order_num} отклонена, '
              'но QUIK подтвердил, что она уже неактивна. Состояние заявок сверено.')


def find_active_orders(qp):
    """Возвращает список номеров активных заявок по инструменту.

    Параметры:
        qp: экземпляр QuikPy.

    Возвращает:
        list[int]: список номеров активных заявок.
    """
    orders = qp.GetOrders(CLASS, SECCODE)['data']  # Все заявки по классу и тикеру
    active_orders = [order for order in orders if order['flags'] & 0b1 == 0b1]  # Активные заявки
    active_orders_nums = [order['order_num'] for order in active_orders]
    return active_orders_nums


def cancel_all_orders(qp):
    """Cancel each observed order once, then wait for exact terminal statuses."""
    qp.RefreshOrders()
    attempted = set()
    while True:
        targets = find_active_orders(qp)
        if not targets:
            return
        for order_num in targets:
            if str(order_num) not in attempted:
                attempted.add(str(order_num))
                try:
                    cancel_order(qp, order_num)
                except CancellationBlocked:
                    # A confirmed refusal is recoverable; never replay its transaction.
                    pass
            wait_for_inactive_order(qp, order_num)
        # Keep observing if transport fails after confirmed cancellations, too.
        while True:
            try:
                qp.RefreshOrders()
                break
            except (RemoteError, ConnectionError, TimeoutError, OSError):
                # Exact order reads also restore the connection when necessary.
                wait_for_inactive_order(qp, targets[-1])
                time.sleep(2)
        time.sleep(0.2)


def wait_until_tradable(qp):
    """Recheck all gates after every reconnect/stabilization delay."""
    reconnect_wait = False
    session_wait = False
    while True:
        if not check_quik_connection(qp):
            reconnect_wait = True
            time.sleep(POLL_MS * RECONNECT_POLL_SEC)
            continue
        if not check_time(qp) or not check_session_status(qp):
            session_wait = True
            time.sleep(POLL_MS)
            continue
        if reconnect_wait:
            time.sleep(RECONNECT_DELAY_SEC)
            reconnect_wait = False
            continue
        if session_wait:
            time.sleep(STABILIZATION_DELAY_SEC)
            session_wait = False
            continue
        return


def has_duplicate_orders(qp):
    orders = [o for o in qp.GetOrders(CLASS, SECCODE)['data'] if int(o['flags']) & 1]
    # Side is bit 2 of QUIK order flags. Partial fills keep the same order.
    keys = [(Decimal(str(o['price'])), int(o['flags']) & 4) for o in orders]
    return (len(orders) > LEVELS * 2 or len(keys) != len(set(keys))
            or any(sum(side == direction for _, side in keys) > LEVELS for direction in (0, 4)))


def reconcile_grid(qp, price, position_now):
    """Repair missing levels against a fresh QUIK snapshot, preserving partial orders."""
    orders = [o for o in qp.GetOrders(CLASS, SECCODE)['data'] if int(o['flags']) & 1]
    if not orders:
        retry_pending_grid(qp)
        return  # The existing empty-grid path performs a full, gated rebuild.
    desired = {}
    for level in range(1, LEVELS + 1):
        for side, sign in (('B', -1), ('S', 1)):
            level_price = round(float(price + sign * level * GRID_STEP), 6)
            desired[(Decimal(str(level_price)), side)] = level_price
    occupied = set()
    budgets = {'B': max(0, MAX_LOTS_TOTAL - position_now),
               'S': max(0, MAX_LOTS_TOTAL + position_now)}
    for order in orders:
        try:
            side = 'S' if int(order['flags']) & 4 else 'B'
            key = (Decimal(str(order['price'])), side)
            quantity = Decimal(str(order['qty']))
            balance = Decimal(str(order['balance']))
            number = str(order['order_num'])
            if (not key[0].is_finite() or not quantity.is_finite() or not balance.is_finite() or quantity <= 0
                    or balance <= 0 or balance > quantity or not number.isdigit()):
                raise ValueError('invalid order fields')
        except (KeyError, TypeError, ValueError, ArithmeticError) as exc:
            raise TransactionUncertain(dict(error='QUIK returned an invalid active order; grid reconciliation stopped')) from exc
        if key not in desired or key in occupied or quantity != LOT_PER_LEVEL:
            print(f'{datetime.now():%Y-%m-%d %H:%M:%S} Сетка не соответствует заявкам QUIK; сверяем и перестраиваем.')
            set_grid(qp, price)
            return
        occupied.add(key)
        budgets[side] -= balance
        grid_order_numbers.add(number)
    if any(value < 0 for value in budgets.values()):
        print(f'{datetime.now():%Y-%m-%d %H:%M:%S} Остатки заявок превышают лимит позиции; перестраиваем сетку.')
        set_grid(qp, price)
        return
    pending = {(Decimal(str(round(float(p), 6))), side) for p, _, side in pending_grid_orders}
    for key, level_price in desired.items():
        _, side = key
        if key in occupied:
            continue
        if budgets[side] >= LOT_PER_LEVEL:
            if key not in pending:
                pending_grid_orders.append((level_price, LOT_PER_LEVEL, side))
                pending.add(key)
            budgets[side] -= LOT_PER_LEVEL
    retry_pending_grid(qp)


def set_grid(qp, price):
    """Выставляет сетку лимитных заявок вокруг заданной цены.

    Логика:
        - Перед выставлением сетки отменяет все активные заявки.
        - Если позиция нулевая — строит симметричную сетку от заданной цены.
        - Если позиция не нулевая — учитывает ограничение MAX_LOTS_TOTAL.

    Параметры:
        qp: экземпляр QuikPy.
        price (float): базовая цена сетки.
    """
    global base_price, grid_build_in_progress
    if not math.isfinite(float(price)):
        raise ValueError('Invalid grid base price')
    grid_build_in_progress = True
    wait_until_tradable(qp)
    cancel_all_orders(qp)
    grid_order_numbers.clear()
    pending_grid_orders.clear()
    wait_until_tradable(qp)
    qty = get_current_position(qp)  # A fill may have occurred while cancelling.
    buy_budget = max(0, MAX_LOTS_TOTAL - qty)
    sell_budget = max(0, MAX_LOTS_TOTAL + qty)
    buy_levels = min(LEVELS, buy_budget // LOT_PER_LEVEL)
    sell_levels = min(LEVELS, sell_budget // LOT_PER_LEVEL)
    if buy_levels < LEVELS or sell_levels < LEVELS:
        print(f'{datetime.now():%Y-%m-%d %H:%M:%S} Лимит позиции ограничивает сетку: '
              f'позиция {qty}, лимит ±{MAX_LOTS_TOTAL}, уровней покупки {buy_levels}, '
              f'продажи {sell_levels}, объём уровня {LOT_PER_LEVEL}')
    for level in range(1, LEVELS + 1):
        if buy_budget >= LOT_PER_LEVEL:
            reply = send_limit_order(qp, price - level * GRID_STEP, LOT_PER_LEVEL, "B")
            if isinstance(reply, dict) and reply.get('order_num'):
                grid_order_numbers.add(str(reply['order_num']))
            elif reply is None:
                pending_grid_orders.append((price - level * GRID_STEP, LOT_PER_LEVEL, 'B'))
            buy_budget -= LOT_PER_LEVEL
        if sell_budget >= LOT_PER_LEVEL:
            reply = send_limit_order(qp, price + level * GRID_STEP, LOT_PER_LEVEL, "S")
            if isinstance(reply, dict) and reply.get('order_num'):
                grid_order_numbers.add(str(reply['order_num']))
            elif reply is None:
                pending_grid_orders.append((price + level * GRID_STEP, LOT_PER_LEVEL, 'S'))
            sell_budget -= LOT_PER_LEVEL
    if qty == 0:
        base_price = price

    grid_build_in_progress = False


def check_base_price_by_grid(qp, price, cur_pos):
    """Проверяет необходимость перестройки сетки и выполняет выравнивание на старте.

    Логика:
        1. Если позиция в плюсе (цена >= begin_grid_price) и заявок нет — выставляем заявку на закрытие.
        2. Если цена вне канала (±GRID_STEP) — ждём возврата.
        3. Если цена в канале — нужна сетка.

    Параметры:
        qp: экземпляр QuikPy.
        price (float): текущая рыночная цена.
        cur_pos (int): текущая позиция (передаётся из цикла, чтобы не запрашивать её повторно).

    Возвращает:
        tuple: (need_grid, close_position, position_closed) — нужна ли сетка, закрыли ли позицию, позиция закрыта.
    """
    global base_price, prev_position
    if cur_pos == 0:
        # Позиция нулевая — проверяем, только что закрылась или уже была нулевой
        was_closed = prev_position != 0  # True если позиция изменилась
        prev_position = cur_pos
        # Обновляем базовую цену по цене последней сделки
        if was_closed:
            trade_price = get_last_trade_price(qp)
            if trade_price is not None:
                base_price = trade_price
            time.sleep(2)
        # A saved anchor remains binding while flat. A session gap is not a
        # completed level and must not open a new position outside the channel.
        return (was_closed or abs(price - base_price) <= GRID_STEP, False, was_closed)

    # 1. Проверка на закрытие прибыльной позиции
    if cur_pos > 0:
        # Длинная позиция: закрываем, если цена выше begin_grid_price
        begin_grid_price = (cur_pos // LOT_PER_LEVEL) * GRID_STEP + base_price
        if price >= begin_grid_price:
            # Проверяем, есть ли уже активные заявки
            orders = find_active_orders(qp)
            if len(orders) == 0:
                # Заявок нет — выставляем на закрытие
                bid_result = qp.GetParamEx(CLASS, SECCODE, "BID")
                if bid_result and 'data' in bid_result and int(bid_result['data'].get('result', 0)) == 1:
                    bid_price = float(bid_result['data'].get('param_value', 0))
                    # Получаем шаг цены для корректного расчёта цены заявки
                    price_step = get_price_step(qp)
                    # Продажа на один тик выше BID: немедленное исполнение не гарантируется.
                    price_to_order = round(bid_price + price_step, 6)
                    if price_to_order >= begin_grid_price and math.isfinite(bid_price):
                        reply = send_limit_order(qp, price_to_order, abs(cur_pos),  "S")
                        if reply is None:
                            return (False, True, False)
                        print(f"\n{datetime.now().strftime('%Y-%m-%d %H:%M:%S')} Выставлена заявка на закрытие позиции: {abs(cur_pos)} лотов по {price_to_order}")
                        return (False, True, False)  # Заявка выставлена, ждём исполнения
            # Заявка уже есть — ждём исполнения
            return (False, True, False)
    elif cur_pos < 0:
        # Короткая позиция: закрываем, если цена ниже begin_grid_price
        begin_grid_price = base_price - (abs(cur_pos) // LOT_PER_LEVEL) * GRID_STEP
        if price <= begin_grid_price:
            # Проверяем, есть ли уже активные заявки
            orders = find_active_orders(qp)
            if len(orders) == 0:
                # Заявок нет — выставляем на закрытие
                offer_result = qp.GetParamEx(CLASS, SECCODE, "OFFER")
                if offer_result and 'data' in offer_result and int(offer_result['data'].get('result', 0)) == 1:
                    offer_price = float(offer_result['data'].get('param_value', 0))
                    # Получаем шаг цены для корректного расчёта цены заявки
                    price_step = get_price_step(qp)
                    # Покупка на один тик ниже OFFER: немедленное исполнение не гарантируется.
                    price_to_order = round(offer_price - price_step, 6)
                    if price_to_order <= begin_grid_price and math.isfinite(offer_price):
                        reply = send_limit_order(qp, price_to_order, abs(cur_pos),  "B")
                        if reply is None:
                            return (False, True, False)
                        print(f"\n{datetime.now().strftime('%Y-%m-%d %H:%M:%S')} Выставлена заявка на закрытие позиции: {abs(cur_pos)} лотов по {price_to_order}")
                        return (False, True, False)  # Заявка выставлена, ждём исполнения
            # Заявка уже есть — ждём исполнения
            return (False, True, False)

    # 2. Позиция в минусе или в плюсе, но не достигла уровня закрытия
    if abs(price - base_price) > GRID_STEP:
        # Цена за пределами канала — ждём возврата, сетка не нужна
        return (False, False, False)
    else:
        # Цена в канале (±GRID_STEP) — нужна сетка
        return (True, False, False)


def registration_ready(registration):
    """Read-only validation; an unresolved transaction blocks new trading."""
    from run_bot import validate_tick
    validate_tick(_settings, registration)
    if registration['foreign_active_orders'] or registration['unresolved_transactions']:
        raise RuntimeError(f'Торговля заблокирована: чужих заявок '
                           f'{registration["foreign_active_orders"]}, неопределённых транзакций '
                           f'{registration["unresolved_transactions"]}. '
                           'Выполняется только сверка; для старой неопределённости — recover_transaction.py.')


def connect_and_register():
    """Wait for gateway, terminal metadata and ledger readiness without sending orders."""
    from gateway_control import ensure_gateway
    while True:
        client = None
        try:
            ensure_gateway(REQUESTS_PORT, CALLBACKS_PORT)
            client = QuikPy(requests_port=REQUESTS_PORT, callbacks_port=CALLBACKS_PORT)
            registration = client.RegisterBot(BOT_ID, CLASS, SECCODE, BASE_ASSET_CODE, ACCOUNT)['data']
            registration_ready(registration)
            if _wait_status.message is not None:
                print(f'{datetime.now():%Y-%m-%d %H:%M:%S} QUIK готов для {CLASS}/{SECCODE}; продолжаем запуск.')
            _wait_status.clear()
            return client, registration
        except KeyboardInterrupt:
            if client is not None:
                try:
                    if client.connected and getattr(client, '_registration', None):
                        cancel_all_orders(client)
                except Exception as exc:
                    print(f'Не удалось подтвердить снятие заявок при остановке: {exc}')
                finally:
                    client.CloseConnectionAndThread()
            raise
        except Exception as exc:
            if client is not None:
                client.CloseConnectionAndThread()
            _wait_status.show(f'Ожидание готовности QUIK для {CLASS}/{SECCODE}: {exc}. '
                              f'Повторная проверка через {RECONNECT_DELAY_SEC} сек. '
                              'Новые заявки не отправляются. Выход — Ctrl+C.')
            time.sleep(RECONNECT_DELAY_SEC)


def wait_for_runtime_ready(qp, error):
    """Keep the process alive while reconciling; never replay the failed request."""
    global _cached_step
    from gateway_control import ensure_gateway
    reason = error
    while True:
        _wait_status.show(f'Работа приостановлена: {reason}. '
                          f'Повторная сверка через {RECONNECT_DELAY_SEC} сек.; новые заявки заблокированы.')
        time.sleep(RECONNECT_DELAY_SEC)
        try:
            ensure_gateway(REQUESTS_PORT, CALLBACKS_PORT)
            if not qp.connected:
                qp.Reconnect()
            registration = qp.RegisterBot(BOT_ID, CLASS, SECCODE, BASE_ASSET_CODE, ACCOUNT)['data']
            registration_ready(registration)
            if not check_quik_connection(qp):
                raise RuntimeError('QUIK не подключён к серверу')
            if not check_time(qp):
                raise RuntimeError(f'Вне рабочего времени {START_TIME}–{END_TIME}')
            if not check_session_status(qp):
                raise RuntimeError('Торговая сессия закрыта или её статус пока недоступен')
            qp.RefreshOrders(strict=True)
            get_current_position(qp)
            get_current_price(qp)
            verify_tracked_grid_orders(qp)
            _cached_step = float(registration['tick']['param_value'])
            print(f'{datetime.now():%Y-%m-%d %H:%M:%S} Данные и заявки сверены; возобновляем работу.')
            _wait_status.clear()
            return
        except Exception as exc:
            reason = exc


if __name__ == "__main__":
    try:
        qp, registration = connect_and_register()
    except KeyboardInterrupt:
        print('Подключение остановлено пользователем')
        raise SystemExit(0)
    stop_requested = False
    print("\n\n=============================================")
    print(f"Grid bot is running for {SECCODE}")
    print(f"Connected to QuikPy on ports {REQUESTS_PORT}/{CALLBACKS_PORT}")

    try:
        # Даём QUIK время на получение актуальных данных после подключения
        print(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')} Получение данных от QUIK...")
        time.sleep(3)
        
        # Проверяем, что QUIK возвращает актуальные данные
        try:
            server_time = qp.GetInfoParam('SERVERTIME')['data']
            trade_date = qp.GetInfoParam('TRADEDATE')['data']
            print(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')} Серверное время QUIK: {trade_date} {server_time}")
        except Exception as e:
            print(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')} Предупреждение: не удалось получить время QUIK: {e}")
        
        # Ждём наступления торгового окна START_TIME..END_TIME
        last_status = None
        waited_for_trading = False  # Флаг: ждали ли мы начала торгов
        while not check_time(qp) or not check_session_status(qp):
            waited_for_trading = True  # Мы вошли в цикл ожидания
            # Проверяем каждое условие отдельно для корректного сообщения
            is_time_ok = check_time(qp)
            is_session_ok = check_session_status(qp)

            if not is_session_ok:
                status_msg = "клиринг/перерыв"
            elif not is_time_ok:
                status_msg = "внерабочее время"
            else:
                break  # Оба условия выполнены

            if status_msg != last_status:
                print(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')} Ожидание: {status_msg}...")
                last_status = status_msg
            time.sleep(POLL_MS)
        print()  # Новая строка после ожидания
        
        # Задержка 15 секунд только если мы ждали начала торгового времени
        if waited_for_trading:
            print(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')} Торговое время началось. Стабилизация данных ({STABILIZATION_DELAY_SEC} сек)...")
            time.sleep(STABILIZATION_DELAY_SEC)
        
        # Инициализация текущих значений цены и позиции
        current_price = get_current_price(qp)
        local_base_price = read_from_file()
        cur_pos = get_current_position(qp)
        # Используем позицию как исходную для prev_position
        prev_position = cur_pos
        position = cur_pos
        if cur_pos is None:
            cur_pos = 0
            prev_position = cur_pos
            print(f"Current position: {cur_pos}")
        # Если есть базовая цена в файле — восстановим сетку относительно неё
        if local_base_price is not None:
            base_price = local_base_price
            print(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')} Базовая цена восстановлена из файла: {base_price}")
            # Сохранённая база требует возврата цены и при нулевой позиции.
            last_wait_status = None
            wait_printed = False
            position_closed = False  # Флаг закрытия позиции
            while True:
                wait_until_tradable(qp)
                current_price = get_current_price(qp)
                # Получаем актуальную позицию (один раз за итерацию)
                cur_pos = get_current_position(qp)

                # Проверяем режим работы
                need_grid, close_position, pos_closed = check_base_price_by_grid(qp, current_price, cur_pos)
                
                # Если позиция закрылась — выходим
                if pos_closed:
                    position_closed = True
                    break
                
                # Если нужна сетка (цена в канале) — выходим
                if need_grid:
                    break
                
                # Цена вне канала — ждём возврата
                # Проверяем соединение с QUIK
                if not check_quik_connection(qp):
                    print(f"\n{datetime.now().strftime('%Y-%m-%d %H:%M:%S')} QUIK отключен от сервера. Ожидание подключения...")
                    while not check_quik_connection(qp):
                        time.sleep(POLL_MS * RECONNECT_POLL_SEC)
                    print(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')} QUIK подключен к серверу — возобновляем работу...")
                    time.sleep(RECONNECT_DELAY_SEC)  # Даём время на обновление данных
                    # После подключения проверяем актуальную цену
                    current_price = get_current_price(qp)
                    continue

                # Проверяем статус сессии и выводим сообщение
                if not check_session_status(qp):
                    wait_status = "клиринг"
                elif close_position:
                    wait_status = "Режим закрытия позиции: ожидание исполнения активных заявок"
                else:
                    wait_status = f"Ожидание возврата цены к {base_price} для построения сетки"
                if wait_status != last_wait_status:
                    print(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')} {wait_status}...")
                    last_wait_status = wait_status
                    wait_printed = True
                current_price = get_current_price(qp)
                time.sleep(POLL_MS)

            if position_closed:
                # Позиция закрылась — базовая цена уже обновлена, сетка выставится ниже
                print(f"\n{datetime.now().strftime('%Y-%m-%d %H:%M:%S')} Позиция закрыта. Новая базовая цена: {base_price}")
            elif wait_printed:
                print(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')} Цена вернулась к базовой — строим сетку\n")
        else:
            base_price = get_current_price(qp)
            print(f'Base price initialized: {base_price}')

        # Save the anchor before the first send so an early failure cannot reset it.
        write_in_file(base_price)
        set_grid(qp, base_price)
        print(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')} Сетка выставлена. Базовая цена: {base_price}\n")
        time.sleep(POLL_MS * 2)

        # Основной цикл работы робота: следим за временем, состоянием позиций и активными заявками
        in_session_wait = False
        connection_lost = False
        time_wait = False
        reconciliation_due = False
        cycle_counter = 1  # Счётчик циклов для проверки дублирования заявок
        waiting_for_price = False
        
        gc_counter = 0  # Счётчик для периодической очистки мусора
        while True:
            try:
                # Раз в 300 циклов (~5 мин) очищаем мусор Python
                gc_counter += 1
                if gc_counter >= GC_INTERVAL_CYCLES:
                    gc.collect()
                    gc_counter = 0

                # Проверяем торговое время — если вне окна, просто ждём
                if not check_time(qp):
                    time_wait = True
                    reconciliation_due = True
                    time.sleep(POLL_MS)
                    continue

                if time_wait:
                    time_wait = False
                    print(f'{datetime.now():%Y-%m-%d %H:%M:%S} Торговое время наступило; стабилизация данных.')
                    time.sleep(STABILIZATION_DELAY_SEC)
                    continue

                # Проверяем соединение QUIK с сервером
                if not check_quik_connection(qp):
                    reconciliation_due = True
                    if not connection_lost:
                        print(f"\n{datetime.now().strftime('%Y-%m-%d %H:%M:%S')} QUIK отключен от сервера. Ожидание подключения...")
                        connection_lost = True
                    time.sleep(POLL_MS * RECONNECT_POLL_SEC)
                    continue

                if connection_lost:
                    connection_lost = False
                    print(f"\n{datetime.now().strftime('%Y-%m-%d %H:%M:%S')} QUIK подключен к серверу — возобновляем работу...")
                    # После подключения даем время на получение актуальных данных
                    time.sleep(RECONNECT_DELAY_SEC)
                    wait_until_tradable(qp)
                    qp.RefreshOrders(strict=True)
                    verify_tracked_grid_orders(qp)
                    reconciliation_due = True
                    # Проверяем, не закрылась ли позиция во время разрыва
                    cur_pos_after_connect = get_current_position(qp)
                    if cur_pos_after_connect == 0 and position != 0:
                        # Позиция закрылась во время разрыва — обновляем базовую цену
                        trade_price = get_last_trade_price(qp)
                        if trade_price is not None:
                            base_price = trade_price
                            write_in_file(base_price)
                            print(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')} Позиция закрыта во время разрыва. Новая базовая цена: {base_price}")
                        prev_position = 0
                        position = 0
                    # Nonzero changes must be processed by check_levels_executed.
                    # Не проверяем время сразу после подключения — даём QUIK время на обновление
                    continue

                # Проверяем статус сессии — если закрыта (клиринг), ждём
                if not check_session_status(qp):
                    reconciliation_due = True
                    if not in_session_wait:
                        print(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')} Сессия закрыта (клиринг/перерыв). Ожидание...")
                        in_session_wait = True
                    time.sleep(POLL_MS)
                    continue
                if in_session_wait:
                    in_session_wait = False
                    print(f"\n{datetime.now().strftime('%Y-%m-%d %H:%M:%S')} Сессия открыта — возобновляем торговлю...")
                    # Задержка стабилизации данных после клиринга
                    print(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')} Стабилизация данных после клиринга ({STABILIZATION_DELAY_SEC} сек)...")
                    time.sleep(STABILIZATION_DELAY_SEC)
                    reconciliation_due = True
                    # Восстановление пройдёт в обычном цикле с ожиданием возврата цены.
                    continue

                # Reconcile immediately after any observed pause, before trading.
                orders_reconciled = False
                if reconciliation_due:
                    qp.RefreshOrders(strict=True)
                    verify_tracked_grid_orders(qp)
                    reconciliation_due = False
                    orders_reconciled = True

                # Сверяем заявки после задержек восстановления и проверки сессии.
                if cycle_counter >= 10:
                    cycle_counter = 0
                    qp.RefreshOrders(strict=True)
                    orders_reconciled = True
                    if has_duplicate_orders(qp):
                        print(f"\n{datetime.now().strftime('%Y-%m-%d %H:%M:%S')} Обнаружены дубли заявок или превышение числа уровней. Отменяем все...")
                        cancel_all_orders(qp)

                if check_time(qp):
                    cur_pos = get_current_position(qp)
                    current_price = get_current_price(qp)

                    # Ждём, если базовая цена ещё не получена
                    if base_price is None:
                        time.sleep(POLL_MS)
                        continue

                    # Inspect completed own orders before any empty-grid recovery.
                    # Opposite fills can leave the net position unchanged.
                    filled_order = completed_grid_level(qp)
                    if filled_order is not None and check_levels_executed(cur_pos, qp, filled_order):
                        check_position_change_and_update(cur_pos)
                        set_grid(qp, base_price)
                        print(f'{datetime.now():%Y-%m-%d %H:%M:%S} Сетка переставлена после исполнения уровня')
                        cycle_counter += 1
                        time.sleep(POLL_MS)
                        continue

                    # Проверяем режим работы: закрытие прибыли или сетка
                    need_grid, close_position, pos_closed = check_base_price_by_grid(qp, current_price, cur_pos)
                    price_wait = not (need_grid or close_position or pos_closed)
                    if price_wait and not waiting_for_price:
                        print(f'{datetime.now():%Y-%m-%d %H:%M:%S} Ожидание возврата цены: '
                              f'цена {current_price}, база {base_price}, канал '
                              f'[{base_price - GRID_STEP}; {base_price + GRID_STEP}], позиция {cur_pos}')
                    elif waiting_for_price and not price_wait:
                        print(f'{datetime.now():%Y-%m-%d %H:%M:%S} Ожидание возврата цены завершено')
                    waiting_for_price = price_wait
                    
                    if pos_closed:
                        # Позиция только что закрылась — обновляем базовую цену и выставляем сетку (один раз)
                        write_in_file(base_price)  # Сохраняем обновлённую базовую цену
                        set_grid(qp, base_price)
                        print(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')} Позиция закрыта. Сетка выставлена с базовой ценой: {base_price}")
                        position = 0  # Обновляем глобальную позицию
                    elif close_position:
                        # Заявка на закрытие выставлена — проверяем исполнение
                        if cur_pos == 0:
                            # Позиция закрылась — обновляем базовую цену и выставляем сетку
                            trade_price = get_last_trade_price(qp)
                            if trade_price is not None:
                                base_price = trade_price
                                write_in_file(base_price)
                                print(f"\n{datetime.now().strftime('%Y-%m-%d %H:%M:%S')} Заявка на закрытие исполнилась. Новая базовая цена: {base_price}")
                            set_grid(qp, base_price)
                            print(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')} Сетка выставлена после исполнения заявки")
                        # else: продолжаем ждать исполнения
                    elif need_grid:
                        if grid_build_in_progress:
                            set_grid(qp, base_price)
                            print(f'{datetime.now():%Y-%m-%d %H:%M:%S} Незавершённая сетка восстановлена после сверки.')
                        if orders_reconciled:
                            reconcile_grid(qp, base_price, cur_pos)
                        else:
                            retry_pending_grid(qp)
                        # Цена в канале — проверяем, есть ли заявки
                        orders = find_active_orders(qp)
                        if len(orders) == 0 and not pending_grid_orders:
                            # Заявок нет — выставляем сетку
                            set_grid(qp, base_price)
                            print(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')} Сетка выставлена/восстановлена")
                        # else: заявки уже есть — ничего не делаем
                    # else: цена вне канала — ждём возврата, ничего не делаем
                        
                    # Проверяем, не изменилась ли позиция (уровень сетки исполнился)
                    check_position_change_and_update(cur_pos)
                    if check_levels_executed(cur_pos, qp):
                        # Уровень сетки исполнился — переставляем сетку
                        set_grid(qp, base_price)
                        print(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')} Сетка переставлена. Базовая цена: {base_price}\n")
                
                # Увеличиваем счётчик циклов
                cycle_counter += 1
                count_exept = 0
                time.sleep(POLL_MS)
                
            except Exception as exc:
                wait_for_runtime_ready(qp, exc)
                connection_lost = True  # Preserve the existing post-recovery stabilization delay.
                reconciliation_due = True
                count_exept = 0
                continue
    except Exception:
        raise  # The launcher waits and retries read-only initialization after early failures.
    except KeyboardInterrupt:
        stop_requested = True
        print('Робот остановлен пользователем')
    finally:
        # Перед выходом снимаем все заявки и закрываем соединение с QUIK
        try:
            if stop_requested and qp.connected:
                cancel_all_orders(qp)
        except Exception as cleanup_error:
            print(f'Orders could not be confirmed cancelled: {cleanup_error}')
        finally:
            qp.CloseConnectionAndThread()
