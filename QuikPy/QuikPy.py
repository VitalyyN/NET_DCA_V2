import logging
import socket
import threading
import time

from transport import (BridgeDisconnected, JsonLineSocket, RemoteError,
                       TransactionUncertain, close_socket, REGISTRATION_TIMEOUT_SECONDS,
                       TRANSACTION_CONFIRM_TIMEOUT_SECONDS)

LOGGER = logging.getLogger(__name__)

class QuikPy:
    """QUIK API with bounded frames, deadlines, and explicit socket ownership."""
    buffer_size = 65536
    EVENTS = ('OnFirm', 'OnAllTrade', 'OnTrade', 'OnOrder', 'OnAccountBalance',
              'OnFuturesLimitChange', 'OnFuturesLimitDelete', 'OnFuturesClientHolding',
              'OnMoneyLimit', 'OnMoneyLimitDelete', 'OnDepoLimit', 'OnDepoLimitDelete',
              'OnAccountPosition', 'OnStopOrder', 'OnTransReply', 'OnParam', 'OnQuote',
              'OnDisconnected', 'OnConnected', 'OnClose', 'OnStop', 'OnInit',
              'OnNewCandle', 'OnError', 'OnBridgeMetrics')

    def __init__(self, host='127.0.0.1', requests_port=34130, callbacks_port=34131,
                 request_timeout=10.0, event_handler=None):
        self.Host, self.RequestsPort, self.CallbacksPort = host, requests_port, callbacks_port
        self.request_timeout = request_timeout
        self.socket_requests = self.socket_callbacks = None
        self.callback_thread = None
        self._closed = threading.Event()
        self._request_lock = threading.Lock()
        self._registration = None
        self._event_handler = event_handler
        self._callback_error = None
        for name in self.EVENTS:
            setattr(self, name, self.DefaultHandler)
        try:
            self._open()
        except Exception:
            self.CloseConnectionAndThread()
            raise

    def _open(self):
        self._closed.clear()
        self._callback_error = None
        self.socket_requests = socket.create_connection((self.Host, self.RequestsPort), timeout=5)
        self.socket_requests.settimeout(0.5)
        self._requests = JsonLineSocket(self.socket_requests)
        self.socket_callbacks = socket.create_connection((self.Host, self.CallbacksPort), timeout=5)
        self.socket_callbacks.settimeout(0.5)
        self._callbacks = JsonLineSocket(self.socket_callbacks)
        self.callback_thread = threading.Thread(target=self.callback_handler,
                                               name='QuikPyCallbacks', daemon=True)
        self.callback_thread.start()

    @property
    def connected(self):
        return not self._closed.is_set() and self._callback_error is None

    def DefaultHandler(self, data):
        pass

    def callback_handler(self):
        try:
            while not self._closed.is_set():
                try:
                    event = self._callbacks.read()
                except socket.timeout:
                    continue
                if self._event_handler:
                    self._event_handler(event)
                name = event['cmd']
                if name == 'NewCandle':
                    name = 'OnNewCandle'
                elif name == 'lua_error':
                    name = 'OnError'
                getattr(self, name, self.DefaultHandler)(event)
        except Exception as exc:
            if not self._closed.is_set():
                self._callback_error = exc
                LOGGER.warning('QUIK callback channel stopped: %s', exc)
        finally:
            self._closed.set()
            close_socket(self.socket_requests)
            close_socket(self.socket_callbacks)

    def process_request(self, request):
        with self._request_lock:
            if not self.connected:
                raise BridgeDisconnected('QUIK transport is disconnected')
            try:
                self._requests.send(request)
                timeout = (max(self.request_timeout, REGISTRATION_TIMEOUT_SECONDS)
                           if request['cmd'] == 'GatewayRegisterBot' else self.request_timeout)
                if request['cmd'] == 'GatewayWaitTransaction':
                    timeout = max(timeout,TRANSACTION_CONFIRM_TIMEOUT_SECONDS)
                response = self._requests.read(time.monotonic() + timeout)
                if response.get('id') != request.get('id'):
                    raise BridgeDisconnected('Unexpected response id; resynchronization required')
            except (ConnectionError, TimeoutError, OSError):
                self.CloseConnectionAndThread()
                raise
            if response.get('cmd') == 'gateway_disconnected':
                self.CloseConnectionAndThread()
                raise BridgeDisconnected(response.get('error', 'Upstream disconnected'))
            if response.get('cmd') == 'transaction_uncertain':
                raise TransactionUncertain(response)
            if response.get('lua_error') or response.get('error') or response.get('cmd') in ('lua_error', 'lua_transaction_error', 'gateway_error'):
                raise RemoteError(response)
            return response

    def RegisterBot(self, bot_id, class_code, sec_code, base_asset_code, account):
        profile = dict(bot_id=bot_id, class_code=class_code, sec_code=sec_code,
                       base_asset_code=base_asset_code, account=account)
        response = self.process_request(dict(cmd='GatewayRegisterBot', data=profile, id=0, t=''))
        self._registration = profile
        return response

    def GatewayStatus(self):
        return self.process_request(dict(cmd='GatewayStatus', data='', id=0, t=''))

    def RefreshOrders(self, strict=False):
        return self.process_request(dict(cmd='GatewayRefreshOrders',
                                         data={'strict': True} if strict else '', id=0, t=''))

    def GetConfirmedUnfilledCancellation(self, order_num):
        return self.process_request(dict(cmd='GatewayConfirmedUnfilledCancellation',
                                         data=str(order_num), id=0, t=''))

    def WaitTransaction(self, trans_id, timeout=8.0):
        return self.process_request(dict(cmd='GatewayWaitTransaction', data=dict(trans_id=str(trans_id), timeout=timeout), id=0, t=''))

    def Reconnect(self):
        self.CloseConnectionAndThread()
        try:
            self._open()
        except Exception:
            self.CloseConnectionAndThread()
            raise
        if self._registration:
            response = self.process_request(dict(cmd='GatewayRegisterBot', data=self._registration, id=0, t=''))
            if response['data'].get('unresolved_transactions'):
                raise TransactionUncertain(dict(cmd='transaction_uncertain', error='Unresolved transaction after reconnect'))
        return True

    def __enter__(self):
        """Вход в класс, например, с with"""
        return self

    # Фукнции связи с QuikSharp
    
    def Ping(self, trans_id=0):
        """Проверка соединения. Отправка ping. Получение pong"""
        return self.process_request({'data': 'Ping', 'id': trans_id, 'cmd': 'ping', 't': ''})

    def Echo(self, message, trans_id=0):
        """Эхо. Отправка и получение одного и того же сообщения"""
        return self.process_request({'data': message, 'id': trans_id, 'cmd': 'echo', 't': ''})

    def DivideStringByZero(self, trans_id=0):
        """Тест обработки ошибок. Выполняется деление на 0 с выдачей ошибки"""
        return self.process_request({'data': '', 'id': trans_id, 'cmd': 'divide_string_by_zero', 't': ''})

    def IsQuik(self, trans_id=0):
        """Скрипт запущен в Квике"""
        return self.process_request({'data': '', 'id': trans_id, 'cmd': 'is_quik', 't': ''})

    # 2.1 Сервисные функции

    def IsConnected(self, trans_id=0):  # 1
        """Состояние подключения терминала к серверу QUIK. Возвращает 1 - подключено / 0 - не подключено"""
        return self.process_request({'data': '', 'id': trans_id, 'cmd': 'isConnected', 't': ''})

    def GetScriptPath(self, trans_id=0):  # 2
        """Путь скрипта без завершающего обратного слэша"""
        return self.process_request({'data': '', 'id': trans_id, 'cmd': 'getScriptPath', 't': ''})

    def GetInfoParam(self, params, trans_id=0):  # 3
        """Значения параметров информационного окна"""
        return self.process_request({'data': params, 'id': trans_id, 'cmd': 'getInfoParam', 't': ''})

    # message - 4. Сообщение в терминале QUIK. Реализовано в виде 3-х отдельных функций в QuikSharp

    def Sleep(self, time, trans_id=0):  # 5
        """Приостановка скрипта. Время в миллисекундах"""
        return self.process_request({'data': time, 'id': trans_id, 'cmd': 'sleep', 't': ''})

    def GetWorkingFolder(self, trans_id=0):  # 6
        """Путь к info.exe, исполняющего скрипт без завершающего обратного слэша"""
        return self.process_request({'data': '', 'id': trans_id, 'cmd': 'getWorkingFolder', 't': ''})

    def PrintDbgStr(self, message, trans_id=0):  # 7
        """Вывод отладочной информации. Можно посмотреть с помощью DebugView"""
        return self.process_request({'data': message, 'id': trans_id, 'cmd': 'PrintDbgStr', 't': ''})

    # sysdate - 8. Системные дата и время
    # isDarkTheme - 9. Тема оформления. true - тёмная, false - светлая

    # Сервисные функции QuikSharp
    
    def MessageInfo(self, message, trans_id=0):  # В QUIK LUA message icon_type=1
        """Отправка информационного сообщения в терминал QUIK"""
        return self.process_request({'data': message, 'id': trans_id, 'cmd': 'message', 't': ''})

    def MessageWarning(self, message, trans_id=0):  # В QUIK LUA message icon_type=2
        """Отправка сообщения с предупреждением в терминал QUIK"""
        return self.process_request({'data': message, 'id': trans_id, 'cmd': 'warning_message', 't': ''})

    def MessageError(self, message, trans_id=0):  # В QUIK LUA message icon_type=3
        """Отправка сообщения об ошибке в терминал QUIK"""
        return self.process_request({'data': message, 'id': trans_id, 'cmd': 'error_message', 't': ''})

    # 3.1. Функции для обращения к строкам произвольных таблиц

    # getItem - 1. Строка таблицы
    # getOrderByNumber - 2. Заявка
    # getNumberOf - 3. Кол-во записей в таблице
    # SearchItems - 4. Быстрый поиск по таблице заданной функцией поиска

    # Функции для обращения к строкам произвольных таблиц QuikSharp

    def GetTradeAccounts(self, trans_id=0):
        """Торговые счета, у которых указаны поддерживаемые классы инструментов"""
        return self.process_request({'data': '', 'id': trans_id, 'cmd': 'getTradeAccounts', 't': ''})

    def GetTradeAccount(self, class_code, trans_id=0):
        """Торговый счет для запрашиваемого кода класса"""
        return self.process_request({'data': class_code, 'id': trans_id, 'cmd': 'getTradeAccount', 't': ''})
        
    def GetAllOrders(self, trans_id=0):
        """Таблица заявок (вся)"""
        return self.process_request({'data': f'', 'id': trans_id, 'cmd': 'get_orders', 't': ''})

    def GetOrders(self, class_code, sec_code, trans_id=0):
        """Таблица заявок (по инструменту)"""
        return self.process_request({'data': f'{class_code}|{sec_code}', 'id': trans_id, 'cmd': 'get_orders', 't': ''})

    def GetOrderByNumber(self, order_id, trans_id=0):
        """Заявка по номеру"""
        return self.process_request({'data': order_id, 'id': trans_id, 'cmd': 'getOrder_by_Number', 't': ''})

    def GetOrderById(self, class_code, sec_code, order_trans_id, trans_id=0):
        """Заявка по инструменту и Id транзакции"""
        return self.process_request({'data': f'{class_code}|{sec_code}|{order_trans_id}', 'id': trans_id, 'cmd': 'getOrder_by_ID', 't': ''})

    def GetOrderByClassNumber(self, class_code, order_id, trans_id=0):
        """Заявка по классу инструмента и номеру"""
        return self.process_request({'data': f'{class_code}|{order_id}', 'id': trans_id, 'cmd': 'get_order_by_number', 't': ''})

    def GetMoneyLimits(self, trans_id=0):
        """Все денежные лимиты"""
        return self.process_request({'data': '', 'id': trans_id, 'cmd': 'getMoneyLimits', 't': ''})

    def GetClientCode(self, trans_id=0):
        """Основной (первый) код клиента"""
        return self.process_request({'data': '', 'id': trans_id, 'cmd': 'getClientCode', 't': ''})

    def GetClientCodes(self, trans_id=0):
        """Все коды клиента"""
        return self.process_request({'data': '', 'id': trans_id, 'cmd': 'getClientCodes', 't': ''})

    def GetAllDepoLimits(self, trans_id=0):
        """Лимиты по бумагам (всем)"""
        return self.process_request({'data': '', 'id': trans_id, 'cmd': 'get_depo_limits', 't': ''})

    def GetDepoLimits(self, sec_code, trans_id=0):
        """Лимиты по бумагам (по инструменту)"""
        return self.process_request({'data': sec_code, 'id': trans_id, 'cmd': 'get_depo_limits', 't': ''})

    def GetAllTrades(self, trans_id=0):
        """Таблица сделок (вся)"""
        return self.process_request({'data': f'', 'id': trans_id, 'cmd': 'get_trades', 't': ''})

    def GetTrades(self, class_code, sec_code, trans_id=0):
        """Таблица сделок (по инструменту)"""
        return self.process_request({'data': f'{class_code}|{sec_code}', 'id': trans_id, 'cmd': 'get_trades', 't': ''})

    def GetTradesByOrderNumber(self, order_num, trans_id=0):
        """Таблица сделок по номеру заявки"""
        return self.process_request({'data': order_num, 'id': trans_id, 'cmd': 'get_Trades_by_OrderNumber', 't': ''})

    def GetAllStopOrders(self, trans_id=0):
        """Стоп заявки (все)"""
        return self.process_request({'data': '', 'id': trans_id, 'cmd': 'get_stop_orders', 't': ''})

    def GetStopOrders(self, class_code, sec_code, trans_id=0):
        """Стоп заявки (по инструменту)"""
        return self.process_request({'data': f'{class_code}|{sec_code}', 'id': trans_id, 'cmd': 'get_stop_orders', 't': ''})

    def GetAllTrade(self, trans_id=0):
        """Таблица обезличенных сделок (вся)"""
        return self.process_request({'data': f'', 'id': trans_id, 'cmd': 'get_all_trades', 't': ''})

    def GetTrade(self, class_code, sec_code, trans_id=0):
        """Таблица обезличенных сделок (по инструменту)"""
        return self.process_request({'data': f'{class_code}|{sec_code}', 'id': trans_id, 'cmd': 'get_all_trades', 't': ''})

    # 3.2 Функции для обращения к спискам доступных параметров

    def GetClassesList(self, trans_id=0):  # 1
        """Список классов"""
        return self.process_request({'data': '', 'id': trans_id, 'cmd': 'getClassesList', 't': ''})

    def GetClassInfo(self, class_code, trans_id=0):  # 2
        """Информация о классе"""
        return self.process_request({'data': class_code, 'id': trans_id, 'cmd': 'getClassInfo', 't': ''})

    def GetClassSecurities(self, class_code, trans_id=0):  # 3
        """Список инструментов класса"""
        return self.process_request({'data': class_code, 'id': trans_id, 'cmd': 'getClassSecurities', 't': ''})
    
    # Функции для обращения к спискам доступных параметров QuikSharp

    def GetOptionBoard(self, class_code, sec_code, trans_id=0):
        """Доска опционов"""
        return self.process_request({'data': f'{class_code}|{sec_code}', 'id': trans_id, 'cmd': 'getOptionBoard', 't': ''})

    # 3.3 Функции для получения информации по денежным средствам

    def GetMoney(self, client_code, firm_id, tag, curr_code, trans_id=0):  # 1
        """Денежные позиции"""
        return self.process_request({'data': f'{client_code}|{firm_id}|{tag}|{curr_code}', 'id': trans_id, 'cmd': 'getMoney', 't': ''})

    def GetMoneyEx(self, firm_id, client_code, tag, curr_code, limit_kind, trans_id=0):  # 2
        """Денежные позиции указанного типа"""
        return self.process_request({'data': f'{firm_id}|{client_code}|{tag}|{curr_code}|{limit_kind}', 'id': trans_id, 'cmd': 'getMoneyEx', 't': ''})

    # 3.4 Функции для получения позиций по инструментам

    def GetDepo(self, client_code, firm_id, sec_code, account, trans_id=0):  # 1
        """Позиции по инструментам"""
        return self.process_request({'data': f'{client_code}|{firm_id}|{sec_code}|{account}', 'id': trans_id, 'cmd': 'getDepo', 't': ''})

    def GetDepoEx(self, firm_id, client_code, sec_code, account, limit_kind, trans_id=0):  # 2
        """Позиции по инструментам указанного типа"""
        return self.process_request({'data': f'{firm_id}|{client_code}|{sec_code}|{account}|{limit_kind}', 'id': trans_id, 'cmd': 'getDepoEx', 't': ''})

    # 3.5 Функция для получения информации по фьючерсным лимитам

    def GetFuturesLimit(self, firm_id, account_id, limit_type, curr_code, trans_id=0):  # 1
        """Фьючерсные лимиты"""
        return self.process_request({'data': f'{firm_id}|{account_id}|{limit_type}|{curr_code}', 'id': trans_id, 'cmd': 'getFuturesLimit', 't': ''})

    # Функция для получения информации по фьючерсным лимитам QuikSharp

    def GetFuturesClientLimits(self, trans_id=0):
        """Все фьючерсные лимиты"""
        return self.process_request({'data': '', 'id': trans_id, 'cmd': 'getFuturesClientLimits', 't': ''})

    # 3.6 Функция для получения информации по фьючерсным позициям

    def GetFuturesHolding(self, firm_id, account_id, sec_code, position_type, trans_id=0):  # 1
        """Фьючерсные позиции"""
        return self.process_request({'data': f'{firm_id}|{account_id}|{sec_code}|{position_type}', 'id': trans_id, 'cmd': 'getFuturesHolding', 't': ''})

    # Функция для получения информации по фьючерсным позициям QuikSharp

    def GetFuturesHoldings(self, trans_id=0):
        """Все фьючерсные позиции"""
        return self.process_request({'data': '', 'id': trans_id, 'cmd': 'getFuturesClientHoldings', 't': ''})

    # 3.7 Функция для получения информации по инструменту

    def GetSecurityInfo(self, class_code, sec_code, trans_id=0):  # 1
        """Информация по инструменту"""
        return self.process_request({'data': f'{class_code}|{sec_code}', 'id': trans_id, 'cmd': 'getSecurityInfo', 't': ''})

    # Функция для получения информации по инструменту QuikSharp

    def GetSecurityInfoBulk(self, class_codes, sec_codes, trans_id=0):
        """Информация по инструментам"""
        return self.process_request({'data': f'{class_codes}|{sec_codes}', 'id': trans_id, 'cmd': 'getSecurityInfoBulk', 't': ''})

    def GetSecurityClass(self, classes_list, sec_code, trans_id=0):
        """Класс по коду инструмента из заданных классов"""
        return self.process_request({'data': f'{classes_list}|{sec_code}', 'id': trans_id, 'cmd': 'getSecurityClass', 't': ''})

    # 3.8 Функция для получения даты торговой сессии

    # getTradeDate - 1. Дата текущей торговой сессии

    # 3.9 Функция для получения стакана по указанному классу и инструменту

    def GetQuoteLevel2(self, class_code, sec_code, trans_id=0):  # 1
        """Стакан по классу и инструменту"""
        return self.process_request({'data': f'{class_code}|{sec_code}', 'id': trans_id, 'cmd': 'GetQuoteLevel2', 't': ''})

    # 3.10 Функции для работы с графиками

    # getLinesCount - 1. Кол-во линий в графике

    def GetNumCandles(self, tag, trans_id=0):  # 2
        """Кол-во свечей по тэгу"""
        return self.process_request({'data': tag, 'id': trans_id, 'cmd': 'get_num_candles', 't': ''})

    # getCandlesByIndex - 3. Информация о свечках (реализовано в get_candles)
    # CreateDataSource - 4. Создание источника данных c функциями: (реализовано в get_candles_from_data_source)
    # - SetUpdateCallback - Привязка функции обратного вызова на изменение свечи
    # - O, H, L, C, V, T - Функции получения цен, объемов и времени
    # - Size - Функция кол-ва свечек в источнике данных
    # - Close - Функция закрытия источника данных. Терминал прекращает получать данные с сервера
    # - SetEmptyCallback - Функция сброса функции обратного вызова на изменение свечи

    # Функции для работы с графиками QuikSharp

    def GetCandles(self, tag, line, first_candle, count, trans_id=0):
        """Свечки по идентификатору графика"""
        return self.process_request({'data': f'{tag}|{line}|{first_candle}|{count}', 'id': trans_id, 'cmd': 'get_candles', 't': ''})

    def GetCandlesFromDataSource(self, class_code, sec_code, interval, count):  # ichechet - Добавлен выход по таймауту
        """Свечки"""
        return self.process_request({'data': f'{class_code}|{sec_code}|{interval}|{count}', 'id': '1', 'cmd': 'get_candles_from_data_source', 't': ''})

    def SubscribeToCandles(self, class_code, sec_code, interval, trans_id=0):
        """Подписка на свечки"""
        return self.process_request({'data': f'{class_code}|{sec_code}|{interval}', 'id': trans_id, 'cmd': 'subscribe_to_candles', 't': ''})

    def IsSubscribed(self, class_code, sec_code, interval, trans_id=0):
        """Есть ли подписка на свечки"""
        return self.process_request({'data': f'{class_code}|{sec_code}|{interval}', 'id': trans_id, 'cmd': 'is_subscribed', 't': ''})

    def UnsubscribeFromCandles(self, class_code, sec_code, interval, trans_id=0):
        """Отмена подписки на свечки"""
        return self.process_request({'data': f'{class_code}|{sec_code}|{interval}', 'id': trans_id, 'cmd': 'unsubscribe_from_candles', 't': ''})

    # 3.11 Функции для работы с заявками

    def SendTransaction(self, transaction, trans_id=0):  # 1
        """Отправка транзакции в торговую систему"""
        return self.process_request({'data': transaction, 'id': trans_id, 'cmd': 'sendTransaction', 't': ''})

    # CalcBuySell - 2. Максимальное кол-во лотов в заявке

    # 3.12 Функции для получения значений таблицы "Текущие торги"

    def GetParamEx(self, class_code, sec_code, param_name, trans_id=0):  # 1
        """Таблица текущих торгов"""
        return self.process_request({'data': f'{class_code}|{sec_code}|{param_name}', 'id': trans_id, 'cmd': 'getParamEx', 't': ''})

    def GetParamEx2(self, class_code, sec_code, param_name, trans_id=0):  # 2
        """Таблица текущих торгов по инструменту с возможностью отказа от получения"""
        return self.process_request({'data': f'{class_code}|{sec_code}|{param_name}', 'id': trans_id, 'cmd': 'getParamEx2', 't': ''})

    # Функция для получения значений таблицы "Текущие торги" QuikSharp

    def GetParamEx2Bulk(self, class_codes, sec_codes, param_names, trans_id=0):
        """Таблица текущих торгов по инструментам с возможностью отказа от получения"""
        return self.process_request({'data': f'{class_codes}|{sec_codes}|{param_names}', 'id': trans_id, 'cmd': 'getParamEx2Bulk', 't': ''})

    # 3.13 Функции для получения параметров таблицы "Клиентский портфель"

    def GetPortfolioInfo(self, firm_id, client_code, trans_id=0):  # 1
        """Клиентский портфель"""
        return self.process_request({'data': f'{firm_id}|{client_code}', 'id': trans_id, 'cmd': 'getPortfolioInfo', 't': ''})

    def GetPortfolioInfoEx(self, firm_id, client_code, limit_kind, trans_id=0):  # 2
        """Клиентский портфель по сроку расчетов"""
        return self.process_request({'data': f'{firm_id}|{client_code}|{limit_kind}', 'id': trans_id, 'cmd': 'getPortfolioInfoEx', 't': ''})

    # 3.14 Функции для получения параметров таблицы "Купить/Продать"

    # getBuySellInfo - 1. Параметры таблицы купить/продать
    # getBuySellInfoEx - 2. Параметры таблицы купить/продать с дополнительными полями вывода

    # 3.15 Функции для работы с таблицами Рабочего места QUIK

    # AddColumn - 1. Добавление колонки в таблицу
    # AllocTable - 2. Структура, описывающая таблицу
    # Clear - 3. Удаление содержимого таблицы
    # CreateWindow - 4. Создание окна таблицы
    # DeleteRow - 5. Удаление строки из таблицы
    # DestroyTable - 6. Закрытие окна таблицы
    # InsertRow - 7. Добавление строки в таблицу
    # IsWindowClosed - 8. Закрыто ли окно с таблицей
    # GetCell - 9. Данные ячейки таблицы
    # GetTableSize - 10. Кол-во строк и столбцов таблицы
    # GetWindowCaption - 11. Заголовок окна таблицы
    # GetWindowRect - 12. Координаты верхнего левого и правого нижнего углов таблицы
    # SetCell - 13. Установка значения ячейки таблицы
    # SetWindowCaption - 14. Установка заголовка окна таблицы
    # SetWindowPos - 15. Установка верхнего левого угла, и размеры таблицы
    # SetTableNotificationCallback - 16. Установка функции обратного вызова для обработки событий в таблице
    # RGB - 17. Преобразование каждого цвета в одно число для функци SetColor
    # SetColor - 18. Установка цвета ячейки, столбца или строки таблицы
    # Highlight - 19. Подсветка диапазона ячеек цветом фона и цветом текста на заданное время с плавным затуханием
    # SetSelectedRow - 20. Выделение строки таблицы

    # 3.16 Функции для работы с метками

    def AddLabel(self, price, cur_date, cur_time, qty, path, label_id, alignment, background, trans_id=0):  # 1
        """Добавление метки на график"""
        return self.process_request({'data': f'{price}|{cur_date}|{cur_time}|{qty}|{path}|{label_id}|{alignment}|{background}', 'id': trans_id, 'cmd': 'AddLabel', 't': ''})

    def DelLabel(self, chart_tag, label_id, trans_id=0):  # 2
        """Удаление метки с графика"""
        return self.process_request({'data': f'{chart_tag}|{label_id}', 'id': trans_id, 'cmd': 'DelLabel', 't': ''})

    def DelAllLabels(self, chart_tag, trans_id=0):  # 3
        """Удаление всех меток с графика"""
        return self.process_request({'data': chart_tag, 'id': trans_id, 'cmd': 'DelAllLabels', 't': ''})

    def GetLabelParams(self, chart_tag, label_id, trans_id=0):  # 4
        """Получение параметров метки"""
        return self.process_request({'data': f'{chart_tag}|{label_id}', 'id': trans_id, 'cmd': 'GetLabelParams', 't': ''})

    # SetLabelParams - 5. Установка параметров метки

    # 3.17 Функции для заказа стакана котировок

    def SubscribeLevel2Quotes(self, class_code, sec_code, trans_id=0):  # 1
        """Подписка на стакан по Классу|Коду бумаги"""
        return self.process_request({'data': f'{class_code}|{sec_code}', 'id': trans_id, 'cmd': 'Subscribe_Level_II_Quotes', 't': ''})

    def UnsubscribeLevel2Quotes(self, class_code, sec_code, trans_id=0):  # 2
        """Отмена подписки на стакан по Классу|Коду бумаги"""
        return self.process_request({'data': f'{class_code}|{sec_code}', 'id': trans_id, 'cmd': 'Unsubscribe_Level_II_Quotes', 't': ''})

    def IsSubscribedLevel2Quotes(self, class_code, sec_code, trans_id=0):  # 3
        """Есть ли подписка на стакан по Классу|Коду бумаги"""
        return self.process_request({'data': f'{class_code}|{sec_code}', 'id': trans_id, 'cmd': 'IsSubscribed_Level_II_Quotes', 't': ''})

    # 3.18 Функции для заказа параметров Таблицы текущих торгов

    def ParamRequest(self, class_code, sec_code, param_name, trans_id=0):  # 1
        """Заказ получения таблицы текущих торгов по инструменту"""
        return self.process_request({'data': f'{class_code}|{sec_code}|{param_name}', 'id': trans_id, 'cmd': 'paramRequest', 't': ''})

    def CancelParamRequest(self, class_code, sec_code, param_name, trans_id=0):  # 2
        """Отмена заказа получения таблицы текущих торгов по инструменту"""
        return self.process_request({'data': f'{class_code}|{sec_code}|{param_name}', 'id': trans_id, 'cmd': 'cancelParamRequest', 't': ''})

    # Функции для заказа параметров Таблицы текущих торгов QuikSharp

    def ParamRequestBulk(self, class_codes, sec_codes, param_names, trans_id=0):
        """Заказ получения таблицы текущих торгов по инструментам"""
        return self.process_request({'data': f'{class_codes}|{sec_codes}|{param_names}', 'id': trans_id, 'cmd': 'paramRequestBulk', 't': ''})

    def CancelParamRequestBulk(self, class_codes, sec_codes, param_names, trans_id=0):
        """Отмена заказа получения таблицы текущих торгов по инструментам"""
        return self.process_request({'data': f'{class_codes}|{sec_codes}|{param_names}', 'id': trans_id, 'cmd': 'cancelParamRequestBulk', 't': ''})

    # 3.19 Функции для получения информации по единой денежной позиции

    def GetTrdAccByClientCode(self, firm_id, client_code, trans_id=0):  # 1
        """Торговый счет срочного рынка по коду клиента фондового рынка"""
        return self.process_request({'data': f'{firm_id}|{client_code}', 'id': trans_id, 'cmd': 'getTrdAccByClientCode', 't': ''})

    def GetClientCodeByTrdAcc(self, firm_id, trade_account_id, trans_id=0):  # 2
        """Код клиента фондового рынка с единой денежной позицией по торговому счету срочного рынка"""
        return self.process_request({'data': f'{firm_id}|{trade_account_id}', 'id': trans_id, 'cmd': 'getClientCodeByTrdAcc', 't': ''})

    def IsUcpClient(self, firm_id, client, trans_id=0):  # 3
        """Имеет ли клиент единую денежную позицию"""
        return self.process_request({'data': f'{firm_id}|{client}', 'id': trans_id, 'cmd': 'IsUcpClient', 't': ''})

    def CloseConnectionAndThread(self):
        closed = getattr(self, '_closed', None)
        if closed is not None:
            closed.set()
        close_socket(getattr(self, 'socket_requests', None))
        close_socket(getattr(self, 'socket_callbacks', None))
        thread = getattr(self, 'callback_thread', None)
        if thread and thread is not threading.current_thread():
            thread.join(timeout=2)

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.CloseConnectionAndThread()

    def __del__(self):
        try:
            self.CloseConnectionAndThread()
        except Exception:
            pass
