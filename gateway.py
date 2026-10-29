"""One QUIK connection shared by independent grid-bot processes."""
import argparse
from concurrent.futures import Future, TimeoutError as FutureTimeout
from collections import deque
import copy
import json
import logging
from log_policy import make_handler, cleanup_logs
from pathlib import Path
import queue
import re
import socket
import threading
import time
import uuid
import sys

from QuikPy import QuikPy
from registry import Registry
from transport import (BridgeDisconnected, JsonLineSocket, RemoteError,
                       TransactionUncertain, close_socket, encode_frame,
                       REQUEST_WAIT_SECONDS, REGISTRATION_WAIT_SECONDS)

LOGGER = logging.getLogger('netdca.gateway')
READ_COMMANDS = {
    'ping', 'echo', 'isConnected', 'is_quik', 'getInfoParam',
    'getClassesList', 'getClassInfo', 'getClassSecurities', 'getSecurityClass',
    'getSecurityInfo', 'getSecurityInfoBulk', 'getParamEx', 'getParamEx2',
    'getParamEx2Bulk', 'getFuturesClientHoldings', 'getFuturesClientLimits',
    'getFuturesHolding', 'getTradeAccounts', 'getTradeAccount', 'getClientCodes',
    'getClientCode', 'getMoneyLimits', 'getPortfolioInfo', 'getPortfolioInfoEx',
    'getOrder_by_Number', 'get_order_by_number', 'get_orders', 'get_trades'}


class BoundedOutbox:
    def __init__(self, max_messages=4096, max_bytes=8 * 1024 * 1024):
        self.max_messages, self.max_bytes = max_messages, max_bytes
        self.items, self.bytes, self.high_water = deque(), 0, 0
        self.closed = False
        self.condition = threading.Condition()

    def put(self, data):
        with self.condition:
            if self.closed or len(self.items) >= self.max_messages or self.bytes + len(data) > self.max_bytes:
                return False
            self.items.append(data)
            self.bytes += len(data)
            self.high_water = max(self.high_water, self.bytes)
            self.condition.notify()
            return True

    def get(self):
        with self.condition:
            while not self.items and not self.closed:
                self.condition.wait(0.5)
            if self.closed:
                return None
            data = self.items.popleft()
            self.bytes -= len(data)
            return data

    def close(self):
        with self.condition:
            self.closed = True
            self.items.clear()
            self.bytes = 0
            self.condition.notify_all()


class Engine:
    def __init__(self, database, upstream_host='127.0.0.1',
                 upstream_ports=(34230, 34231), client_factory=QuikPy):
        self.registry = Registry(database)
        self.host, self.ports = upstream_host, upstream_ports
        self.client_factory, self.client = client_factory, None
        self.profiles = {}
        self.retire_checks = {}
        self.profile_lock = threading.RLock()
        self.listeners = []
        self.jobs = queue.Queue(maxsize=128)
        self.stopped = threading.Event()
        self.request_id = 0
        self.last_snapshot = {}
        self.last_metrics = 0
        self.last_log_cleanup = 0
        self.pending_trades = deque(maxlen=256)
        self.event_lock = threading.RLock()
        self.metrics = {'requests': 0, 'events': 0, 'lua': {}}
        self.worker = threading.Thread(target=self._work, name='QUIKRequests', daemon=True)
        self.worker.start()

    def _profiles(self):
        with self.profile_lock:
            return copy.deepcopy(self.profiles)

    def _connect(self):
        if self.client and self.client.connected:
            return
        if self.client:
            self.client.CloseConnectionAndThread()
        self.client = self.client_factory(
            host=self.host, requests_port=self.ports[0],
            callbacks_port=self.ports[1], event_handler=self.on_event)
        self._call('set_event_filters', list(self._profiles().values()))
        for profile in self._profiles().values():
            self.refresh(profile)
            self.refresh_trades(profile)
        LOGGER.info('Upstream connected')

    def _call(self, cmd, data=''):
        self.request_id += 1
        try:
            response = self.client.process_request(
                dict(cmd=cmd, data=data, id=self.request_id, t=''))
        except TimeoutError as exc:
            raise TimeoutError(f'QUIK did not answer {cmd}; check the running QuikSharp.lua connector') from exc
        self.metrics['requests'] += 1
        # Older Lua RPCs omit a key when a terminal function returns nil.
        # A missing security in one class is a normal result during leg lookup.
        response.setdefault('data', None)
        if cmd in ('getFuturesClientHoldings', 'getTradeAccounts',
                   'get_active_orders', 'get_recent_trades'):
            if response['data'] == {}:
                response['data'] = []  # dkjson encodes untyped empty Lua tables as {}.
            if not isinstance(response['data'], list):
                raise RemoteError(dict(error=f'{cmd}: expected an array from QUIK, got {type(response["data"]).__name__}'))
        return response

    def refresh(self, profile):
        watermark = self.registry.sequence
        response = self._call('get_active_orders', profile)
        seen = {str(order['order_num']) for order in response['data']}
        for known in self.registry.active(profile):
            if str(known['order_num']) not in seen:
                # Absence from a filtered table is not a cancellation receipt.
                order = self.verify_order(dict(order_num=known['order_num']),profile)
                if int(order['flags']) & 1:
                    response['data'].append(order)
        self.registry.snapshot(response['data'], profile, self._profiles(), watermark)
        self.last_snapshot[profile['bot_id']] = time.monotonic()
        with self.event_lock:
            waiting = list(self.pending_trades)
            self.pending_trades.clear()
            for event in waiting:
                self._on_event(event)

    def refresh_trades(self, profile):
        for trade in self._call('get_recent_trades', profile)['data']:
            self.registry.on_trade(trade)

    def verify_order(self, transaction, profile):
        number = transaction['order_num']
        if not number:
            raise TransactionUncertain(dict(error='Accepted order has no order number; new orders blocked'))
        order = None
        deadline = time.monotonic()+2
        while order is None:
            value = self._call('get_order_by_number',profile['class_code']+'|'+str(number))['data']
            if isinstance(value,dict) and str(value.get('order_num')) == str(number):
                order = value
                break
            if time.monotonic() >= deadline:
                self.registry.unverify(number)
                raise TransactionUncertain(dict(error=f'Order {number} accepted but not found in QUIK; new orders blocked'))
            time.sleep(0.05)
        if (order.get('class_code') != profile['class_code']
                or order.get('sec_code') != profile['sec_code']
                or order.get('account',order.get('trdaccid')) != profile['account']):
            self.registry.unverify(number)
            raise RemoteError(dict(error=f'Order {number} identity mismatch: '
                f'configured {profile["class_code"]}/{profile["sec_code"]}, account {profile["account"]}; '
                f'QUIK returned {order.get("class_code")}/{order.get("sec_code")}, '
                f'account {order.get("account",order.get("trdaccid"))}. New orders blocked.'))
        self.registry.on_order(order,self._profiles())
        return order

    def on_event(self, event):
        with self.event_lock:
            self._on_event(event)
            if event['cmd'] in ('OnOrder', 'OnTransReply'):
                waiting = list(self.pending_trades)
                self.pending_trades.clear()
                for trade_event in waiting:
                    self._on_event(trade_event)

    def _on_event(self, event):
        self.metrics['events'] += 1
        data = event.get('data') if isinstance(event.get('data'), dict) else {}
        owner, row = None, None
        if event['cmd'] == 'OnBridgeMetrics':
            self.metrics['lua'] = data
            return
        if event['cmd'] == 'OnTransReply':
            LOGGER.info('TX_REPLY %s', json.dumps(data, ensure_ascii=False))
            row = self.registry.on_reply(data)
            owner = row['bot'] if row else None
        elif event['cmd'] == 'OnOrder':
            owner = self.registry.on_order(data, self._profiles())
        elif event['cmd'] == 'OnTrade':
            owner = self.registry.on_trade(data)
            if owner is None:
                profiles = self._profiles()
                if any(self.registry.unresolved(p['bot_id']) and
                       data.get('sec_code') == p['sec_code'] and
                       data.get('class_code') == p['class_code'] and
                       data.get('account', data.get('trdaccid')) == p['account']
                       for p in profiles.values()):
                    if len(self.pending_trades) == self.pending_trades.maxlen:
                        LOGGER.error('Unmatched trade queue full; reconnect for reconciliation')
                        if self.client:
                            self.client.CloseConnectionAndThread()
                    else:
                        self.pending_trades.append(copy.deepcopy(event))
                return
        if event['cmd'] in ('OnOrder', 'OnTrade', 'OnTransReply') and owner is None:
            return
        outgoing = copy.deepcopy(event)
        tx = row or self.registry.transaction(data.get('trans_id', -1))
        if tx and isinstance(outgoing.get('data'), dict):
            outgoing['data']['trans_id'] = int(tx['client_id'])
        for listener in list(self.listeners):
            listener(outgoing, owner)

    def submit(self, request, profile):
        future = Future()
        try:
            self.jobs.put_nowait((request, profile, future))
        except queue.Full:
            future.set_exception(BridgeDisconnected('Upstream request queue is full'))
        return future

    def _register(self, profile):
        required = ('bot_id', 'class_code', 'sec_code', 'base_asset_code', 'account')
        if not isinstance(profile, dict) or any(
                not isinstance(profile.get(key), str) or not profile[key] for key in required):
            raise ValueError('Registration requires bot_id, class_code, sec_code, base_asset_code, account')
        if len(profile['bot_id']) > 64 or not all(
                c.isalnum() or c in '_-' for c in profile['bot_id']):
            raise ValueError('Invalid bot_id')
        profiles = self._profiles()
        for other in profiles.values():
            if other['bot_id'] != profile['bot_id'] and (
                    other['account'], other['base_asset_code']) == (
                    profile['account'], profile['base_asset_code']):
                raise ValueError('Bots cannot share the same account and working leg')
        previous = profiles.get(profile['bot_id'])
        if previous and previous != profile and (
                self.registry.active(previous) or self.registry.unresolved(profile['bot_id'])):
            raise ValueError('Cannot change a profile with active orders or unresolved transactions')
        info = self._call('getSecurityInfo', profile['class_code'] + '|' + profile['sec_code'])['data']
        if not info:
            raise ValueError(f'Instrument not found in QUIK: {profile["class_code"]}/{profile["sec_code"]}. '
                             'Check settings and terminal data availability; an empty order table is unrelated.')
        classes = self._call('getClassesList')['data']
        classes = re.split(r'[;,]', classes) if isinstance(classes, str) else classes
        if not isinstance(classes, list):
            raise ValueError('QUIK did not return a valid class list')
        leg = None
        candidates = dict.fromkeys([profile['class_code']] +
            [code.strip() for code in classes if isinstance(code, str)])
        for class_code in candidates:
            if class_code:
                leg = self._call('getSecurityInfo', class_code + '|' + profile['base_asset_code'])['data']
                if leg:
                    break
        if not leg:
            raise ValueError('Working leg not found in QUIK; check BASE_ASSET_CODE')
        holdings = self._call('getFuturesClientHoldings')['data']
        accounts = self._call('getTradeAccounts')['data']
        valid_accounts = {
            item.get('trdaccid', item.get('account')) for item in holdings + accounts}
        if profile['account'] not in valid_accounts:
            raise ValueError('Account not found in QUIK; check the account code')
        tick = self._call('getParamEx', profile['class_code'] + '|' +
                          profile['sec_code'] + '|SEC_PRICE_STEP')['data']
        if int(tick.get('result', 0)) != 1 or float(tick.get('param_value', 0)) <= 0:
            raise ValueError('QUIK did not return an instrument tick size')
        with self.profile_lock:
            self.profiles[profile['bot_id']] = dict(profile)
            self.retire_checks.pop(profile['bot_id'], None)
        self._call('set_event_filters', list(self._profiles().values()))
        self.refresh(profile)
        while True:
            missing = self.registry.missing_orders(profile['bot_id'])
            if not missing:
                break
            for transaction in missing:
                self.verify_order(transaction,profile)
        self.refresh_trades(profile)
        return dict(
            foreign_active_orders=len(self.registry.active(profile, owned=False)),
            unresolved_transactions=self.registry.unresolved(profile['bot_id']),
            instrument=info, working_leg=leg, tick=tick)

    def _send(self, transaction, profile, session=""):
        if profile is None or not isinstance(transaction, dict):
            raise ValueError('Register the bot before sending transactions')
        if transaction.get('CLASSCODE') != profile['class_code'] or transaction.get('SECCODE') != profile['sec_code']:
            raise ValueError('Transaction instrument does not match registration')
        action = transaction.get('ACTION')
        if action not in ('NEW_ORDER', 'KILL_ORDER'):
            raise ValueError('Only NEW_ORDER and KILL_ORDER are supported')
        if action == 'NEW_ORDER':
            if transaction.get('ACCOUNT') != profile['account']:
                raise ValueError('Transaction account does not match registration')
            if (transaction.get('TYPE') != 'L'
                    or transaction.get('OPERATION') not in ('B', 'S')
                    or int(transaction.get('QUANTITY', 0)) <= 0):
                raise ValueError('Invalid limit order')
            if self.registry.unresolved(profile['bot_id']):
                raise TransactionUncertain(dict(
                    cmd='transaction_uncertain',
                    error='Resolve the previous transaction before placing new orders'))
            if self.registry.missing_orders(profile['bot_id'],limit=1):
                raise TransactionUncertain(dict(error='Previously accepted order not verified in QUIK; new orders blocked'))
            if self.registry.active(profile, owned=False):
                raise ValueError('Unowned active orders exist; reconcile them before trading')
        else:
            owned = {str(order['order_num']) for order in self.registry.active(profile)}
            if str(transaction.get('ORDER_KEY')) not in owned:
                raise ValueError('Cannot cancel an order not owned by this bot')
        client_id = str(transaction.get('TRANS_ID', ''))
        if not client_id.isdigit():
            raise ValueError('TRANS_ID must be a nonnegative integer string')
        outgoing = dict(transaction)
        gid = self.registry.allocate(
            profile['bot_id'], client_id, action, transaction.get('ORDER_KEY'), session, request=transaction)
        outgoing['TRANS_ID'] = str(gid)
        LOGGER.info('TX_SEND gid=%s bot=%s action=%s instrument=%s/%s side=%s qty=%s price=%s order=%s',
                    gid, profile['bot_id'], action, profile['class_code'], profile['sec_code'],
                    outgoing.get('OPERATION'), outgoing.get('QUANTITY'), outgoing.get('PRICE'), outgoing.get('ORDER_KEY'))
        try:
            response = self._call('sendTransaction', outgoing)
            self.registry.mark(gid, 'sent')
            return response['data']
        except RemoteError as exc:
            self.registry.mark(gid, 'rejected', exc.response)
            raise
        except (ConnectionError, TimeoutError, OSError) as exc:
            self.registry.mark(gid, 'uncertain')
            raise TransactionUncertain(dict(
                cmd='transaction_uncertain',
                error='Connection lost after transaction submission; do not replay')) from exc

    def _execute(self, request, profile):
        cmd, data = request['cmd'], request.get('data', '')
        if cmd is None:  # Internal session-release job; unavailable over JSON RPC.
            if profile and profile['bot_id'] not in self._profiles():
                with self.profile_lock:
                    self.retire_checks.pop(profile['bot_id'], None)
                return True
            if (request['still_unused']() and profile
                    and self._profiles().get(profile['bot_id']) == profile
                    and not self.registry.active(profile)
                    and not self.registry.unresolved(profile['bot_id'])):
                with self.profile_lock:
                    self.profiles.pop(profile['bot_id'], None)
                    self.retire_checks.pop(profile['bot_id'], None)
                self.last_snapshot.pop(profile['bot_id'], None)
                if self.client and self.client.connected:
                    self._call('set_event_filters', list(self._profiles().values()))
            return True
        if cmd == 'GatewayStatus':
            return dict(
                upstream_connected=bool(self.client and self.client.connected),
                metrics=copy.deepcopy(self.metrics))
        self._connect()
        if cmd == 'GatewayRegisterBot':
            return self._register(data)
        if cmd == 'GatewayVerifyTransaction':
            if profile is None:
                raise ValueError('Bot not registered')
            transaction = self.registry.client_transaction(profile['bot_id'],data['trans_id'],request.get('__session_id',''))
            if transaction is None or transaction['state'] != 'accepted':
                raise ValueError('Transaction has not been accepted')
            if transaction['action'] == 'NEW_ORDER':
                self.verify_order(transaction,profile)
            return True
        if cmd == 'GatewayRefreshOrders':
            if profile is None:
                raise ValueError('Bot not registered')
            self.refresh(profile)
            return self.registry.active(profile)
        if cmd == 'GatewayConfirmedUnfilledCancellation':
            if profile is None:
                raise ValueError('Bot not registered')
            if not isinstance(data, str) or not data.isdigit():
                raise ValueError('Order number must be a decimal string')
            order = self.registry.confirmed_unfilled_cancellation(profile, data)
            if order is not None:
                LOGGER.info('Using saved QUIK unfilled cancellation: bot=%s order=%s', profile['bot_id'], data)
            return order
        if cmd == 'sendTransaction':
            return self._send(data, profile, request.get('__session_id', ''))
        if cmd not in READ_COMMANDS:
            raise ValueError('Command not supported by the grid gateway: ' + cmd)
        if cmd in ('get_orders', 'get_trades'):
            if profile:
                if data != profile['class_code'] + '|' + profile['sec_code']:
                    raise ValueError('Query instrument does not match registration')
                if cmd == 'get_orders':
                    for transaction in self.registry.missing_orders(profile['bot_id']):
                        self.verify_order(transaction,profile)
                    return self.registry.active(profile)
                self.refresh_trades(profile)
                return self.registry.trades(profile)
            raise ValueError('Register a bot before requesting orders or trades')
        response = self._call(cmd, data)
        if profile and cmd == 'getFuturesClientHoldings':
            return [
                item for item in response['data']
                if item.get('trdaccid', item.get('account')) == profile['account']]
        return response['data']

    def _maintenance(self):
        now = time.monotonic()
        if now - self.last_log_cleanup >= 3600:
            cleanup_logs()
            self.last_log_cleanup = now
        if self.client:
            for profile in self._profiles().values():
                if now - self.last_snapshot.get(profile['bot_id'], 0) < 10:
                    continue
                try:
                    self._connect()
                    self.refresh(profile)
                    with self.profile_lock:
                        retire = self.retire_checks.get(profile['bot_id'])
                    if retire:
                        self._execute(dict(cmd=None, still_unused=retire), profile)
                except Exception as exc:
                    self.last_snapshot[profile['bot_id']] = now
                    LOGGER.warning('Periodic reconciliation failed: %s', exc)
                break  # At most one upstream snapshot between client requests.
        if now - self.last_metrics >= 60:
            from telemetry import process_metrics
            LOGGER.info('metrics %s', json.dumps(
                dict(bridge=self.metrics, processes=process_metrics(),
                     request_queue=self.jobs.qsize(),
                     unmatched_trades=len(self.pending_trades))))
            self.last_metrics = now

    def _work(self):
        while not self.stopped.is_set():
            try:
                request, profile, future = self.jobs.get(timeout=0.5)
            except queue.Empty:
                self._maintenance()
                continue
            if not future.set_running_or_notify_cancel():
                continue
            try:
                future.set_result(dict(
                    cmd=request['cmd'], id=request.get('id'),
                    data=self._execute(request, profile), t=''))
            except Exception as exc:
                LOGGER.exception('Gateway request failed: %s', request.get('cmd'))
                future.set_exception(exc)
            self._maintenance()
        while True:
            try:
                _, _, future = self.jobs.get_nowait()
            except queue.Empty:
                break
            if not future.done():
                future.set_exception(BridgeDisconnected('Gateway stopped'))
        if self.client:
            self.client.CloseConnectionAndThread()

    def close(self):
        self.stopped.set()
        if self.client:
            self.client.CloseConnectionAndThread()
        self.worker.join(timeout=12)
        if self.worker.is_alive():
            raise RuntimeError('Upstream worker did not stop')
        self.registry.close()


class Session:
    def __init__(self, server, slot, request_socket):
        self.server, self.slot, self.request_socket = server, slot, request_socket
        self.callback_socket = None
        self.callback_ready = threading.Event()
        self.outbox = BoundedOutbox()
        self.closed = threading.Event()
        self._close_lock = threading.Lock()
        self.profile = None
        self.registration_profile = None
        self.session_id = uuid.uuid4().hex
        request_socket.settimeout(0.5)
        self.stream = JsonLineSocket(request_socket)
        self.request_thread = threading.Thread(
            target=self.run, daemon=True, name=f'Bot{slot}Requests')
        self.callback_thread = None
        self.pending_future = None
        self.request_thread.start()

    def attach(self, sock):
        if self.closed.is_set() or self.callback_socket is not None:
            close_socket(sock)
            return
        self.callback_socket = sock
        sock.settimeout(2)
        self.callback_thread = threading.Thread(
            target=self.send_events, daemon=True, name=f'Bot{self.slot}Callbacks')
        self.callback_thread.start()
        self.callback_ready.set()

    def send_events(self):
        try:
            while not self.closed.is_set():
                raw = self.outbox.get()
                if raw is None:
                    break
                self.callback_socket.sendall(raw)
        except OSError:
            pass
        finally:
            self.close()

    def run(self):
        try:
            if not self.callback_ready.wait(5):
                raise BridgeDisconnected('Callback channel was not paired')
            while not self.closed.is_set():
                try:
                    request = self.stream.read()
                except socket.timeout:
                    continue
                try:
                    if request['cmd'] == 'GatewayWaitTransaction':
                        if self.profile is None:
                            raise ValueError('Bot not registered')
                        data = request['data']
                        result = self.server.engine.registry.wait(
                            self.profile['bot_id'], data['trans_id'], data.get('timeout', 8), self.session_id)
                        transaction = self.server.engine.registry.client_transaction(
                            self.profile['bot_id'],data['trans_id'],self.session_id)
                        if transaction['action'] == 'NEW_ORDER':
                            future = self.server.engine.submit(dict(cmd='GatewayVerifyTransaction',
                                data=dict(trans_id=data['trans_id']),__session_id=self.session_id),self.profile)
                            self.pending_future = future
                            try:
                                future.result(timeout=12)
                            except FutureTimeout as exc:
                                future.cancel()
                                raise TransactionUncertain(dict(error='Order verification timed out; new orders blocked')) from exc
                        response = dict(cmd=request['cmd'], id=request.get('id'), data=result, t='')
                    else:
                        if request['cmd'] == 'GatewayRegisterBot':
                            self.server.reserve(self, request['data'])
                            self.registration_profile = dict(request['data'])
                        request['__session_id'] = self.session_id
                        future = self.server.engine.submit(request, self.profile)
                        self.pending_future = future
                        if self.closed.is_set():
                            future.cancel()
                        try:
                            wait = (REGISTRATION_WAIT_SECONDS if request['cmd'] == 'GatewayRegisterBot'
                                    else REQUEST_WAIT_SECONDS)
                            response = future.result(timeout=wait)
                        except FutureTimeout as exc:
                            future.cancel()
                            if request['cmd'] == 'sendTransaction':
                                raise TransactionUncertain(dict(
                                    cmd='transaction_uncertain',
                                    error='Transaction processing timed out; do not replay')) from exc
                            raise BridgeDisconnected('Upstream request deadline exceeded') from exc
                        if request['cmd'] == 'GatewayRegisterBot':
                            self.profile = dict(request['data'])
                except Exception as exc:
                    response = dict(
                        cmd=('transaction_uncertain' if isinstance(exc, TransactionUncertain)
                             else 'gateway_disconnected' if isinstance(exc, (ConnectionError, TimeoutError, OSError))
                             else 'gateway_error'),
                        id=request.get('id'), data=None, error=str(exc), t='')
                self.pending_future = None
                self.stream.send(response)
        except (ConnectionError, OSError):
            pass
        finally:
            self.close()

    def close(self):
        with self._close_lock:
            if self.closed.is_set():
                return
            self.closed.set()
        if self.pending_future is not None:
            self.pending_future.cancel()
        self.callback_ready.set()
        self.outbox.close()
        close_socket(self.request_socket)
        close_socket(self.callback_socket)
        self.server.release(self)


class GatewayServer:
    def __init__(self, engine, pairs=(),
                 host='127.0.0.1'):
        self.engine, self.pairs, self.host = engine, list(pairs), host
        self.lock = threading.RLock()
        self.paired = threading.Condition(self.lock)
        self.sessions, self.reservations, self.sockets, self.threads = {}, {}, [], []
        self.stopped = threading.Event()
        self.engine.listeners.append(self.on_event)

    def reserve(self, session, profile):
        if not isinstance(profile, dict):
            raise ValueError('Invalid bot registration')
        bot_id = profile.get('bot_id')
        with self.lock:
            previous = self.reservations.get(bot_id)
            if previous and previous is not session and not previous.closed.is_set():
                raise ValueError('BOT_ID is already connected through another port pair')
            self.reservations[bot_id] = session

    def release(self, session):
        with self.paired:
            if self.sessions.get(session.slot) is session:
                del self.sessions[session.slot]
            for bot_id, owner in list(self.reservations.items()):
                if owner is session:
                    del self.reservations[bot_id]
            self.paired.notify_all()
        profile = session.profile or session.registration_profile
        if profile and profile.get('bot_id') and not self.stopped.is_set():
            bot_id = profile['bot_id']
            def still_unused():
                with self.lock:
                    current = self.reservations.get(bot_id)
                    return current is None or current.closed.is_set()
            with self.engine.profile_lock:
                self.engine.retire_checks[bot_id] = still_unused
            self.engine.submit(dict(cmd=None, still_unused=still_unused), profile)

    def on_event(self, event, owner):
        raw = encode_frame(event)
        with self.lock:
            sessions = list(self.sessions.values())
        for session in sessions:
            if session.closed.is_set() or not session.profile:
                continue
            if owner and session.profile['bot_id'] != owner:
                continue
            if not owner and event['cmd'] == 'OnFuturesClientHolding':
                data = event.get('data') if isinstance(event.get('data'), dict) else {}
                if (data.get('trdaccid') != session.profile['account']
                        or data.get('sec_code') != session.profile['base_asset_code']):
                    continue
            if not session.outbox.put(raw):
                LOGGER.warning('Slow client disconnected: slot %s', session.slot)
                session.close()
        self.engine.metrics['clients'] = {
            str(s.slot): dict(queue_messages=len(s.outbox.items),
                              queue_bytes=s.outbox.bytes,
                              high_water=s.outbox.high_water,
                              closed=s.closed.is_set())
            for s in sessions}

    def start(self):
        try:
            initial = self.pairs[:]
            self.pairs.clear()
            for pair in initial:
                self.add_pair(*pair)
        except Exception:
            self.close()
            raise
        return self

    def add_pair(self, requests_port, callbacks_port):
        with self.lock:
            pair = (requests_port, callbacks_port)
            if pair != (0, 0) and pair in self.pairs:
                return pair
            if self.stopped.is_set():
                raise BridgeDisconnected('Gateway stopped')
            used = {port for existing in self.pairs for port in existing}
            if (any(not isinstance(p, int) or isinstance(p, bool) or not 0 <= p <= 65535 for p in pair)
                    or (requests_port == callbacks_port and requests_port != 0)
                    or any(p in used for p in pair if p)):
                raise ValueError('Bot ports must be distinct and unused')
            listeners = []
            try:
                for port in pair:
                    listener = socket.socket()
                    listeners.append(listener)
                    if hasattr(socket, 'SO_EXCLUSIVEADDRUSE'):
                        listener.setsockopt(socket.SOL_SOCKET, socket.SO_EXCLUSIVEADDRUSE, 1)
                    else:
                        listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
                    listener.bind((self.host, port))
                    listener.listen(8)
                    listener.settimeout(0.5)
            except Exception:
                for listener in listeners:
                    close_socket(listener)
                raise
            actual = tuple(listener.getsockname()[1] for listener in listeners)
            slot = len(self.pairs) + 1
            self.pairs.append(actual)
            self.sockets.extend(listeners)
            for callback, listener in enumerate(listeners):
                worker = threading.Thread(target=self.accept,
                    args=(listener, slot, bool(callback)), daemon=True)
                self.threads.append(worker)
                worker.start()
            return actual

    def accept(self, listener, slot, callback):
        while not self.stopped.is_set():
            try:
                sock, _ = listener.accept()
            except socket.timeout:
                continue
            except OSError:
                break
            with self.paired:
                session = self.sessions.get(slot)
                if callback:
                    deadline = time.monotonic() + 5
                    while (session is None or session.closed.is_set()) and not self.stopped.is_set():
                        remaining = deadline - time.monotonic()
                        if remaining <= 0:
                            break
                        self.paired.wait(min(remaining, 0.5))
                        session = self.sessions.get(slot)
                    if session and not session.closed.is_set():
                        session.attach(sock)
                    else:
                        close_socket(sock)
                elif session and not session.closed.is_set():
                    close_socket(sock)
                else:
                    self.sessions[slot] = Session(self, slot, sock)
                    self.paired.notify_all()
            # Listener threads must not retain the last disconnected session.
            session = None
            sock = None

    def close(self):
        self.stopped.set()
        with self.paired:
            sessions = list(self.sessions.values())
            self.paired.notify_all()
        for session in sessions:
            session.close()
        for listener in self.sockets:
            close_socket(listener)
        for worker in self.threads:
            worker.join(timeout=2)
        for session in sessions:
            session.request_thread.join(timeout=10)
            if session.callback_thread:
                session.callback_thread.join(timeout=3)
        if self.on_event in self.engine.listeners:
            self.engine.listeners.remove(self.on_event)


def serve(args):
    from gateway_control import ControlServer
    runtime = Path(__file__).resolve().parent / 'runtime'
    runtime.mkdir(exist_ok=True)
    cleanup_logs()
    handler = make_handler(runtime/'gateway.log')
    handler.setFormatter(logging.Formatter('%(asctime)s %(levelname)s %(message)s'))
    LOGGER.setLevel(logging.INFO)
    LOGGER.addHandler(handler)
    LOGGER.propagate = False
    root_logger = logging.getLogger()
    root_logger.addHandler(handler)
    if sys.stderr is not None:
        LOGGER.addHandler(logging.StreamHandler())
    engine = Engine(runtime / 'gateway.sqlite3', args.upstream_host,
                    (args.upstream_request_port, args.upstream_callback_port))
    server = GatewayServer(engine)
    control = None
    try:
        server.start()
        control = ControlServer(server).start()
        LOGGER.info('Gateway started; client ports are allocated on demand')
        while not control.shutdown_requested.wait(0.5):
            pass
    except KeyboardInterrupt:
        pass
    finally:
        if control:
            control.close()
        server.close()
        engine.close()
        LOGGER.removeHandler(handler)
        root_logger.removeHandler(handler)
        handler.close()


def main():
    from gateway_control import FileLock
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--upstream-host', default='127.0.0.1')
    parser.add_argument('--upstream-request-port', type=int, default=34230)
    parser.add_argument('--upstream-callback-port', type=int, default=34231)
    args = parser.parse_args()
    with FileLock(Path(__file__).resolve().parent/'runtime'/'gateway.instance.lock', timeout=0):
        serve(args)


if __name__ == '__main__':
    main()
