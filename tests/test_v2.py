"""Offline regression tests: no connection to a real QUIK or broker."""
import copy
import importlib.util
import json
import itertools
from pathlib import Path
import socket
import tempfile
import threading
import time
import unittest
from unittest.mock import Mock, patch

from QuikPy import QuikPy
from gateway import BoundedOutbox, Engine, GatewayServer
from registry import Registry
from transport import (BridgeDisconnected, BridgeProtocolError, JsonLineSocket,
                       RemoteError, TransactionUncertain, encode_frame)

ROOT = Path(__file__).resolve().parents[1]
TEST_ROOT = ROOT / 'runtime' / 'tests'
TEST_ROOT.mkdir(parents=True, exist_ok=True)


def wait_until(predicate, timeout=2):
    deadline = time.monotonic() + timeout
    while not predicate():
        if time.monotonic() >= deadline:
            raise AssertionError('Condition did not become true')
        time.sleep(0.01)


def profile(number=1):
    return dict(bot_id=f'bot_{number}', class_code='FUTSPREAD',
                sec_code=f'SPREAD_{number}', base_asset_code=f'LEG_{number}', account='DEMO')


class Broker:
    """Deterministic fake of the Lua RPC interface and broker callbacks."""
    def __init__(self):
        self.clients, self.transactions, self.orders, self.timers = [], [], {}, []
        self.filters = []
        self.reject_next = False
        self.lose_next = False
        self.delay = 0.04
        self.actual_account = None
        self.drop_order_events = False
        self.num = 100
        self.inflight = self.maximum_inflight = 0
        self.lock = threading.RLock()

    def factory(self, **kwargs):
        broker = self

        class Client:
            connected = True

            def process_request(self, request):
                with broker.lock:
                    broker.inflight += 1
                    broker.maximum_inflight = max(broker.maximum_inflight, broker.inflight)
                try:
                    time.sleep(0.001)
                    result = broker.call(request, self, kwargs['event_handler'])
                    return dict(cmd=request['cmd'], id=request['id'], data=result, t='')
                finally:
                    with broker.lock:
                        broker.inflight -= 1

            def CloseConnectionAndThread(self):
                self.connected = False

        client = Client()
        self.clients.append(client)
        return client

    def call(self, request, client, handler):
        cmd, data = request['cmd'], request.get('data', '')
        if cmd == 'set_event_filters':
            self.filters = data
            return True
        if cmd == 'ping':
            return 'Pong'
        if cmd == 'echo':
            return data
        if cmd == 'getClassesList':
            return 'FUTSPREAD;SPBFUT;'
        if cmd == 'getSecurityInfo':
            cls, sec = data.split('|')
            valid = (cls == 'FUTSPREAD' and sec.startswith('SPREAD_')) or (cls == 'SPBFUT' and sec.startswith('LEG_'))
            return dict(class_code=cls, sec_code=sec) if valid else None
        if cmd == 'getFuturesClientHoldings':
            return [dict(sec_code=f'LEG_{n}', trdaccid='DEMO', totalnet=0) for n in (1, 2, 3)]
        if cmd == 'getTradeAccounts':
            return [dict(trdaccid='DEMO', class_codes='FUTSPREAD|SPBFUT|')]
        if cmd == 'getParamEx':
            return dict(result='1', param_value='1', param_image='открыта')
        if cmd == 'getInfoParam':
            return 'DEMO'
        if cmd == 'isConnected':
            return 1
        if cmd == 'get_recent_trades':
            return []
        if cmd in ('getOrder_by_Number','get_order_by_number'):
            with self.lock:
                number = data.split('|')[1] if cmd == 'get_order_by_number' else data
                return copy.deepcopy(self.orders.get(int(number)))
        if cmd == 'get_active_orders':
            with self.lock:
                return [copy.deepcopy(order) for order in self.orders.values()
                        if order['class_code'] == data['class_code']
                        and order['sec_code'] == data['sec_code']
                        and order['account'] == data['account']
                        and order['flags'] & 1]
        if cmd == 'sendTransaction':
            with self.lock:
                self.transactions.append(copy.deepcopy(data))
                gid = int(data['TRANS_ID'])
                reject = self.reject_next
                self.reject_next = False
                if data['ACTION'] == 'NEW_ORDER':
                    self.num += 1
                    num = self.num
                    order = dict(order_num=num, trans_id=gid,
                                 class_code=data['CLASSCODE'], sec_code=data['SECCODE'],
                                 account=self.actual_account or data['ACCOUNT'],
                                 flags=5 if data['OPERATION'] == 'S' else 1, price=float(data['PRICE']),
                                 qty=int(data['QUANTITY']), balance=int(data['QUANTITY']))
                    if not reject:
                        self.orders[num] = order
                else:
                    num = int(data['ORDER_KEY'])
                    order = copy.deepcopy(self.orders[num])
                if self.lose_next:
                    self.lose_next = False
                    client.connected = False
                    raise BridgeDisconnected('Connection lost after broker applied the request')

            def deliver():
                if not client.connected:
                    return
                if not reject and data['ACTION'] == 'KILL_ORDER':
                    with self.lock:
                        self.orders[num]['flags'] = 0
                        order['flags'] = 0
                handler(dict(cmd='OnTransReply', data=dict(
                    trans_id=gid, status=4 if reject else 3,
                    order_num=0 if reject else num, result_msg='rejected' if reject else 'accepted')))
                if not reject and not self.drop_order_events:
                    handler(dict(cmd='OnOrder', data=copy.deepcopy(order)))

            timer = threading.Timer(self.delay, deliver)
            self.timers.append(timer)
            timer.start()
            return True
        raise AssertionError('Unexpected mock command: ' + cmd)

    def close(self):
        for timer in self.timers:
            timer.join(timeout=2)


class TransportTests(unittest.TestCase):
    def test_fragmented_json_and_eof(self):
        left, right = socket.socketpair()
        self.addCleanup(left.close)
        self.addCleanup(right.close)
        left.settimeout(0.02)
        value = dict(cmd='echo', id=3, data={"text": "тест 'quoted' \n", "bool": True, "none": None})
        raw = encode_frame(value)

        def send():
            for pos in range(0, len(raw), 3):
                right.sendall(raw[pos:pos+3])
            right.shutdown(socket.SHUT_WR)

        thread = threading.Thread(target=send)
        thread.start()
        stream = JsonLineSocket(left)
        self.assertEqual(stream.read(time.monotonic()+2), value)
        with self.assertRaises(BridgeDisconnected):
            stream.read()
        thread.join()
        self.assertEqual(len(stream.buffer), 0)

    def test_oversized_unterminated_frame(self):
        left, right = socket.socketpair()
        self.addCleanup(left.close)
        self.addCleanup(right.close)
        right.sendall(b'x'*20)
        with self.assertRaises(BridgeProtocolError):
            JsonLineSocket(left, max_bytes=16).read()

    def test_partial_frame_deadline(self):
        left, right = socket.socketpair()
        self.addCleanup(left.close)
        self.addCleanup(right.close)
        left.settimeout(0.02)
        right.sendall(b'{"cmd":')
        stream = JsonLineSocket(left)
        with self.assertRaises(TimeoutError):
            stream.read(time.monotonic()+0.05)
        right.sendall(b'"echo","data":true}\n')
        self.assertEqual(stream.read()['data'], True)

    def test_queue_limits_bytes_and_items(self):
        outbox = BoundedOutbox(max_messages=2, max_bytes=4)
        self.assertTrue(outbox.put(b'aa'))
        self.assertTrue(outbox.put(b'bb'))
        self.assertFalse(outbox.put(b'c'))
        self.assertEqual(outbox.bytes, 4)
        self.assertEqual(outbox.get(), b'aa')
        outbox.close()
        self.assertEqual(outbox.bytes, 0)
        self.assertFalse(outbox.put(b'z'))


class RegistryTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory(dir=TEST_ROOT)
        self.addCleanup(self.directory.cleanup)
        self.path = Path(self.directory.name) / 'ledger.sqlite3'
        self.registry = Registry(self.path)
        self.addCleanup(lambda: self.registry.close())

    def test_ids_survive_restart(self):
        first = self.registry.allocate('bot_1', '2', 'NEW_ORDER')
        self.registry.close()
        self.registry = Registry(self.path)
        second = self.registry.allocate('bot_2', '2', 'NEW_ORDER')
        self.assertGreater(second, first)

    def test_snapshot_cannot_overwrite_newer_event(self):
        p = profile()
        gid = self.registry.allocate(p['bot_id'], '1', 'NEW_ORDER')
        order = dict(order_num=1, trans_id=gid, flags=1,
                     class_code=p['class_code'], sec_code=p['sec_code'], account=p['account'])
        self.registry.on_order(order, {p['bot_id']: p})
        watermark = self.registry.sequence
        inactive = dict(order, flags=0)
        self.registry.on_order(inactive, {p['bot_id']: p})
        self.registry.snapshot([order], p, {p['bot_id']: p}, watermark)
        self.assertEqual(self.registry.active(p), [])

    def test_uncertain_is_not_success(self):
        self.registry.allocate('bot_1', '9', 'NEW_ORDER')
        with self.assertRaises(TransactionUncertain):
            self.registry.wait('bot_1', '9', 0.01)
        self.assertEqual(self.registry.unresolved('bot_1'), 1)

    def test_cached_unfilled_cancellation_requires_exact_terminal_evidence(self):
        p = profile(); num = '2024401013557188673'
        gid = self.registry.allocate('bot_1', '2', 'NEW_ORDER')
        self.registry.on_reply(dict(trans_id=gid, status=3, order_num=int(num)))
        order = dict(order_num=int(num),trans_id=gid,class_code=p['class_code'],
                     sec_code=p['sec_code'],account=p['account'],flags=30,qty=1,balance=1,
                     withdraw_datetime=dict(year=2026,month=10,day=28,hour=23,min=50,sec=0))
        self.registry.on_order(order, {'bot_1':p})
        self.assertEqual(self.registry.confirmed_unfilled_cancellation(p,num),order)
        variants = [dict(order,flags=29),dict(order,flags=28,balance=0),
                    dict(order,qty=2,balance=1),dict(order,account='OTHER'),
                    dict(order,sec_code='OTHER'),dict(order,trans_id=gid+1),
                    dict(order,order_num=int(num)+1),dict(order,withdraw_datetime={}),
                    dict(order,balance='nan'),dict(order,qty=0,balance=0)]
        for item in variants:
            with self.subTest(order=item):
                self.registry.db.execute('UPDATE orders SET payload=? WHERE num=?', (json.dumps(item),num))
                self.assertIsNone(self.registry.confirmed_unfilled_cancellation(p,num))
        self.registry.db.execute('UPDATE orders SET payload=?,active=1 WHERE num=?',(json.dumps(order),num))
        self.assertIsNone(self.registry.confirmed_unfilled_cancellation(p,num))
        self.registry.db.execute('UPDATE orders SET active=0 WHERE num=?',(num,))
        self.registry.db.execute("UPDATE transactions SET state='uncertain' WHERE gid=?",(gid,))
        self.assertIsNone(self.registry.confirmed_unfilled_cancellation(p,num))
        self.registry.db.execute("UPDATE transactions SET state='accepted' WHERE gid=?",(gid,))
        self.registry.db.execute('UPDATE orders SET active=0,bot=? WHERE num=?',('bot_2',num))
        self.assertIsNone(self.registry.confirmed_unfilled_cancellation(p,num))

    def test_broker_status_distinguishes_refusal_from_unknown_outcome(self):
        for status in (12, 15, 99):
            gid = self.registry.allocate('bot_1', str(status), 'NEW_ORDER')
            self.registry.on_reply(dict(trans_id=gid, status=status, order_num=0))
            self.assertEqual(self.registry.transaction(gid)['state'], 'uncertain')
        gid = self.registry.allocate('bot_2', '2', 'NEW_ORDER')
        self.registry.on_reply(dict(trans_id=gid, status=2, order_num=0))
        self.assertEqual(self.registry.transaction(gid)['state'], 'rejected')

    def test_late_ambiguous_reply_cannot_override_confirmed_acceptance(self):
        gid = self.registry.allocate('bot_1', '2', 'NEW_ORDER')
        self.registry.on_reply(dict(trans_id=gid, status=3, order_num=123))
        self.registry.on_reply(dict(trans_id=gid, status=12, order_num=0))
        self.assertEqual(self.registry.transaction(gid)['state'], 'accepted')
        self.assertEqual(self.registry.transaction(gid)['order_num'], '123')

    def test_wrong_account_order_is_not_verified_even_with_known_transaction(self):
        p = profile()
        gid = self.registry.allocate('bot_1','2','NEW_ORDER')
        self.registry.on_reply(dict(trans_id=gid,status=3,order_num=123))
        order = dict(order_num=123,trans_id=gid,flags=1,class_code=p['class_code'],
            sec_code=p['sec_code'],account='OTHER')
        self.assertIsNone(self.registry.on_order(order,{'bot_1':p}))
        self.assertEqual(len(self.registry.missing_orders('bot_1')),1)
        self.assertEqual(self.registry.active(p),[])
        order['account'] = p['account']
        self.assertEqual(self.registry.on_order(order,{'bot_1':p}),'bot_1')
        self.assertEqual(self.registry.missing_orders('bot_1'),[])

    def test_restart_snapshot_removes_stale_active_order(self):
        p = profile()
        gid = self.registry.allocate('bot_1', '1', 'NEW_ORDER')
        order = dict(order_num=1, trans_id=gid, flags=1,
                     class_code=p['class_code'], sec_code=p['sec_code'], account=p['account'])
        self.registry.on_order(order, {'bot_1': p})
        self.registry.close()
        self.registry = Registry(self.path)
        self.registry.snapshot([], p, {'bot_1': p}, self.registry.sequence)
        self.assertEqual(self.registry.active(p), [])


class GatewayTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory(dir=TEST_ROOT)
        self.broker = Broker()
        self.engine = Engine(
            Path(self.directory.name)/'ledger.sqlite3',
            client_factory=self.broker.factory)
        self.server = GatewayServer(self.engine, pairs=((0, 0), (0, 0), (0, 0))).start()
        self.pairs = [(self.server.sockets[2*n].getsockname()[1],
                       self.server.sockets[2*n+1].getsockname()[1]) for n in range(3)]
        self.clients = []
        self.addCleanup(self.cleanup)

    def cleanup(self):
        for client in self.clients:
            client.CloseConnectionAndThread()
        self.server.close()
        self.broker.close()
        self.engine.close()
        self.directory.cleanup()

    def connect(self, n=1, register=True):
        ports = self.pairs[n-1]
        client = QuikPy(requests_port=ports[0], callbacks_port=ports[1])
        self.clients.append(client)
        if register:
            p = profile(n)
            client.RegisterBot(p['bot_id'], p['class_code'], p['sec_code'],
                               p['base_asset_code'], p['account'])
        return client

    def transaction(self, n=1, trans_id=2, qty=1):
        p = profile(n)
        return dict(TRANS_ID=str(trans_id), ACTION='NEW_ORDER', TYPE='L', OPERATION='B',
                    CLASSCODE=p['class_code'], SECCODE=p['sec_code'],
                    ACCOUNT=p['account'], PRICE='10', QUANTITY=str(qty))

    def create_order(self, client, n=1, trans_id=2):
        client.SendTransaction(self.transaction(n, trans_id))
        client.WaitTransaction(trans_id)
        wait_until(lambda: len(self.engine.registry.active(profile(n))) == 1)
        return self.engine.registry.active(profile(n))[0]

    def test_rollover_uses_owned_unfilled_cancellation_without_trading(self):
        client = self.connect()
        p = profile(); num = 2024401013557188673
        gid = self.engine.registry.allocate('bot_1','88','NEW_ORDER')
        self.engine.registry.on_reply(dict(trans_id=gid,status=3,order_num=num))
        order = dict(order_num=num,trans_id=gid,class_code=p['class_code'],sec_code=p['sec_code'],
            account=p['account'],flags=30,qty=1,balance=1,price=1540,
            withdraw_datetime=dict(year=2026,month=10,day=28,hour=23,min=50,sec=0))
        self.engine.registry.on_order(order,{'bot_1':p})
        self.assertIsNone(client.GetOrderByClassNumber(p['class_code'],str(num))['data'])
        self.assertEqual(client.GetConfirmedUnfilledCancellation(str(num))['data'],order)
        spec=importlib.util.spec_from_file_location('grid_rollover',ROOT/'main.py')
        bot=importlib.util.module_from_spec(spec);spec.loader.exec_module(bot)
        bot.CLASS,bot.SECCODE,bot.ACCOUNT=p['class_code'],p['sec_code'],p['account']
        bot.base_price=1400;bot.grid_order_numbers.add(str(num))
        self.assertIsNone(bot.completed_grid_level(client))
        self.assertFalse(bot.grid_order_numbers)
        self.assertEqual(bot.base_price,1400)
        self.assertEqual(self.broker.transactions,[])

    def test_calendar_spread_far_leg_position_through_shared_gateway(self):
        original = self.broker.call
        holdings = []
        def with_spread_legs(request, client, handler):
            if request['cmd'] == 'getFuturesClientHoldings':
                return copy.deepcopy(holdings)
            return original(request, client, handler)
        self.broker.call = with_spread_legs
        client = self.connect(1)
        spec = importlib.util.spec_from_file_location('spread_position_audit', ROOT/'main.py')
        bot = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(bot)
        bot.CLASS, bot.SECCODE, bot.BASE_ASSET_CODE, bot.ACCOUNT = 'FUTSPREAD', 'SPREAD_1', 'LEG_1', 'DEMO'
        for far_qty in (7, -5, 0):
            holdings[:] = [dict(sec_code='NEAR_1', trdaccid='DEMO', totalnet=-far_qty),
                          dict(sec_code='LEG_1', trdaccid='OTHER', totalnet=99),
                          dict(sec_code='SPREAD_1', trdaccid='DEMO', totalnet=88),
                          dict(sec_code='LEG_1', trdaccid='DEMO', totalnet=far_qty)]
            with self.subTest(far_qty=far_qty):
                self.assertEqual(bot.get_current_position(client), far_qty)
                self.assertTrue(all(row['trdaccid']=='DEMO' for row in client.GetFuturesHoldings()['data']))
        # Position is read from LEG_1, but transactions still target the spread.
        self.create_order(client)
        self.assertEqual(self.broker.transactions[-1]['SECCODE'], 'SPREAD_1')
        self.assertEqual(self.broker.transactions[-1]['CLASSCODE'], 'FUTSPREAD')

    def test_position_callbacks_route_by_far_leg_and_account(self):
        one, two = self.connect(1), self.connect(2)
        received = [[], []]
        one.OnFuturesClientHolding = lambda msg: received[0].append(msg['data'])
        two.OnFuturesClientHolding = lambda msg: received[1].append(msg['data'])
        events = [dict(sec_code='LEG_1', trdaccid='OTHER', totalnet=99),
                  dict(sec_code='NEAR_1', trdaccid='DEMO', totalnet=-3),
                  dict(sec_code='SPREAD_1', trdaccid='DEMO', totalnet=88),
                  dict(sec_code='LEG_1', trdaccid='DEMO', totalnet=3),
                  dict(sec_code='LEG_2', trdaccid='DEMO', totalnet=-4)]
        for data in events:
            self.server.on_event(dict(cmd='OnFuturesClientHolding', data=data), None)
        wait_until(lambda: bool(received[0]) and bool(received[1]))
        self.assertEqual(received, [[events[-2]], [events[-1]]])

    def test_three_clients_one_upstream_and_unique_transaction_ids(self):
        clients = [self.connect(n) for n in (1, 2, 3)]
        events = [[], [], []]
        for index, client in enumerate(clients):
            client.OnOrder = lambda message, i=index: events[i].append(message['data'])
        failures = []

        def order(n):
            try:
                self.create_order(clients[n-1], n)
                self.assertEqual(clients[n-1].Echo({"name": f"bot '{n}'"})['data'], {"name": f"bot '{n}'"})
            except Exception as exc:
                failures.append(exc)

        threads = [threading.Thread(target=order, args=(n,)) for n in (1, 2, 3)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()
        self.assertEqual(failures, [])
        self.assertEqual(len(self.broker.clients), 1)
        self.assertEqual(self.broker.maximum_inflight, 1)
        self.assertEqual(len({item['TRANS_ID'] for item in self.broker.transactions}), 3)
        wait_until(lambda: all(events))
        for n, received in enumerate(events, 1):
            self.assertTrue(all(item['sec_code'] == profile(n)['sec_code'] for item in received))
            self.assertTrue(all(item['trans_id'] == 2 for item in received))

    def test_rejected_transaction(self):
        client = self.connect()
        self.broker.reject_next = True
        client.SendTransaction(self.transaction())
        with self.assertRaises(RemoteError):
            client.WaitTransaction(2)
        self.assertEqual(self.engine.registry.active(profile()), [])

    def test_rewritten_account_without_order_callback_blocks_new_orders(self):
        client = self.connect()
        self.broker.actual_account = 'ACTUAL_ACCOUNT'
        self.broker.drop_order_events = True
        client.SendTransaction(self.transaction())
        with self.assertRaisesRegex(RemoteError,'ACTUAL_ACCOUNT'):
            client.WaitTransaction(2)
        with self.assertRaises(TransactionUncertain):
            client.SendTransaction(self.transaction(trans_id=3))
        self.assertEqual(len(self.broker.transactions),1)
        self.assertEqual(len(self.engine.registry.missing_orders('bot_1')),1)

    def test_missing_order_callback_does_not_make_grid_appear_empty(self):
        client = self.connect()
        self.broker.drop_order_events = True
        self.broker.num = 1953477698907386300
        order = self.create_order(client)
        self.assertEqual(order['order_num'],1953477698907386301)
        self.assertEqual(len(client.GetOrders('FUTSPREAD','SPREAD_1')['data']),1)
        self.assertEqual(self.engine.registry.missing_orders('bot_1'),[])

    def test_grid_stays_visible_and_replacement_cancels_all_old_orders(self):
        client = self.connect()
        self.broker.drop_order_events = True
        spec = importlib.util.spec_from_file_location('grid_integration',ROOT/'main.py')
        bot = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(bot)
        bot.CLASS,bot.SECCODE,bot.BASE_ASSET_CODE,bot.ACCOUNT = 'FUTSPREAD','SPREAD_1','LEG_1','DEMO'
        bot.LEVELS,bot.LOT_PER_LEVEL,bot.GRID_STEP,bot.MAX_LOTS_TOTAL = 2,2,1,15
        bot._cached_step = 1
        with patch.object(bot,'wait_until_tradable'):
            bot.set_grid(client,100)
            first = set(bot.find_active_orders(client))
            self.assertEqual(len(first),4)
            for _ in range(5):
                if not bot.find_active_orders(client):
                    bot.set_grid(client,100)
            self.assertEqual(len(self.broker.transactions),4)
            bot.set_grid(client,101)
            second = set(bot.find_active_orders(client))
        self.assertEqual(len(second),4)
        self.assertTrue(first.isdisjoint(second))
        self.assertTrue(all(not self.broker.orders[num]['flags'] & 1 for num in first))
        self.assertEqual(len(self.broker.transactions),12)

    def test_accepted_order_absent_from_terminal_blocks_new_orders(self):
        client = self.connect()
        self.broker.drop_order_events = True
        client.SendTransaction(self.transaction())
        self.broker.orders.clear()
        with self.assertRaises(TransactionUncertain):
            client.WaitTransaction(2)
        with self.assertRaises(TransactionUncertain):
            client.SendTransaction(self.transaction(trans_id=3))
        self.assertEqual(len(self.broker.transactions),1)

    def test_partial_buy_and_full_sell_restore_grid_within_position_limit(self):
        client = self.connect()
        spec = importlib.util.spec_from_file_location('opposite_grid', ROOT/'main.py')
        bot = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(bot)
        bot.CLASS,bot.SECCODE,bot.BASE_ASSET_CODE,bot.ACCOUNT = 'FUTSPREAD','SPREAD_1','LEG_1','DEMO'
        bot.LEVELS,bot.LOT_PER_LEVEL,bot.GRID_STEP,bot.MAX_LOTS_TOTAL = 2,2,1,15
        bot._cached_step = 1
        with patch.object(bot,'wait_until_tradable'), patch.object(bot,'get_current_position',side_effect=itertools.chain([12], itertools.repeat(11))):
            bot.set_grid(client,100)
            initial = list(client.GetOrders(bot.CLASS,bot.SECCODE)['data'])
            self.assertEqual(len(initial),3)  # Position 12 permits only one 2-lot buy.
            buy = next(o for o in initial if o['price']==99)
            sell = next(o for o in initial if o['price']==101)
            for order,balance in ((buy,1),(sell,0)):
                order['balance'] = balance
                if not balance:
                    order['flags'] &= ~1
                self.broker.orders[order['order_num']] = order
                self.engine.on_event(dict(cmd='OnOrder',data=order))
            bot.prev_position,bot.base_price = 12,100
            self.assertFalse(bot.check_levels_executed(11,client))
            filled = bot.completed_grid_level(client)
            self.assertEqual(filled['order_num'],sell['order_num'])
            with patch.object(bot,'write_in_file'):
                self.assertTrue(bot.check_levels_executed(11,client,filled))
            self.assertEqual(bot.base_price,101)
            bot.set_grid(client,bot.base_price)
            replacement = client.GetOrders(bot.CLASS,bot.SECCODE)['data']
            self.assertEqual(len(replacement),4)
            self.assertTrue(all(o['qty']==o['balance']==2 for o in replacement))
            self.assertEqual(len(bot.grid_order_numbers),4)
            self.assertFalse(self.broker.orders[buy['order_num']]['flags'] & 1)
            self.assertLessEqual(11+sum(o['balance'] for o in replacement if not o['flags'] & 4),15)

    def test_fill_between_snapshot_and_cancel_allows_confirmed_grid_replacement(self):
        client = self.connect()
        spec = importlib.util.spec_from_file_location('cancel_race_grid',ROOT/'main.py')
        bot = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(bot)
        bot.CLASS,bot.SECCODE,bot.BASE_ASSET_CODE,bot.ACCOUNT = 'FUTSPREAD','SPREAD_1','LEG_1','DEMO'
        bot.LEVELS,bot.LOT_PER_LEVEL,bot.GRID_STEP,bot.MAX_LOTS_TOTAL = 2,2,1,15
        bot._cached_step = 1
        original_send = client.SendTransaction
        race = []
        def fill_before_cancel(transaction):
            if transaction['ACTION']=='KILL_ORDER' and not race:
                number = int(transaction['ORDER_KEY'])
                race.append(number)
                with self.broker.lock:
                    self.broker.orders[number]['balance'] = 0
                    self.broker.orders[number]['flags'] &= ~1
                    self.broker.reject_next = True
            return original_send(transaction)
        with patch.object(bot,'wait_until_tradable'), patch.object(bot,'get_current_position',side_effect=itertools.chain([0], itertools.repeat(2))):
            bot.set_grid(client,100)
            original = set(bot.grid_order_numbers)
            with patch.object(client,'SendTransaction',side_effect=fill_before_cancel):
                bot.set_grid(client,101)
        replacement = client.GetOrders(bot.CLASS,bot.SECCODE)['data']
        self.assertEqual(len(race),1)
        self.assertEqual(len(replacement),4)
        self.assertTrue(original.isdisjoint(bot.grid_order_numbers))
        self.assertTrue(all(not self.broker.orders[int(n)]['flags'] & 1 for n in original))
        killed = [t for t in self.broker.transactions if t['ACTION']=='KILL_ORDER']
        self.assertEqual(len(killed),4)  # No retry of the rejected cancellation.
        self.assertEqual(sum(t['ACTION']=='NEW_ORDER' for t in self.broker.transactions),8)

    def test_empty_snapshot_is_not_proof_that_an_order_was_cancelled(self):
        client = self.connect()
        order = self.create_order(client)
        original = self.broker.call
        def empty_snapshot(request,client,handler):
            if request['cmd'] == 'get_active_orders':
                return []
            return original(request,client,handler)
        self.broker.call = empty_snapshot
        client.RefreshOrders()
        self.assertEqual([o['order_num'] for o in client.GetOrders('FUTSPREAD','SPREAD_1')['data']],
                         [order['order_num']])
        self.assertEqual(len(self.broker.transactions),1)

    def test_lost_status_of_previously_verified_order_blocks_replacement(self):
        client = self.connect()
        self.create_order(client)
        self.broker.orders.clear()
        with self.assertRaises(TransactionUncertain):
            client.RefreshOrders()
        self.assertEqual(len(self.engine.registry.missing_orders('bot_1')),1)
        with self.assertRaises(TransactionUncertain):
            client.SendTransaction(self.transaction(trans_id=3))
        self.assertEqual(len(self.broker.transactions),1)

    def test_registration_continues_when_lua_omits_nil_security_data(self):
        original_factory = self.broker.factory
        def factory(**kwargs):
            client = original_factory(**kwargs)
            process = client.process_request
            def legacy_lua(request):
                response = process(request)
                if response['data'] is None:
                    response.pop('data')
                return response
            client.process_request = legacy_lua
            return client
        self.engine.client_factory = factory
        client = self.connect()
        self.assertEqual(client.GetOrders('FUTSPREAD','SPREAD_1')['data'], [])
        self.assertEqual(self.broker.transactions, [])

    def test_empty_lua_holdings_object_is_an_empty_position_list(self):
        original = self.broker.call
        def legacy_empty(request, client, handler):
            if request['cmd'] == 'getFuturesClientHoldings':
                return {}
            return original(request,client,handler)
        self.broker.call = legacy_empty
        client = self.connect()
        self.assertEqual(client.GetFuturesHoldings()['data'], [])
        self.assertEqual(self.broker.transactions, [])

    def test_registration_accepts_comma_separated_quik_classes(self):
        original = self.broker.call
        def comma_classes(request,client,handler):
            if request['cmd'] == 'getClassesList':
                return 'TQBR, FUTSPREAD, SPBFUT,'
            return original(request,client,handler)
        self.broker.call = comma_classes
        self.connect()
        self.assertIn('bot_1',self.engine.profiles)
        self.assertEqual(self.broker.transactions, [])

    def test_registration_has_a_separate_multi_rpc_deadline(self):
        original = self.broker.call
        def delayed(request,client,handler):
            if request['cmd'] == 'getSecurityInfo':
                time.sleep(0.6)
            return original(request,client,handler)
        self.broker.call = delayed
        client = self.connect(register=False)
        client.request_timeout = 0.02
        # A normal request's tiny test deadline must not apply to registration.
        with patch('gateway.REQUEST_WAIT_SECONDS',0.02), patch('gateway.REGISTRATION_WAIT_SECONDS',5):
            p = profile()
            client.RegisterBot(p['bot_id'],p['class_code'],p['sec_code'],p['base_asset_code'],p['account'])
        self.assertEqual(self.broker.transactions, [])

    def test_missing_security_has_actionable_error_instead_of_keyerror(self):
        client = self.connect(register=False)
        with self.assertRaisesRegex(RemoteError,'Instrument not found'):
            client.RegisterBot('bot_1','FUTSPREAD','DOES_NOT_EXIST','LEG_1','DEMO')

    def test_delayed_cancellation_confirmation(self):
        client = self.connect()
        order = self.create_order(client)
        self.broker.delay = 0.12
        client.SendTransaction(dict(
            TRANS_ID='3', ACTION='KILL_ORDER', CLASSCODE='FUTSPREAD',
            SECCODE='SPREAD_1', ORDER_KEY=str(order['order_num'])))
        self.assertEqual(len(client.GetOrders('FUTSPREAD','SPREAD_1')['data']), 1)
        client.WaitTransaction(3)
        wait_until(lambda: client.GetOrders('FUTSPREAD','SPREAD_1')['data'] == [])

    def test_cannot_cancel_another_bot_order(self):
        one, two = self.connect(1), self.connect(2)
        order = self.create_order(one)
        with self.assertRaises(RemoteError):
            two.SendTransaction(dict(
                TRANS_ID='4', ACTION='KILL_ORDER', CLASSCODE='FUTSPREAD',
                SECCODE='SPREAD_2', ORDER_KEY=str(order['order_num'])))
        self.assertEqual(len(self.broker.transactions), 1)

    def test_overflowed_client_does_not_stop_other_bot(self):
        one, two = self.connect(1), self.connect(2)
        session = self.server.sessions[2]
        session.outbox.max_messages = 0
        self.server.on_event(dict(cmd='OnOrder', data=dict(order_num=10)), 'bot_2')
        wait_until(lambda: session.closed.is_set())
        self.assertNotIn(2, self.server.sessions)
        self.assertEqual(one.Ping()['data'], 'Pong')

    def test_closed_client_stops_callback_thread(self):
        client = self.connect()
        thread = client.callback_thread
        client.CloseConnectionAndThread()
        self.assertFalse(thread.is_alive())
        with self.assertRaises(BridgeDisconnected):
            client.Ping()

    def test_transport_loss_does_not_replay_transaction(self):
        client = self.connect()
        self.broker.lose_next = True
        with self.assertRaises(TransactionUncertain):
            client.SendTransaction(self.transaction())
        self.assertEqual(len(self.broker.transactions), 1)
        client.RefreshOrders()  # Reconcile the order accepted before the lost response.
        self.assertEqual(len(self.broker.transactions), 1)
        self.assertEqual(len(client.GetOrders('FUTSPREAD','SPREAD_1')['data']), 1)


    def test_same_transaction_id_is_never_replayed(self):
        client = self.connect()
        self.create_order(client)
        with self.assertRaises(RemoteError):
            client.SendTransaction(self.transaction())
        self.assertEqual(len(self.broker.transactions), 1)

    def test_partial_fill_before_order_is_routed_to_owner(self):
        client = self.connect()
        self.broker.delay = 0.15
        received = []
        client.OnTrade = lambda event: received.append(event['data'])
        client.SendTransaction(self.transaction(qty=2))
        order = next(iter(self.broker.orders.values()))
        trade = dict(order, trade_num=500, qty=1, price=10, balance=1)
        self.engine.on_event(dict(cmd='OnTrade', data=trade))
        self.assertEqual(received, [])
        client.WaitTransaction(2)
        wait_until(lambda: len(received) == 1)
        self.assertEqual(received[0]['qty'], 1)
        partial = dict(order, balance=1)
        self.engine.on_event(dict(cmd='OnOrder', data=partial))
        self.assertEqual(client.GetOrders('FUTSPREAD', 'SPREAD_1')['data'][0]['balance'], 1)
        self.assertEqual(client.GetTrades('FUTSPREAD', 'SPREAD_1')['data'][0]['trade_num'], 500)

    def test_reconnect_same_port_can_register(self):
        client = self.connect()
        for _ in range(3):
            client.Reconnect()
            self.assertEqual(client.Ping()['data'], 'Pong')


    def test_slow_socket_client_is_disconnected(self):
        request_port, callback_port = self.pairs[1]
        request_socket = socket.create_connection(('127.0.0.1', request_port))
        callback_socket = socket.create_connection(('127.0.0.1', callback_port))
        self.addCleanup(request_socket.close)
        self.addCleanup(callback_socket.close)
        request_socket.settimeout(2)
        stream = JsonLineSocket(request_socket)
        stream.send(dict(cmd='GatewayRegisterBot', id=0, data=profile(2)))
        self.assertEqual(stream.read()['cmd'], 'GatewayRegisterBot')
        client = self.connect(1)
        session = self.server.sessions[2]
        session.callback_socket.setsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF, 4096)
        session.outbox.max_bytes = 16384
        for _ in range(100):
            self.server.on_event(dict(cmd='OnConnected', data='x'*8192), 'bot_2')
            if session.closed.is_set():
                break
        wait_until(session.closed.is_set, 4)
        self.assertEqual(client.Ping()['data'], 'Pong')

    def test_periodic_refresh_runs_for_each_profile_under_load(self):
        one, two = self.connect(1), self.connect(2)
        self.engine.last_snapshot = {'bot_1': 0, 'bot_2': 0}
        for _ in range(5):
            one.Ping()
        self.assertGreater(self.engine.last_snapshot['bot_1'], 0)
        self.assertGreater(self.engine.last_snapshot['bot_2'], 0)

    def test_symbol_and_account_validation(self):
        client = self.connect(register=False)
        with self.assertRaises(RemoteError):
            client.RegisterBot('bad', 'FUTSPREAD', 'MISSING', 'LEG_1', 'DEMO')
        with self.assertRaises(RemoteError):
            client.RegisterBot('bad', 'FUTSPREAD', 'SPREAD_1', 'LEG_1', 'LIVE_ACCOUNT')

    def test_more_than_three_bots_share_one_upstream(self):
        for _ in range(3):
            self.pairs.append(self.server.add_pair(0, 0))
        clients = [self.connect(n) for n in range(1, 7)]
        for n, client in enumerate(clients, 1):
            self.create_order(client, n)
        self.assertEqual(len(self.engine.profiles), 6)
        self.assertEqual(len(self.broker.clients), 1)
        self.assertEqual(len({t['TRANS_ID'] for t in self.broker.transactions}), 6)

    def test_closed_empty_profile_can_release_its_working_leg(self):
        client = self.connect(1)
        client.CloseConnectionAndThread()
        wait_until(lambda: 'bot_1' not in self.engine.profiles)
        another = self.connect(2, register=False)
        another.RegisterBot('replacement', 'FUTSPREAD', 'SPREAD_1', 'LEG_1', 'DEMO')

    def test_valid_account_has_no_environment_mode(self):
        original = self.broker.call
        def with_account(request, client, handler):
            if request['cmd'] == 'getTradeAccounts':
                return [{'trdaccid': 'ACCOUNT_WITH_ANY_NAME'}]
            return original(request, client, handler)
        self.broker.call = with_account
        client = self.connect(register=False)
        result = client.RegisterBot('bot_1', 'FUTSPREAD', 'SPREAD_1', 'LEG_1', 'ACCOUNT_WITH_ANY_NAME')['data']
        self.assertNotIn('mode', result)
        transaction = self.transaction()
        transaction['ACCOUNT'] = 'ACCOUNT_WITH_ANY_NAME'
        client.SendTransaction(transaction)
        self.assertEqual(client.WaitTransaction(2)['data']['status'], 3)


class BotTests(unittest.TestCase):
    def setUp(self):
        spec = importlib.util.spec_from_file_location('bot_audit', ROOT/'main.py')
        self.bot = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(self.bot)

    def test_funds_refusal_keeps_bot_alive_and_throttles_direction(self):
        bot = self.bot
        client = Mock()
        refusal = dict(status=4, order_num=0, result_msg='[GW][332] Нехватка средств по лимитам клиента')
        client.WaitTransaction.side_effect = [RemoteError(dict(error=str(refusal))),
                                              {'data': {'order_num': 123}}]
        with patch.object(bot, 'get_price_step', return_value=0.01), patch.object(bot.time, 'monotonic', return_value=100):
            self.assertIsNone(bot.send_limit_order(client, 53.2, 2, 'B'))
            self.assertIsNone(bot.send_limit_order(client, 53.2, 2, 'B'))
            self.assertEqual(client.SendTransaction.call_count, 1)
            self.assertEqual(bot.send_limit_order(client, 53.3, 2, 'S')['order_num'], 123)
        client.WaitTransaction.side_effect = None
        client.WaitTransaction.return_value = {'data': {'order_num': 124}}
        with patch.object(bot, 'get_price_step', return_value=0.01), patch.object(bot.time, 'monotonic', return_value=161):
            self.assertEqual(bot.send_limit_order(client, 53.2, 2, 'B')['order_num'], 124)

    def test_other_refusals_and_uncertain_orders_are_not_retried(self):
        bot = self.bot
        for failure in (TransactionUncertain(dict(error='Нехватка средств')),
                        RemoteError(dict(data=dict(status=4, order_num=0, result_msg='wrong account'))),
                        RemoteError(dict(data=dict(status=4, order_num=123, result_msg='[GW][332]')))):
            client = Mock()
            client.WaitTransaction.side_effect = failure
            with self.subTest(failure=failure), patch.object(bot, 'get_price_step', return_value=0.01):
                with self.assertRaises(type(failure)):
                    bot.send_limit_order(client, 53.2, 2, 'B')
                self.assertFalse(bot.funds_retry_after)

    def test_rejected_grid_levels_recover_without_cancelling_accepted_orders(self):
        bot = self.bot
        bot.LEVELS, bot.LOT_PER_LEVEL, bot.MAX_LOTS_TOTAL = 1, 2, 6
        client = Mock()
        refusal = dict(status=4, order_num=0, result_msg='[GW][332] Нехватка средств')
        client.WaitTransaction.side_effect = [RemoteError(dict(data=refusal)), {'data': {'order_num': 10}}]
        with patch.object(bot, 'wait_until_tradable'), patch.object(bot, 'cancel_all_orders') as cancel, \
             patch.object(bot, 'get_current_position', return_value=0), \
             patch.object(bot, 'get_price_step', return_value=1), patch.object(bot.time, 'monotonic', return_value=100):
            bot.set_grid(client, 100)
            self.assertEqual(bot.pending_grid_orders, [(100-bot.GRID_STEP, 2, 'B')])
            self.assertEqual(bot.grid_order_numbers, {'10'})
            bot.retry_pending_grid(client)
            client.RefreshOrders.assert_not_called()
            cancel.assert_called_once()
        client.GetOrders.return_value = {'data': [dict(flags=5, balance=2, price=100+bot.GRID_STEP)]}
        client.WaitTransaction.side_effect = None
        client.WaitTransaction.return_value = {'data': {'order_num': 11}}
        with patch.object(bot, 'get_current_position', return_value=0), \
             patch.object(bot, 'get_price_step', return_value=1), patch.object(bot.time, 'monotonic', return_value=161), \
             patch.object(bot, 'cancel_all_orders') as cancel:
            bot.retry_pending_grid(client)
            cancel.assert_not_called()
            self.assertEqual(bot.pending_grid_orders, [])
            self.assertEqual(bot.grid_order_numbers, {'10', '11'})
            self.assertEqual(client.SendTransaction.call_count, 3)

    def test_retry_respects_active_volume_and_position_limit(self):
        bot = self.bot
        bot.MAX_LOTS_TOTAL = 6
        bot.pending_grid_orders[:] = [(99, 2, 'B')]
        client = Mock()
        client.GetOrders.return_value = {'data': [dict(flags=1, balance=2, price=98)]}
        with patch.object(bot, 'get_current_position', return_value=3), patch.object(bot, 'send_limit_order') as send:
            bot.retry_pending_grid(client)
            send.assert_not_called()
            self.assertEqual(len(bot.pending_grid_orders), 1)

    def test_position_uses_far_leg_even_when_near_leg_has_opposite_sign(self):
        bot = self.bot
        bot.SECCODE, bot.BASE_ASSET_CODE, bot.ACCOUNT = 'NEAR_FAR', 'FAR', 'ACCOUNT_A'
        client = Mock()
        for qty in (7, -5, 0):
            client.GetFuturesHoldings.return_value = {'data': [
                dict(sec_code='NEAR', trdaccid='ACCOUNT_A', totalnet=-qty),
                dict(sec_code='NEAR_FAR', trdaccid='ACCOUNT_A', totalnet=100),
                dict(sec_code='FAR', trdaccid='ACCOUNT_B', totalnet=200),
                dict(sec_code='FAR', trdaccid='ACCOUNT_A', totalnet=qty)]}
            with self.subTest(qty=qty):
                self.assertEqual(bot.get_current_position(client), qty)

    def test_missing_far_leg_is_zero_not_near_leg_position(self):
        bot = self.bot
        bot.SECCODE, bot.BASE_ASSET_CODE, bot.ACCOUNT = 'NEAR_FAR', 'FAR', 'ACCOUNT_A'
        client = Mock()
        client.GetFuturesHoldings.return_value = {'data': [
            dict(sec_code='NEAR', trdaccid='ACCOUNT_A', totalnet=-7),
            dict(sec_code='FAR', trdaccid='ACCOUNT_B', totalnet=7)]}
        self.assertEqual(bot.get_current_position(client), 0)

    def test_ambiguous_far_leg_records_are_not_summed_or_selected_arbitrarily(self):
        bot = self.bot
        client = Mock()
        row = dict(sec_code=bot.BASE_ASSET_CODE, trdaccid=bot.ACCOUNT, totalnet=7)
        client.GetFuturesHoldings.return_value = {'data': [row, dict(row, totalnet=2)]}
        with self.assertRaisesRegex(ValueError, 'Ambiguous position'):
            bot.get_current_position(client)

    def test_position_filters_account(self):
        bot = self.bot

        class Data:
            def GetFuturesHoldings(self):
                return {'data': [
                    dict(sec_code=bot.BASE_ASSET_CODE, trdaccid='OTHER', totalnet=9),
                    dict(sec_code=bot.BASE_ASSET_CODE, trdaccid=bot.ACCOUNT, totalnet=2)]}
        self.assertEqual(bot.get_current_position(Data()), 2)

    def test_zero_and_negative_spread_prices(self):
        bot = self.bot

        class Data:
            def GetParamEx(self, *args):
                return {'data': dict(result='1', param_value='0')}
        self.assertEqual(bot.get_current_price(Data()), 0)
        class Negative:
            def GetParamEx(self, *args):
                return {'data': dict(result='1', param_value='-0.008')}
        self.assertEqual(bot.get_current_price(Negative()), -0.008)

    def test_grid_respects_both_sides_of_position_limit(self):
        bot = self.bot
        bot.LEVELS, bot.LOT_PER_LEVEL, bot.MAX_LOTS_TOTAL = 3, 4, 6
        sent = []
        with patch.object(bot, 'wait_until_tradable'), patch.object(bot, 'cancel_all_orders'), patch.object(
                bot, 'get_current_position', return_value=5), patch.object(
                bot, 'send_limit_order', side_effect=lambda qp,p,q,s: sent.append((p,q,s))):
            bot.set_grid(None,100)
        self.assertLessEqual(5+sum(q for p,q,s in sent if s == 'B'),6)
        self.assertGreaterEqual(5-sum(q for p,q,s in sent if s == 'S'),-6)

    def test_atomic_state_and_instrument_isolation(self):
        bot = self.bot
        with tempfile.TemporaryDirectory(dir=TEST_ROOT) as folder:
            bot.STATE_DIR = Path(folder)
            bot.file_name = str(Path(folder)/'state.txt')
            bot.write_in_file(0)
            self.assertEqual(bot.read_from_file(),0)
            bot.SECCODE = 'OTHER'
            self.assertIsNone(bot.read_from_file())
            self.assertFalse((Path(folder)/'state.tmp').exists())


    def test_partial_fill_waits_for_full_level(self):
        bot = self.bot
        bot.LOT_PER_LEVEL = 2
        bot.prev_position, bot.base_price = 0, 100
        with patch.object(bot, 'get_last_trade_price', return_value=99), \
             patch.object(bot, 'write_in_file') as save:
            self.assertFalse(bot.check_levels_executed(1, None))
            self.assertEqual(bot.base_price, 100)
            self.assertTrue(bot.check_levels_executed(2, None))
            self.assertEqual(bot.base_price, 99)
            save.assert_called_once_with(99)

    def test_unaligned_position_counts_relative_fills(self):
        bot = self.bot
        bot.LOT_PER_LEVEL, bot.MAX_LOTS_TOTAL = 3, 20
        for start, partial, filled, next_partial, next_filled in (
                (-2,-4,-5,-7,-8), (2,4,5,7,8), (-2,-1,1,2,4)):
            with self.subTest(start=start), patch.object(bot,'get_last_trade_price',return_value=99), \
                    patch.object(bot,'write_in_file') as save:
                bot.prev_position, bot.base_price = start,100
                self.assertFalse(bot.check_levels_executed(partial,None))
                self.assertEqual(bot.prev_position,start)
                self.assertTrue(bot.check_levels_executed(filled,None))
                self.assertEqual(bot.prev_position,filled)
                self.assertFalse(bot.check_levels_executed(next_partial,None))
                self.assertTrue(bot.check_levels_executed(next_filled,None))
                self.assertFalse(bot.check_levels_executed(next_filled,None))
                self.assertEqual(save.call_count,2)

    def test_opposite_fills_are_detected_even_when_net_position_is_unchanged(self):
        bot = self.bot
        bot.grid_order_numbers.update(('101','102'))
        client = Mock()
        client.GetOrders.return_value = {'data': []}
        client.GetTrades.return_value = {'data': []}
        orders = {number: dict(order_num=int(number), class_code=bot.CLASS,
            sec_code=bot.SECCODE, account=bot.ACCOUNT, flags=24, balance=0, price=price)
            for number,price in (('101',99),('102',101))}
        client.GetOrderByClassNumber.side_effect = lambda cls,num: {'data':orders[num]}
        filled = bot.completed_grid_level(client)
        self.assertIn(filled['order_num'],(101,102))
        bot.prev_position, bot.base_price = 4,100
        with patch.object(bot,'write_in_file') as save:
            self.assertTrue(bot.check_levels_executed(4,client,filled))
            self.assertEqual(bot.base_price,filled['price'])
            save.assert_called_once_with(filled['price'])

    def test_partial_order_is_not_a_completed_level(self):
        bot = self.bot
        bot.grid_order_numbers.add('101')
        client = Mock()
        client.GetOrders.return_value = {'data':[dict(order_num=101,flags=25,qty=2,balance=1)]}
        self.assertIsNone(bot.completed_grid_level(client))
        client.GetOrderByClassNumber.assert_not_called()

    def test_cancelled_order_does_not_reprice_grid(self):
        bot = self.bot
        bot.grid_order_numbers.add('101')
        client = Mock()
        client.GetOrders.return_value = {'data':[]}
        client.GetOrderByClassNumber.return_value = {'data':dict(order_num=101,
            class_code=bot.CLASS,sec_code=bot.SECCODE,account=bot.ACCOUNT,
            flags=26,balance=1,qty=2,price=99)}
        self.assertIsNone(bot.completed_grid_level(client))
        self.assertFalse(bot.grid_order_numbers)

    def test_missing_completed_order_status_blocks_replacement(self):
        bot = self.bot
        bot.grid_order_numbers.add('101')
        client = Mock()
        client.GetOrders.return_value = {'data':[]}
        client.GetOrderByClassNumber.return_value = {'data':None}
        client.GetConfirmedUnfilledCancellation.return_value = {'data':None}
        with self.assertRaises(TransactionUncertain):
            bot.completed_grid_level(client)
        self.assertEqual(bot.grid_order_numbers,{'101'})

    def test_skipped_position_updates_count_full_level(self):
        bot = self.bot
        bot.LOT_PER_LEVEL, bot.MAX_LOTS_TOTAL = 3,20
        bot.prev_position = -2
        with patch.object(bot,'get_last_trade_price',return_value=99), patch.object(bot,'write_in_file'):
            self.assertTrue(bot.check_levels_executed(-9,None))
            self.assertEqual(bot.prev_position,-9)
            self.assertFalse(bot.check_levels_executed(-10,None))

    def test_unaligned_position_closed_keeps_delay(self):
        bot = self.bot
        bot.LOT_PER_LEVEL, bot.prev_position = 3,-2
        with patch.object(bot,'get_last_trade_price',return_value=99), \
                patch.object(bot,'write_in_file'), patch.object(bot.time,'sleep') as sleep:
            self.assertTrue(bot.check_levels_executed(0,None))
            sleep.assert_called_once_with(2)

    def test_cancel_timeout_prevents_new_grid(self):
        bot = self.bot
        with patch.object(bot, 'wait_until_tradable'), patch.object(bot, 'cancel_all_orders', side_effect=TransactionUncertain(
                dict(error='Cancellation delayed'))), \
             patch.object(bot, 'send_limit_order') as send:
            with self.assertRaises(TransactionUncertain):
                bot.set_grid(None, 100)
            send.assert_not_called()

    def test_rejected_cancellation_of_inactive_order_is_reconciled_without_retry(self):
        bot = self.bot
        for rejection_at in ('SendTransaction','WaitTransaction'):
            for flags,balance in ((28,0),(26,2)):
                with self.subTest(rejection_at=rejection_at,flags=flags):
                    client = Mock()
                    getattr(client,rejection_at).side_effect = RemoteError({'error':'cannot cancel'})
                    client.GetOrderByClassNumber.return_value = {'data':dict(order_num=101,
                        class_code=bot.CLASS,sec_code=bot.SECCODE,account=bot.ACCOUNT,
                        flags=flags,balance=balance)}
                    bot.cancel_order(client,101)
                    client.SendTransaction.assert_called_once()
                    client.GetOrderByClassNumber.assert_called_once_with(bot.CLASS,101)
                    client.RefreshOrders.assert_called_once()

    def test_cancel_rejection_waits_for_delayed_terminal_order_update(self):
        bot = self.bot
        active = dict(order_num=101, class_code=bot.CLASS, sec_code=bot.SECCODE,
                      account=bot.ACCOUNT, flags=29, balance=1)
        for terminal in (dict(active, flags=28, balance=0), dict(active, flags=30)):
            with self.subTest(flags=terminal['flags']):
                client = Mock()
                client.WaitTransaction.side_effect = RemoteError({'error': 'cannot cancel'})
                client.GetOrderByClassNumber.side_effect = [{'data': active}, {'data': active}, {'data': terminal}]
                with patch.object(bot.time, 'sleep') as sleep:
                    bot.cancel_order(client, 101)
                self.assertEqual(sleep.call_count, 2)
                client.SendTransaction.assert_called_once()
                self.assertEqual(client.GetOrderByClassNumber.call_count, 3)
                client.RefreshOrders.assert_called_once()

    def test_cancel_recheck_must_keep_matching_exact_order_identity(self):
        bot = self.bot
        active = dict(order_num=101, class_code=bot.CLASS, sec_code=bot.SECCODE,
                      account=bot.ACCOUNT, flags=29, balance=1)
        client = Mock()
        client.WaitTransaction.side_effect = RemoteError({'error': 'cannot cancel'})
        client.GetOrderByClassNumber.side_effect = [{'data': active}, {'data': dict(active, flags=28, account='OTHER')}]
        with patch.object(bot.time, 'sleep'), self.assertRaises(bot.CancellationBlocked):
            bot.cancel_order(client, 101)
        client.SendTransaction.assert_called_once()
        client.RefreshOrders.assert_not_called()

    def test_rejected_cancellation_of_active_partial_order_still_fails(self):
        bot = self.bot
        client = Mock()
        refusal = RemoteError({'error':'cannot cancel active order'})
        client.WaitTransaction.side_effect = refusal
        client.GetOrderByClassNumber.return_value = {'data':dict(order_num=101,
            class_code=bot.CLASS,sec_code=bot.SECCODE,account=bot.ACCOUNT,flags=25,balance=1)}
        with self.assertRaises(RemoteError) as caught:
            bot.cancel_order(client,101)
        self.assertIsInstance(caught.exception,bot.CancellationBlocked)
        client.RefreshOrders.assert_not_called()
        client.SendTransaction.assert_called_once()

    def test_rejected_cancellation_with_unknown_or_wrong_identity_stops(self):
        bot = self.bot
        valid = dict(order_num=101,class_code=bot.CLASS,sec_code=bot.SECCODE,
                     account=bot.ACCOUNT,flags=28,balance=0)
        for order in (None,dict(valid,account='OTHER'),dict(valid,order_num=102),
                      dict(valid,class_code='OTHER'),dict(valid,sec_code='OTHER')):
            with self.subTest(order=order):
                client = Mock()
                client.WaitTransaction.side_effect = RemoteError({'error':'cannot cancel'})
                client.GetOrderByClassNumber.return_value = {'data':order}
                with self.assertRaises(bot.CancellationBlocked):
                    bot.cancel_order(client,101)
                client.SendTransaction.assert_called_once()
                client.RefreshOrders.assert_not_called()

    def test_blocked_cancel_keeps_waiting_and_grid_resumes_only_after_inactive(self):
        bot = self.bot
        client = Mock()
        active = dict(order_num=101, class_code=bot.CLASS, sec_code=bot.SECCODE,
                      account=bot.ACCOUNT, flags=29, balance=1)
        client.GetOrderByClassNumber.side_effect = [{'data': active}, {'data': active},
                                                   {'data': dict(active, flags=28, balance=0)}]
        bot.LEVELS, bot.LOT_PER_LEVEL, bot.MAX_LOTS_TOTAL, bot.GRID_STEP = 1, 2, 20, 1
        def sleeping(_):
            client.SendTransaction.assert_not_called()
        with patch.object(bot, 'wait_until_tradable'), \
             patch.object(bot, 'find_active_orders', side_effect=[[101], []]), \
             patch.object(bot, 'cancel_order', side_effect=bot.CancellationBlocked({'error': 'still active'})) as cancel, \
             patch.object(bot, 'get_current_position', side_effect=[3, 3, 2, 2]), \
             patch.object(bot, 'send_limit_order', return_value={'order_num': 102}) as send, \
             patch.object(bot.time, 'sleep', side_effect=sleeping) as sleep:
            # No new level may be submitted during either active-order observation.
            def no_new_while_waiting(_):
                send.assert_not_called()
            sleep.side_effect = no_new_while_waiting
            bot.set_grid(client, 100)
        self.assertEqual(sleep.call_count, 3)
        cancel.assert_called_once_with(client, 101)
        self.assertEqual(send.call_count, 2)

    def test_stale_active_snapshot_does_not_repeat_rejected_cancel(self):
        bot = self.bot
        client = Mock()
        client.GetOrderByClassNumber.return_value = {'data': dict(order_num=101,
            class_code=bot.CLASS, sec_code=bot.SECCODE, account=bot.ACCOUNT, flags=28, balance=0)}
        with patch.object(bot, 'find_active_orders', side_effect=[[101], [101], []]), \
             patch.object(bot, 'cancel_order', side_effect=bot.CancellationBlocked({'error': 'cannot cancel'})) as cancel, \
             patch.object(bot, 'get_current_position', return_value=2), patch.object(bot.time, 'sleep'):
            bot.cancel_all_orders(client)
        cancel.assert_called_once_with(client, 101)
        self.assertEqual(client.GetOrderByClassNumber.call_count, 2)

    def test_unknown_identity_waits_instead_of_assuming_order_cancelled(self):
        bot = self.bot
        client = Mock()
        terminal = dict(order_num=101, class_code=bot.CLASS, sec_code=bot.SECCODE,
                        account=bot.ACCOUNT, flags=28, balance=0)
        client.GetOrderByClassNumber.side_effect = [{'data': None},
            {'data': dict(terminal, account='OTHER')}, {'data': terminal}]
        with patch.object(bot, 'get_current_position', return_value=2) as position, \
             patch.object(bot.time, 'sleep') as sleep:
            bot.wait_for_inactive_order(client, 101)
        self.assertEqual(position.call_count, 3)
        self.assertEqual(sleep.call_count, 2)
        client.SendTransaction.assert_not_called()

    def test_cancel_wait_is_interruptible_and_logs_at_most_once_per_minute(self):
        import contextlib, io
        bot = self.bot
        client = Mock()
        client.GetOrderByClassNumber.return_value = {'data': dict(order_num=101,
            class_code=bot.CLASS, sec_code=bot.SECCODE, account=bot.ACCOUNT, flags=29, balance=1)}
        output = io.StringIO()
        with patch.object(bot, 'get_current_position', return_value=2), \
             patch.object(bot.time, 'monotonic', side_effect=[100, 102, 161]), \
             patch.object(bot.time, 'sleep', side_effect=[None, None, KeyboardInterrupt]), \
             contextlib.redirect_stdout(output), self.assertRaises(KeyboardInterrupt):
            bot.wait_for_inactive_order(client, 101)
        self.assertEqual(output.getvalue().count('Новая сетка заблокирована'), 2)
        client.SendTransaction.assert_not_called()

    def test_cancel_wait_survives_connection_loss_without_resending_transaction(self):
        bot = self.bot
        client = Mock()
        client.connected = False
        terminal = dict(order_num=101, class_code=bot.CLASS, sec_code=bot.SECCODE,
                        account=bot.ACCOUNT, flags=28, balance=0)
        def restored():
            client.connected = True
        client.Reconnect.side_effect = [BridgeDisconnected('not ready'), restored]
        client.GetOrderByClassNumber.return_value = {'data': terminal}
        with patch('gateway_control.ensure_gateway'), \
             patch.object(bot, 'get_current_position', return_value=2), patch.object(bot.time, 'sleep') as sleep:
            bot.wait_for_inactive_order(client, 101)
        self.assertEqual(client.Reconnect.call_count, 2)
        sleep.assert_called_once_with(2)
        client.SendTransaction.assert_not_called()

    def test_uncertain_cancellation_is_not_suppressed(self):
        bot = self.bot
        client = Mock()
        client.WaitTransaction.side_effect = TransactionUncertain({'error':'confirmation lost'})
        with self.assertRaises(TransactionUncertain):
            bot.cancel_order(client,101)
        client.GetOrderByClassNumber.assert_not_called()
        client.SendTransaction.assert_called_once()

    def test_decimal_price_does_not_use_scientific_notation(self):
        bot = self.bot
        bot._cached_step = 0.001

        class Data:
            transaction = None

            def SendTransaction(self, transaction):
                self.transaction = transaction

            def WaitTransaction(self, trans_id):
                return {'data': {'status': 3}}

        client = Data()
        bot.send_limit_order(client,0.008,2,'B')
        self.assertEqual(client.transaction['PRICE'],'0.008')

    def test_demo_tick_validation_accepts_fractional_grid(self):
        from run_bot import validate_tick
        from types import SimpleNamespace
        settings = SimpleNamespace(GRID_STEP=0.008)
        validate_tick(settings, {'tick': {'param_value': '0.001'}})
        with self.assertRaises(ValueError):
            validate_tick(settings, {'tick': {'param_value': '0.003'}})


if __name__ == '__main__':
    unittest.main()
