"""Compare actual original strategy functions and exercise readiness gates offline."""
import ast
import contextlib
import io
import types
import unittest
from unittest.mock import Mock, patch

from tests import test_v2 as fixtures

ROOT, wait_until = fixtures.ROOT, fixtures.wait_until


class StrategyAuditTests(unittest.TestCase):
    setUp = fixtures.BotTests.setUp

    def test_startup_retries_only_connection_setup_and_closes_failed_client(self):
        bot = self.bot
        first,second = Mock(),Mock()
        first.RegisterBot.side_effect = fixtures.BridgeDisconnected('Lua not ready')
        second.RegisterBot.return_value = {'data':{'tick':{'param_value':'1'},
            'foreign_active_orders':0,'unresolved_transactions':0}}
        with patch.object(bot,'QuikPy',side_effect=[first,second]), \
             patch.object(bot.time,'sleep') as sleep, \
             patch('gateway_control.ensure_gateway'), contextlib.redirect_stdout(io.StringIO()):
            client,result = bot.connect_and_register()
        self.assertIs(client,second)
        first.CloseConnectionAndThread.assert_called_once()
        sleep.assert_called_once_with(bot.RECONNECT_DELAY_SEC)
        first.SendTransaction.assert_not_called()
        second.SendTransaction.assert_not_called()

    def test_startup_waits_for_unresolved_transactions_without_replaying_them(self):
        bot = self.bot
        client = Mock()
        client.RegisterBot.side_effect = fixtures.TransactionUncertain({'error':'Uncertain transaction'})
        with patch.object(bot,'QuikPy',return_value=client) as factory, \
             patch.object(bot.time,'sleep',side_effect=KeyboardInterrupt) as sleep, \
             patch('gateway_control.ensure_gateway'):
            with self.assertRaises(KeyboardInterrupt):
                bot.connect_and_register()
        factory.assert_called_once()
        sleep.assert_called_once_with(bot.RECONNECT_DELAY_SEC)
        client.CloseConnectionAndThread.assert_called_once()
        client.SendTransaction.assert_not_called()

    def original(self, name):
        tree = ast.parse((ROOT.parent / 'NetDCA' / 'main.py').read_text(encoding='utf-8-sig'))
        node = next(n for n in tree.body if isinstance(n, ast.FunctionDef) and n.name == name)
        scope = dict(vars(self.bot))
        scope['time'] = types.SimpleNamespace(sleep=Mock())
        exec(compile(ast.Module(body=[node], type_ignores=[]), '<original strategy>', 'exec'), scope)
        return scope

    def test_original_price_return_and_profit_decisions(self):
        bot = self.bot
        original = self.original('check_base_price_by_grid')
        cases = 0
        with contextlib.redirect_stdout(io.StringIO()), patch.object(bot.time, 'sleep'):
            for lot in (1, 2):
                for position in (-6, -3, -2, -1, 0, 1, 2, 3, 6):
                    for price in (20, 60, 79, 80, 99, 100, 101, 120, 121, 140, 220):
                        for orders in ([], [1], [1, 2]):
                            bot.LOT_PER_LEVEL, bot.GRID_STEP = lot, 20
                            bot.base_price, bot.prev_position = 100, position
                            original.update(LOT_PER_LEVEL=lot, GRID_STEP=20,
                                            base_price=100, prev_position=position)
                            calls_old, calls_new = [], []
                            client = Mock()
                            client.GetParamEx.side_effect = lambda c,s,p: {'data': {
                                'result': '1', 'param_value': str(price - 1 if p == 'BID' else price + 1)}}
                            original.update(find_active_orders=lambda qp: orders,
                                get_price_step=lambda qp: 1,
                                send_limit_order=lambda qp,p,q,s: calls_old.append((p,q,s)))
                            with patch.object(bot, 'find_active_orders', return_value=orders), \
                                 patch.object(bot, 'get_price_step', return_value=1), \
                                 patch.object(bot, 'send_limit_order', side_effect=lambda qp,p,q,s: calls_new.append((p,q,s))):
                                old = original['check_base_price_by_grid'](client, price, position)
                                new = bot.check_base_price_by_grid(client, price, position)
                            if position == 0 and abs(price - 100) > 20:
                                # Intentional correction: a flat position must
                                # also wait for the saved price after a gap.
                                self.assertEqual(new,(False,False,False))
                            else:
                                self.assertEqual(new, old, (lot,position,price,orders))
                            self.assertEqual(calls_new, calls_old)
                            cases += 1
        self.assertEqual(cases, 594)

    def test_original_level_execution_and_two_second_delay(self):
        bot = self.bot
        old = self.original('check_levels_executed')
        with contextlib.redirect_stdout(io.StringIO()), patch.object(bot.time, 'sleep') as sleep:
            for lot in (1,2,4):
                for previous, current in ((0,1),(0,2),(2,1),(2,0),(-2,0),(-2,-4),(4,4)):
                    if previous % lot:
                        # Unaligned startup positions are covered by regression
                        # tests: the original absolute-modulo rule is defective.
                        continue
                    bot.LOT_PER_LEVEL, bot.MAX_LOTS_TOTAL = lot, 15
                    bot.prev_position, bot.base_price = previous, 100
                    old.update(LOT_PER_LEVEL=lot, MAX_LOTS_TOTAL=15, prev_position=previous,
                               base_price=100, get_last_trade_price=lambda qp: 99, write_in_file=Mock())
                    sleep.reset_mock()
                    old['time'].sleep.reset_mock()
                    with patch.object(bot, 'get_last_trade_price', return_value=99), \
                         patch.object(bot, 'write_in_file'):
                        result = bot.check_levels_executed(current, None)
                        self.assertEqual(result, old['check_levels_executed'](current, None))
                    self.assertEqual((bot.base_price,bot.prev_position), (old['base_price'],old['prev_position']))
                    self.assertEqual(sleep.call_args_list, old['time'].sleep.call_args_list)

    def test_wait_rechecks_gates_after_stabilization(self):
        bot = self.bot
        # Session closes again during the first stabilization delay.
        with patch.object(bot, 'check_quik_connection', return_value=True), \
             patch.object(bot, 'check_time', return_value=True), \
             patch.object(bot, 'check_session_status', side_effect=[False,True,False,True,True]), \
             patch.object(bot.time, 'sleep') as sleep:
            bot.wait_until_tradable(None)
        self.assertEqual([c.args[0] for c in sleep.call_args_list],
                         [bot.POLL_MS,bot.STABILIZATION_DELAY_SEC]*2)

    def test_reconnect_delay_is_preserved_and_rechecked(self):
        bot = self.bot
        with patch.object(bot, 'check_quik_connection', side_effect=[False,True,True]), \
             patch.object(bot, 'check_time', return_value=True), \
             patch.object(bot, 'check_session_status', return_value=True), \
             patch.object(bot.time, 'sleep') as sleep:
            bot.wait_until_tradable(None)
        self.assertEqual([c.args[0] for c in sleep.call_args_list],
                         [bot.POLL_MS*bot.RECONNECT_POLL_SEC,bot.RECONNECT_DELAY_SEC])

    def test_duplicate_side_and_price_below_count_threshold(self):
        bot = self.bot
        bot.LEVELS = 2
        client = Mock()
        client.GetOrders.return_value = {'data': [
            {'order_num':1, 'flags':1, 'price':99},
            {'order_num':2, 'flags':1, 'price':99.0}]}
        self.assertTrue(bot.has_duplicate_orders(client))
        client.GetOrders.return_value['data'][1]['flags'] = 5
        self.assertFalse(bot.has_duplicate_orders(client))

    def test_normal_position_close_waits_two_seconds_once(self):
        bot = self.bot
        bot.prev_position, bot.base_price = 2, 100
        with patch.object(bot, 'get_last_trade_price', return_value=99), \
             patch.object(bot.time, 'sleep') as sleep:
            self.assertEqual(bot.check_base_price_by_grid(None,99,0), (True,False,True))
            self.assertEqual(bot.check_base_price_by_grid(None,99,0), (True,False,False))
            sleep.assert_called_once_with(2)

    def test_manual_base_controls_close_threshold_not_actual_profit(self):
        bot = self.bot
        bot.LOT_PER_LEVEL,bot.GRID_STEP = 1,10
        cases = ((69926,1,72316,'S'),(72926,1,72316,None),
                 (0,4,72336,'S'),(72926,-1,72316,'B'),
                 (69926,-1,72316,None),(0,0,72316,None))
        for base,qty,price,side in cases:
            with self.subTest(base=base,qty=qty):
                bot.base_price,bot.prev_position = base,qty
                client = Mock()
                quote = price-1 if side=='S' else price+1
                client.GetParamEx.return_value = {'data':{'result':'1','param_value':str(quote)}}
                with patch.object(bot,'find_active_orders',return_value=[]), \
                     patch.object(bot,'get_price_step',return_value=1), \
                     patch.object(bot,'send_limit_order') as send:
                    result = bot.check_base_price_by_grid(client,price,qty)
                self.assertEqual(result,(False,side is not None,False))
                if side is None:
                    send.assert_not_called()
                else:
                    send.assert_called_once_with(client,price,abs(qty),side)
                self.assertEqual(bot.base_price,base)

    def run_main_loop(self, *, connections, sessions, positions, prices, stop_after,
                      saved_base=100, grid_step=20, keep_orders=False, times=None):
        tree = ast.parse((ROOT/'main.py').read_text(encoding='utf-8-sig'))
        scope = dict(vars(self.bot))
        functions = [n for n in tree.body if isinstance(n,ast.FunctionDef)]
        exec(compile(ast.Module(body=functions,type_ignores=[]),'<v2 functions>','exec'),scope)
        def sequence(values):
            iterator, last = iter(values), values[-1]
            return lambda *args: next(iterator,last)
        delays, grids = [], []
        def sleep(delay):
            delays.append(delay)
            if len(delays) >= stop_after:
                raise KeyboardInterrupt
        client = Mock(connected=True)
        client.RegisterBot.return_value = {'data': {
            'tick':{'param_value':'1'}, 'foreign_active_orders':0,'unresolved_transactions':0}}
        client.GetInfoParam.return_value = {'data':'12:00:00'}
        client.GetOrders.return_value = {'data': []}
        scope.update(QuikPy=lambda **kw: client, time=types.SimpleNamespace(sleep=sleep),
            GRID_STEP=grid_step, LOT_PER_LEVEL=1, LEVELS=1, POLL_MS=1,
            check_time=sequence(times or [True]), check_quik_connection=sequence(connections),
            check_session_status=sequence(sessions), get_current_position=sequence(positions),
            get_current_price=sequence(prices), read_from_file=lambda:saved_base,
            get_last_trade_price=lambda qp:99, write_in_file=Mock(),
            find_active_orders=lambda qp:[1] if keep_orders and grids else [], wait_until_tradable=lambda qp:None,
            set_grid=lambda qp,price:grids.append(price), cancel_all_orders=Mock(),
            send_limit_order=Mock())
        main = next(n for n in tree.body if isinstance(n,ast.If) and
                    ast.unparse(n.test) == "__name__ == '__main__'")
        with contextlib.redirect_stdout(io.StringIO()), patch('gateway_control.ensure_gateway'):
            exec(compile(ast.Module(body=main.body,type_ignores=[]),'<actual v2 loop>','exec'),scope)
        return scope,grids,delays

    def test_clearing_does_not_bypass_price_return_in_actual_loop(self):
        scope,grids,delays = self.run_main_loop(connections=[True],
            sessions=[True,False,True,True], positions=[2], prices=[100,100,50],stop_after=5)
        self.assertEqual(grids,[100])
        scope['send_limit_order'].assert_not_called()
        self.assertIn(scope['STABILIZATION_DELAY_SEC'],delays)

    def test_flat_startup_gap_waits_for_saved_base_without_orders(self):
        for price in (28844,28680):
            with self.subTest(price=price):
                scope,grids,_ = self.run_main_loop(connections=[True],sessions=[True],
                    positions=[0],prices=[price],stop_after=5,saved_base=28762,grid_step=10)
                self.assertEqual(grids,[])
                self.assertEqual(scope['base_price'],28762)
                scope['write_in_file'].assert_not_called()
                scope['send_limit_order'].assert_not_called()

    def test_flat_startup_gap_builds_only_after_price_returns(self):
        scope,grids,_ = self.run_main_loop(connections=[True],sessions=[True],
            positions=[0],prices=[28844,28844,28844,28772],stop_after=6,
            saved_base=28762,grid_step=10,keep_orders=True)
        self.assertEqual(grids,[28762])
        self.assertEqual(scope['base_price'],28762)

    def test_flat_startup_without_saved_state_uses_current_price(self):
        scope,grids,_ = self.run_main_loop(connections=[True],sessions=[True],
            positions=[0],prices=[28844],stop_after=4,saved_base=None,grid_step=10,keep_orders=True)
        self.assertEqual(grids,[28844])
        self.assertEqual(scope['base_price'],28844)

    def test_flat_empty_grid_is_not_rebuilt_outside_channel(self):
        scope,grids,_ = self.run_main_loop(connections=[True],sessions=[True],
            positions=[0],prices=[100,100,182],stop_after=6,grid_step=10)
        self.assertEqual(grids,[100])
        self.assertEqual(scope['base_price'],100)

    def test_flat_reconnect_gap_does_not_rebuild_old_grid(self):
        scope,grids,delays = self.run_main_loop(connections=[False,True,True],sessions=[True],
            positions=[0],prices=[100,100,182],stop_after=7,grid_step=10)
        self.assertEqual(grids,[100])
        self.assertEqual(scope['base_price'],100)
        self.assertIn(scope['RECONNECT_DELAY_SEC'],delays)

    def test_flat_clearing_gap_does_not_rebuild_old_grid(self):
        scope,grids,delays = self.run_main_loop(connections=[True],sessions=[True,True,False,True,True],
            positions=[0],prices=[100,100,182],stop_after=8,grid_step=10)
        self.assertEqual(grids,[100])
        self.assertEqual(scope['base_price'],100)
        self.assertIn(scope['STABILIZATION_DELAY_SEC'],delays)

    def test_position_close_during_disconnect_remembers_startup_position(self):
        scope,grids,delays = self.run_main_loop(connections=[False,True,True],
            sessions=[True,False], positions=[2,2,0], prices=[100],stop_after=5)
        self.assertEqual(scope['base_price'],99)
        self.assertEqual(scope['position'],0)
        self.assertEqual(grids,[100])  # Session is still closed after reconnect.
        self.assertIn(scope['RECONNECT_DELAY_SEC'],delays)

    def test_order_reconciliation_every_ten_active_cycles(self):
        scope,grids,delays = self.run_main_loop(connections=[True], sessions=[True],
            positions=[2],prices=[100],stop_after=30)
        self.assertEqual(scope['qp'].RefreshOrders.call_count,2)
        self.assertEqual(delays[:2],[3,2])
        self.assertEqual(delays[2:],[1]*28)

    def test_each_pause_reconciles_before_periodic_ten_cycle_check(self):
        cases = (
            dict(connections=[True],sessions=[True],times=[True,True,True,False,False,True],
                 delay=self.bot.STABILIZATION_DELAY_SEC),
            dict(connections=[True],sessions=[True,True,False,True],times=[True],
                 delay=self.bot.STABILIZATION_DELAY_SEC),
            dict(connections=[False,True],sessions=[True],times=[True],
                 delay=self.bot.RECONNECT_DELAY_SEC),
        )
        for case in cases:
            with self.subTest(case=case):
                scope,_,delays=self.run_main_loop(connections=case['connections'],
                    sessions=case['sessions'],times=case['times'],positions=[0],prices=[100],
                    stop_after=9,keep_orders=True)
                self.assertIn(case['delay'],delays)
                self.assertGreaterEqual(scope['qp'].RefreshOrders.call_count,1)


class ConnectionAuditTests(unittest.TestCase):
    setUp = fixtures.GatewayTests.setUp
    cleanup = fixtures.GatewayTests.cleanup
    connect = fixtures.GatewayTests.connect
    transaction = fixtures.GatewayTests.transaction
    create_order = fixtures.GatewayTests.create_order

    def test_disconnected_profile_retires_after_last_order_closes(self):
        client = self.connect(1)
        order = self.create_order(client)
        client.CloseConnectionAndThread()
        wait_until(lambda: 'bot_1' in self.engine.retire_checks)
        self.assertIn('bot_1',self.engine.profiles)
        order['flags'] = 0
        self.broker.orders[order['order_num']] = order
        self.engine.on_event({'cmd':'OnOrder','data':order})
        self.engine.last_snapshot['bot_1'] = 0
        wait_until(lambda: 'bot_1' not in self.engine.profiles)
        self.assertNotIn('bot_1',self.engine.retire_checks)
    def test_repeated_connections_release_session_objects(self):
        import weakref
        import gc
        references = []
        for _ in range(15):
            client = self.connect(1)
            session = self.server.sessions[1]
            references.append(weakref.ref(session))
            client.CloseConnectionAndThread()
            session.request_thread.join(timeout=2)
            if session.callback_thread:
                session.callback_thread.join(timeout=2)
            wait_until(lambda: not self.server.sessions and not self.engine.profiles)
            del session
        gc.collect()
        self.assertTrue(all(reference() is None for reference in references))
        self.assertEqual(len(self.server.sockets), 6)
