"""Exercise premarket startup and fault waits without a real broker connection."""
import contextlib
import ast
import io
from pathlib import Path
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import Mock,patch

import run_bot
from lifecycle import WaitStatus
from tests import test_v2 as fixtures, test_audit as audit_fixtures
ROOT, TEST_ROOT = fixtures.ROOT, fixtures.TEST_ROOT
from transport import RemoteError, TransactionUncertain


def ready():
    return {'data':{'tick':{'param_value':'1'},'foreign_active_orders':0,'unresolved_transactions':0}}


class LifecycleTests(unittest.TestCase):
    setUp=fixtures.BotTests.setUp

    def test_missing_instrument_then_available_retries_only_reads(self):
        first,second=Mock(),Mock()
        first.RegisterBot.side_effect=RemoteError({'error':'Instrument not found in QUIK'})
        second.RegisterBot.return_value=ready()
        with patch.object(self.bot,'QuikPy',side_effect=[first,second]), \
             patch('gateway_control.ensure_gateway'),patch.object(self.bot.time,'sleep') as sleep:
            qp,_=self.bot.connect_and_register()
        self.assertIs(qp,second)
        first.CloseConnectionAndThread.assert_called_once()
        sleep.assert_called_once_with(self.bot.RECONNECT_DELAY_SEC)
        first.SendTransaction.assert_not_called();second.SendTransaction.assert_not_called()

    def test_gateway_start_failure_does_not_exit(self):
        client=Mock();client.RegisterBot.return_value=ready()
        with patch('gateway_control.ensure_gateway',side_effect=[RuntimeError('gateway not ready'),None]), \
             patch.object(self.bot,'QuikPy',return_value=client) as factory,patch.object(self.bot.time,'sleep') as sleep:
            qp,_=self.bot.connect_and_register()
        self.assertIs(qp,client);factory.assert_called_once();sleep.assert_called_once()

    def test_pending_startup_waits_until_broker_outcome_is_known(self):
        first,second=Mock(),Mock()
        blocked=ready();blocked['data']['unresolved_transactions']=1
        first.RegisterBot.return_value=blocked;second.RegisterBot.return_value=ready()
        with patch('gateway_control.ensure_gateway'),patch.object(self.bot,'QuikPy',side_effect=[first,second]), \
             patch.object(self.bot.time,'sleep'):
            qp,_=self.bot.connect_and_register()
        self.assertIs(qp,second)
        first.SendTransaction.assert_not_called();second.SendTransaction.assert_not_called()

    def test_runtime_uncertain_outcome_never_replays_transaction(self):
        qp=Mock(connected=True)
        blocked=ready();blocked['data']['unresolved_transactions']=1
        qp.RegisterBot.side_effect=[blocked,blocked,ready()]
        with patch('gateway_control.ensure_gateway'),patch.object(self.bot.time,'sleep') as sleep, \
             patch.object(self.bot,'check_quik_connection',return_value=True), \
             patch.object(self.bot,'check_time',return_value=True),patch.object(self.bot,'check_session_status',return_value=True), \
             patch.object(self.bot,'get_current_position',return_value=14),patch.object(self.bot,'get_current_price',return_value=800):
            self.bot.wait_for_runtime_ready(qp,TransactionUncertain({'error':'unknown'}))
        self.assertEqual(qp.RegisterBot.call_count,3)
        self.assertEqual(sleep.call_count,3)
        qp.SendTransaction.assert_not_called();qp.WaitTransaction.assert_not_called()
        qp.RefreshOrders.assert_called_once()

    def test_closed_session_in_runtime_waits_until_open(self):
        qp=Mock(connected=True);qp.RegisterBot.return_value=ready()
        with patch('gateway_control.ensure_gateway'),patch.object(self.bot.time,'sleep') as sleep, \
             patch.object(self.bot,'check_quik_connection',return_value=True), \
             patch.object(self.bot,'check_time',return_value=True),patch.object(self.bot,'check_session_status',side_effect=[False,False,True]), \
             patch.object(self.bot,'get_current_position',return_value=0),patch.object(self.bot,'get_current_price',return_value=800):
            self.bot.wait_for_runtime_ready(qp,RemoteError({'error':'market closed'}))
        self.assertEqual(sleep.call_count,3);qp.SendTransaction.assert_not_called()

    def test_runtime_wait_rechecks_the_missing_grid_order_before_resuming(self):
        qp=Mock(connected=True);qp.RegisterBot.return_value=ready()
        number='2024401013557188673'
        self.bot.grid_order_numbers.add(number)
        qp.GetOrders.return_value={'data':[]}
        qp.GetTrades.return_value={'data':[]}
        qp.GetConfirmedUnfilledCancellation.return_value={'data':None}
        valid=dict(order_num=int(number),class_code=self.bot.CLASS,sec_code=self.bot.SECCODE,
                   account=self.bot.ACCOUNT,flags=28,balance=0,price=100)
        qp.GetOrderByClassNumber.side_effect=[{'data':None},{'data':None},{'data':valid},{'data':valid}]
        reason=TransactionUncertain({'error':f'Cannot verify grid order {number}; QUIK не вернул заявку; replacement stopped'})
        output=io.StringIO()
        with patch('gateway_control.ensure_gateway'),patch.object(self.bot.time,'sleep') as sleep, \
             patch.object(self.bot,'check_quik_connection',return_value=True), \
             patch.object(self.bot,'check_time',return_value=True),patch.object(self.bot,'check_session_status',return_value=True), \
             patch.object(self.bot,'get_current_position',return_value=0),patch.object(self.bot,'get_current_price',return_value=100), \
             contextlib.redirect_stdout(output):
            self.bot.wait_for_runtime_ready(qp,reason)
        self.assertEqual(sleep.call_count,3)
        self.assertEqual(qp.GetOrderByClassNumber.call_count,3)
        self.assertEqual(output.getvalue().count('Работа приостановлена:'),1)
        self.assertEqual(output.getvalue().count('возобновляем работу'),1)
        self.assertEqual(self.bot.grid_order_numbers,{number})
        qp.SendTransaction.assert_not_called()
        self.assertEqual(self.bot.completed_grid_level(qp)['order_num'],int(number))

    def test_runtime_wait_never_accepts_wrong_order_identity(self):
        qp=Mock(connected=True);qp.RegisterBot.return_value=ready()
        self.bot.grid_order_numbers.add('101')
        qp.GetOrders.return_value={'data':[]}
        qp.GetOrderByClassNumber.return_value={'data':dict(order_num=101,class_code=self.bot.CLASS,
            sec_code=self.bot.SECCODE,account='OTHER',flags=28,balance=0)}
        with patch('gateway_control.ensure_gateway'),patch.object(self.bot.time,'sleep',side_effect=[None,None,KeyboardInterrupt]), \
             patch.object(self.bot,'check_quik_connection',return_value=True), \
             patch.object(self.bot,'check_time',return_value=True),patch.object(self.bot,'check_session_status',return_value=True), \
             patch.object(self.bot,'get_current_position',return_value=0),patch.object(self.bot,'get_current_price',return_value=100), \
             contextlib.redirect_stdout(io.StringIO()):
            with self.assertRaises(KeyboardInterrupt):
                self.bot.wait_for_runtime_ready(qp,RuntimeError('unknown order'))
        self.assertEqual(self.bot.grid_order_numbers,{'101'})
        qp.SendTransaction.assert_not_called()

    def test_persistent_error_stays_alive_until_user_interrupts(self):
        qp=Mock(connected=True);qp.RegisterBot.side_effect=RemoteError({'error':'still unavailable'})
        with patch('gateway_control.ensure_gateway'),patch.object(self.bot.time,'sleep',side_effect=[None,None,KeyboardInterrupt]):
            with self.assertRaises(KeyboardInterrupt):self.bot.wait_for_runtime_ready(qp,RuntimeError('unavailable'))
        self.assertEqual(qp.RegisterBot.call_count,2);qp.SendTransaction.assert_not_called()

    def test_early_data_failure_closes_socket_without_cancelling_or_repeating_orders(self):
        tree=ast.parse((ROOT/'main.py').read_text(encoding='utf-8'))
        main=next(n for n in tree.body if isinstance(n,ast.If) and ast.unparse(n.test)=="__name__ == '__main__'")
        qp=Mock(connected=True)
        qp.GetInfoParam.return_value={'data':'12:00:00'}
        scope=dict(vars(self.bot))
        failure=RemoteError({'error':'LAST temporarily unavailable'})
        cancel=Mock()
        scope.update(connect_and_register=lambda:(qp,ready()['data']),
                     check_time=lambda qp:True, check_session_status=lambda qp:True,
                     get_current_price=Mock(side_effect=failure), cancel_all_orders=cancel,
                     time=SimpleNamespace(sleep=Mock()))
        with contextlib.redirect_stdout(io.StringIO()),self.assertRaises(RemoteError):
            exec(compile(ast.Module(body=main.body,type_ignores=[]),'<main lifecycle>','exec'),scope)
        cancel.assert_not_called()
        qp.SendTransaction.assert_not_called()
        qp.CloseConnectionAndThread.assert_called_once()

    def test_fault_leaves_grid_incomplete_until_reconciled(self):
        qp=Mock()
        self.bot.LEVELS=1
        with patch.object(self.bot,'wait_until_tradable'),patch.object(self.bot,'cancel_all_orders'), \
             patch.object(self.bot,'get_current_position',return_value=0), \
             patch.object(self.bot,'send_limit_order',side_effect=[{'order_num':101},TransactionUncertain({'error':'unknown'})]):
            with self.assertRaises(TransactionUncertain):self.bot.set_grid(qp,100)
        self.assertTrue(self.bot.grid_build_in_progress)
        self.assertEqual(self.bot.grid_order_numbers,{'101'})


class LauncherSupervisorTests(unittest.TestCase):
    def test_early_initialization_error_retries_and_normal_return_does_not_restart(self):
        with tempfile.TemporaryDirectory(dir=TEST_ROOT) as folder,patch.object(run_bot,'ROOT',Path(folder)), \
             patch.object(run_bot.runpy,'run_path',side_effect=[RemoteError({'error':'not ready'}),{}]) as run, \
             patch.object(run_bot.time,'sleep') as sleep, contextlib.redirect_stdout(io.StringIO()):
            run_bot.execute_bot(SimpleNamespace(BOT_ID='supervisor_test',RECONNECT_DELAY_SEC=20))
        self.assertEqual(run.call_count,2);sleep.assert_called_once_with(20)

    def test_user_stop_never_restarts_main(self):
        with tempfile.TemporaryDirectory(dir=TEST_ROOT) as folder,patch.object(run_bot,'ROOT',Path(folder)), \
             patch.object(run_bot.runpy,'run_path',side_effect=KeyboardInterrupt) as run, \
             patch.object(run_bot.time,'sleep') as sleep,contextlib.redirect_stdout(io.StringIO()):
            run_bot.execute_bot(SimpleNamespace(BOT_ID='supervisor_stop',RECONNECT_DELAY_SEC=20))
        run.assert_called_once();sleep.assert_not_called()

    def test_missing_settings_can_be_fixed_without_relaunch(self):
        cfg=SimpleNamespace()
        with patch.object(run_bot,'load_settings',side_effect=[FileNotFoundError('settings_1.py'),cfg]), \
             patch.object(run_bot.time,'sleep') as sleep:
            self.assertIs(run_bot.wait_for_settings(Path('settings_1.py')),cfg)
        sleep.assert_called_once_with(20)

    def test_identical_wait_messages_are_printed_once(self):
        log=WaitStatus();output=io.StringIO()
        with contextlib.redirect_stdout(output):
            for _ in range(1000):log.show('Waiting')
        self.assertEqual(output.getvalue().count('Waiting'),1)

    def test_changed_wait_reason_is_visible_and_clear_starts_a_new_wait(self):
        log=WaitStatus();output=io.StringIO()
        with contextlib.redirect_stdout(output):
            log.show('No connection');log.show('No connection')
            log.show('Market closed');log.show('Market closed')
            log.clear()
            log.show('Market closed')
        self.assertEqual(output.getvalue().count('No connection'),1)
        self.assertEqual(output.getvalue().count('Market closed'),2)


class PremarketLoopTests(unittest.TestCase):
    setUp=fixtures.BotTests.setUp
    run_main_loop=audit_fixtures.StrategyAuditTests.run_main_loop

    def test_start_before_hours_waits_then_builds_grid_at_open(self):
        scope,grids,delays=self.run_main_loop(connections=[True],sessions=[False,True],
            positions=[0],prices=[100],times=[False,False,True],stop_after=8,keep_orders=True)
        self.assertEqual(grids,[100])
        self.assertIn(scope['STABILIZATION_DELAY_SEC'],delays)

    def test_closed_market_never_builds_grid(self):
        scope,grids,_=self.run_main_loop(connections=[True],sessions=[False],
            positions=[0],prices=[100],times=[False],stop_after=6,keep_orders=True)
        self.assertEqual(grids,[]);scope['send_limit_order'].assert_not_called()

if __name__=='__main__':unittest.main()
