"""Automatic old-unknown reconciliation against a fake QUIK; no live trading."""
import copy
import hashlib
import json
import sqlite3
import threading
import time
import unittest
from contextlib import closing
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock

from gateway import Engine
from tests import test_v2 as fixtures
from transport import RemoteError


class PeriodicReconciliationTests(unittest.TestCase):
    def test_gateway_waits_one_minute_before_periodic_table_scan(self):
        profile = dict(bot_id='bot_1')
        now = time.monotonic()
        engine = SimpleNamespace(last_log_cleanup=now, last_metrics=now, client=True,
                                 last_snapshot={'bot_1': now - 30}, profile_lock=threading.RLock(),
                                 retire_checks={}, _profiles=lambda: {'bot_1': profile},
                                 _connect=Mock(), refresh=Mock())
        Engine._maintenance(engine)
        engine.refresh.assert_not_called()
        engine.last_snapshot['bot_1'] = time.monotonic() - 61
        Engine._maintenance(engine)
        engine.refresh.assert_called_once_with(profile)


class AutoReconciliationTests(unittest.TestCase):
    setUp = fixtures.GatewayTests.setUp
    cleanup = fixtures.GatewayTests.cleanup
    connect = fixtures.GatewayTests.connect

    def seed(self, *, bot='bot_1', age=2*86400, action='NEW_ORDER', request=None):
        p=fixtures.profile(1 if bot=='bot_1' else 2)
        if request is None:
            request=dict(ACTION=action,CLASSCODE=p['class_code'],SECCODE=p['sec_code'],
                         ACCOUNT=p['account'],OPERATION='B',PRICE='99',QUANTITY='1',TYPE='L')
        gid=self.engine.registry.allocate(bot,'45',action,session='old',request=request)
        self.engine.registry.mark(gid,'uncertain')
        with self.engine.registry.lock:
            self.engine.registry.db.execute('UPDATE transactions SET created=? WHERE gid=?',(time.time()-age,gid))
            self.engine.registry.db.commit()
        return gid,p

    def register(self, p=None):
        p=p or fixtures.profile()
        client=self.connect(1,register=False)
        result=client.RegisterBot(p['bot_id'],p['class_code'],p['sec_code'],p['base_asset_code'],p['account'])['data']
        return client,result

    def test_old_unknown_resolves_after_read_only_reconciliation_with_audit_and_backup(self):
        gid,p=self.seed()
        other,_=self.seed(bot='bot_2')
        client,result=self.register(p)
        self.assertEqual(result['reconciled_old_unknown'],[gid])
        self.assertEqual(result['unresolved_transactions'],0)
        reg=self.engine.registry
        self.assertEqual(reg.transaction(gid)['state'],'closed_unknown')
        self.assertEqual(reg.transaction(other)['state'],'uncertain')
        audit=reg.db.execute('SELECT * FROM transaction_resolutions WHERE gid=?',(gid,)).fetchone()
        evidence=reg.db.execute('SELECT evidence_json FROM transaction_auto_snapshots WHERE gid=?',(gid,)).fetchone()
        self.assertEqual(audit['operator'],'automatic')
        self.assertEqual(json.loads(audit['before_json'])['state'],'uncertain')
        self.assertEqual(json.loads(evidence['evidence_json'])['outcome'],'unknown')
        with closing(sqlite3.connect(audit['backup'])) as saved:
            self.assertEqual(saved.execute('SELECT state FROM transactions WHERE gid=?',(gid,)).fetchone()[0],'uncertain')
        self.assertEqual(hashlib.sha256(Path(audit['backup']).read_bytes()).hexdigest(),audit['backup_sha256'])
        self.assertEqual(self.broker.transactions,[])
        with self.assertRaisesRegex(RemoteError,'closed'):
            reg.wait('bot_1','45',0,session='old')

    def test_fresh_unknown_and_unknown_cancellation_remain_blocked(self):
        for kwargs in (dict(age=10),dict(action='KILL_ORDER')):
            with self.subTest(kwargs=kwargs):
                gid,p=self.seed(**kwargs)
                _,result=self.register(p)
                self.assertEqual(result['unresolved_transactions'],1)
                self.assertEqual(self.engine.registry.transaction(gid)['state'],'uncertain')
                self.assertEqual(result['reconciled_old_unknown'],[])
                self.clients[-1].CloseConnectionAndThread()
                with self.engine.registry.lock:
                    self.engine.registry.db.execute('DELETE FROM transactions WHERE gid=?',(gid,))
                    self.engine.registry.db.commit()

    def test_unknown_from_same_day_resolves_after_one_minute(self):
        gid, p = self.seed(age=61)
        _, result = self.register(p)
        self.assertEqual(result['reconciled_old_unknown'], [gid])
        self.assertEqual(self.engine.registry.transaction(gid)['state'], 'closed_unknown')
        self.assertEqual(self.broker.transactions, [])

    def test_matching_active_quik_order_is_confirmed_not_closed_as_unknown(self):
        gid,p=self.seed()
        order=dict(order_num=123,trans_id=gid,class_code=p['class_code'],sec_code=p['sec_code'],
                   account=p['account'],flags=1,qty=1,balance=1,price=99)
        self.broker.orders[123]=order
        client,result=self.register(p)
        self.assertEqual(result['unresolved_transactions'],0)
        self.assertEqual(result['reconciled_old_unknown'],[])
        self.assertEqual(self.engine.registry.transaction(gid)['state'],'accepted')
        self.assertEqual(client.GetOrders(p['class_code'],p['sec_code'])['data'][0]['order_num'],123)
        self.assertEqual(self.broker.transactions,[])

    def test_known_active_order_is_preserved_during_old_unknown_reconciliation(self):
        gid,p=self.seed()
        known=self.engine.registry.allocate('bot_1','60','NEW_ORDER')
        self.engine.registry.on_reply(dict(trans_id=known,status=3,order_num=555))
        order=dict(order_num=555,trans_id=known,class_code=p['class_code'],sec_code=p['sec_code'],
                   account=p['account'],flags=1,qty=1,balance=1,price=101)
        self.engine.registry.on_order(order,{'bot_1':p})
        self.broker.orders[555]=copy.deepcopy(order)
        client,result=self.register(p)
        self.assertEqual(result['reconciled_old_unknown'],[gid])
        self.assertEqual([o['order_num'] for o in client.GetOrders(p['class_code'],p['sec_code'])['data']],[555])
        self.assertEqual(self.broker.transactions,[])

    def test_changed_instrument_still_checks_original_instrument_before_resolution(self):
        gid,_=self.seed()
        p=fixtures.profile(1);p.update(sec_code='SPREAD_3',base_asset_code='LEG_3')
        _,result=self.register(p)
        self.assertEqual(result['reconciled_old_unknown'],[gid])
        evidence=self.engine.registry.db.execute(
            'SELECT evidence_json FROM transaction_auto_snapshots WHERE gid=?',(gid,)).fetchone()[0]
        self.assertEqual(json.loads(evidence)['profile']['sec_code'],'SPREAD_1')
        self.assertEqual(self.broker.transactions,[])

    def test_profile_switch_reconciles_old_unknown_before_switching(self):
        client=self.connect(1)
        gid,_=self.seed()
        p=fixtures.profile(1);p.update(sec_code='SPREAD_3',base_asset_code='LEG_3')
        result=client.RegisterBot(p['bot_id'],p['class_code'],p['sec_code'],p['base_asset_code'],p['account'])['data']
        self.assertEqual(result['reconciled_old_unknown'],[gid])
        self.assertEqual(self.engine.profiles['bot_1']['sec_code'],'SPREAD_3')
        self.assertEqual(self.broker.transactions,[])

    def test_two_bots_reconcile_independently(self):
        gid1,p1=self.seed()
        gid2,p2=self.seed(bot='bot_2')
        _,result1=self.register(p1)
        self.assertEqual(result1['reconciled_old_unknown'],[gid1])
        self.assertEqual(self.engine.registry.transaction(gid2)['state'],'uncertain')
        second=self.connect(2,register=False)
        result2=second.RegisterBot(p2['bot_id'],p2['class_code'],p2['sec_code'],p2['base_asset_code'],p2['account'])['data']
        self.assertEqual(result2['reconciled_old_unknown'],[gid2])
        self.assertEqual(self.engine.registry.transaction(gid1)['state'],'closed_unknown')
        self.assertEqual(self.engine.registry.transaction(gid2)['state'],'closed_unknown')
        self.assertEqual(self.broker.transactions,[])

    def test_saved_order_evidence_blocks_automatic_unknown_resolution(self):
        gid,p=self.seed()
        saved=dict(order_num=777,trans_id=gid,class_code=p['class_code'],sec_code=p['sec_code'],
                   account=p['account'],flags=2,qty=1,balance=1,price=99)
        with self.engine.registry.lock:
            self.engine.registry.db.execute('INSERT INTO orders VALUES (?,?,?,?,?)',
                ('777','bot_1',json.dumps(saved),0,1))
            self.engine.registry.db.commit()
        _,result=self.register(p)
        self.assertEqual(result['unresolved_transactions'],1)
        self.assertEqual(self.engine.registry.transaction(gid)['state'],'uncertain')

    def test_missing_original_request_does_not_auto_resolve(self):
        gid,p=self.seed()
        with self.engine.registry.lock:
            self.engine.registry.db.execute('UPDATE transactions SET request=NULL WHERE gid=?',(gid,))
            self.engine.registry.db.commit()
        _,result=self.register(p)
        self.assertEqual(result['unresolved_transactions'],1)
        self.assertEqual(self.engine.registry.transaction(gid)['state'],'uncertain')

    def test_malformed_original_request_cannot_be_auto_resolved(self):
        gid,p=self.seed()
        reg=self.engine.registry
        original=json.loads(reg.transaction(gid)['request'])
        self.engine._connect()
        for change in (dict(QUANTITY='0'),dict(PRICE='nan'),dict(TYPE='M'),dict(OPERATION='X')):
            with self.subTest(change=change):
                with reg.lock:
                    reg.db.execute('UPDATE transactions SET request=? WHERE gid=?',
                                   (json.dumps(dict(original,**change)),gid))
                    reg.db.commit()
                self.assertEqual(self.engine.reconcile_old_unknown_orders(p),[])
                self.assertEqual(reg.transaction(gid)['state'],'uncertain')
        self.assertFalse((reg.path.parent/'recovery').exists())

    def test_foreign_active_order_blocks_automatic_resolution(self):
        gid,p=self.seed()
        self.broker.orders[123]=dict(order_num=123,trans_id=7,class_code=p['class_code'],
            sec_code=p['sec_code'],account=p['account'],flags=1,qty=1,balance=1,price=99)
        _,result=self.register(p)
        self.assertEqual(result['unresolved_transactions'],1)
        self.assertEqual(result['foreign_active_orders'],1)
        self.assertEqual(self.engine.registry.transaction(gid)['state'],'uncertain')
        self.assertEqual(self.broker.transactions,[])

    def test_no_broker_connection_or_ambiguous_position_prevents_resolution(self):
        for bad in ('disconnected','duplicate'):
            with self.subTest(bad=bad):
                gid,p=self.seed()
                original=self.broker.call
                def response(request,client,handler):
                    if bad=='disconnected' and request['cmd']=='isConnected': return 0
                    if bad=='duplicate' and request['cmd']=='getFuturesClientHoldings':
                        return [dict(sec_code='LEG_1',trdaccid='DEMO',totalnet=1)]*2
                    return original(request,client,handler)
                self.broker.call=response
                _,result=self.register(p)
                self.assertEqual(result['unresolved_transactions'],1)
                self.assertEqual(self.engine.registry.transaction(gid)['state'],'uncertain')
                self.clients[-1].CloseConnectionAndThread()
                self.broker.call=original
                with self.engine.registry.lock:
                    self.engine.registry.db.execute('DELETE FROM transactions WHERE gid=?',(gid,))
                    self.engine.registry.db.commit()

    def test_late_broker_reply_preserves_automatic_audit(self):
        gid,p=self.seed();self.register(p)
        self.engine.registry.on_reply(dict(trans_id=gid,status=4,order_num=0,result_msg='actual'))
        self.assertEqual(self.engine.registry.transaction(gid)['state'],'rejected')
        self.assertEqual(self.engine.registry.db.execute(
            'SELECT COUNT(*) FROM transaction_auto_snapshots WHERE gid=?',(gid,)).fetchone()[0],1)

    def test_late_acceptance_blocks_new_orders_until_order_is_verified(self):
        gid,p=self.seed();client,_=self.register(p)
        self.engine.registry.on_reply(dict(trans_id=gid,status=3,order_num=999))
        request=dict(TRANS_ID='50',ACTION='NEW_ORDER',TYPE='L',OPERATION='B',
                     CLASSCODE=p['class_code'],SECCODE=p['sec_code'],ACCOUNT=p['account'],PRICE='99',QUANTITY='1')
        with self.assertRaises(RemoteError):client.SendTransaction(request)
        self.assertEqual(self.broker.transactions,[])
        self.assertEqual(self.engine.registry.db.execute(
            'SELECT COUNT(*) FROM transaction_auto_snapshots WHERE gid=?',(gid,)).fetchone()[0],1)

    def test_event_during_snapshot_defers_resolution(self):
        gid,p=self.seed();reg=self.engine.registry
        original=dict(reg.transaction(gid));watermark=reg.sequence
        reg.on_order(dict(order_num=555,trans_id=77,class_code=p['class_code'],sec_code=p['sec_code'],
                          account=p['account'],flags=1),{'bot_1':p})
        self.assertFalse(reg.close_old_unknown_after_snapshot(original,{},watermark))
        self.assertEqual(reg.transaction(gid)['state'],'uncertain')
        self.assertFalse((reg.path.parent/'recovery').exists())

    def test_trade_callback_during_active_order_and_position_snapshot_defers_resolution(self):
        gid,p=self.seed();original=self.broker.call
        active_calls=0
        def response(request,client,handler):
            nonlocal active_calls
            if request['cmd']=='get_active_orders':active_calls+=1
            if request['cmd']=='getFuturesClientHoldings' and active_calls>=2:
                self.engine.on_event(dict(cmd='OnTrade',data=dict(order_num=999,trade_num=777,
                    class_code=p['class_code'],sec_code=p['sec_code'],account=p['account'],qty=1,price=99)))
            return original(request,client,handler)
        self.broker.call=response
        _,result=self.register(p)
        self.assertEqual(result['unresolved_transactions'],1)
        self.assertEqual(self.engine.registry.transaction(gid)['state'],'uncertain')
        self.assertFalse((self.engine.registry.path.parent/'recovery').exists())
        self.assertEqual(self.broker.transactions,[])


if __name__=='__main__': unittest.main()
