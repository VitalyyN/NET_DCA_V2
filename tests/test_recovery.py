"""Offline recovery never sends transactions or edits another bot's records."""
from contextlib import closing
from datetime import datetime, timezone, timedelta
import hashlib
import json
from pathlib import Path
import sqlite3
import tempfile
import unittest
from unittest.mock import Mock
from types import SimpleNamespace
from diagnose_bot import instrument_status

from gateway_control import FileLock
from recover_transaction import close_stale, inspect
from registry import Registry
from transport import RemoteError

ROOT = Path(__file__).resolve().parents[1]
NOW = datetime(2026,10,24,18,tzinfo=timezone(timedelta(hours=3))).timestamp()

class RecoveryTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(dir=ROOT/'runtime/tests')
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        (self.root/'runtime').mkdir()
        self.path = self.root/'runtime/gateway.sqlite3'
        reg = Registry(self.path)
        self.gid = reg.allocate('bot_1','3','NEW_ORDER',session='session-one')
        reg.mark(self.gid,'uncertain')
        self.other = reg.allocate('bot_2','7','NEW_ORDER',session='session-two')
        reg.mark(self.other,'accepted',dict(status=3, order_num=123))
        reg.db.execute('UPDATE transactions SET created=?',(NOW-2*86400,))
        reg.db.commit()
        reg.close()

    def read(self, gid):
        with closing(sqlite3.connect(self.path)) as db:
            db.row_factory=sqlite3.Row
            return dict(db.execute('SELECT * FROM transactions WHERE gid=?',(gid,)).fetchone())

    def close(self, **kw):
        return close_stale(self.root,kw.pop('bot','bot_1'),self.gid,
            kw.pop('reason','Reviewed current orders and position; broker outcome remains unknown'),
            acknowledge=kw.pop('acknowledge',True),now=kw.pop('now',NOW),**kw)

    def update(self, query, params=()):
        with closing(sqlite3.connect(self.path)) as db:
            db.execute(query,params);db.commit()

    def test_preview_is_read_only(self):
        before=hashlib.sha256(self.path.read_bytes()).hexdigest()
        result=inspect(self.path,'bot_1',self.gid)
        self.assertEqual(result['state'],'uncertain')
        self.assertEqual(result['active_orders_in_registry'],0)
        self.assertEqual(hashlib.sha256(self.path.read_bytes()).hexdigest(),before)
        self.assertFalse((self.root/'runtime/recovery').exists())

    def test_closes_only_selected_record_and_preserves_audit_and_backup(self):
        before=self.read(self.gid);other=self.read(self.other)
        result=self.close()
        after=self.read(self.gid)
        self.assertEqual(after,dict(before,state='closed_unknown'))
        self.assertEqual(self.read(self.other),other)
        backup=Path(result['backup'])
        with closing(sqlite3.connect(backup)) as db:
            self.assertEqual(db.execute('SELECT state FROM transactions WHERE gid=?',(self.gid,)).fetchone()[0],'uncertain')
        with closing(sqlite3.connect(self.path)) as db:
            audit=db.execute('SELECT before_json,backup_sha256,reason FROM transaction_resolutions WHERE gid=?',(self.gid,)).fetchone()
            self.assertEqual(json.loads(audit[0]),before)
            self.assertEqual(audit[1],hashlib.sha256(backup.read_bytes()).hexdigest())
            self.assertIn('outcome remains unknown',audit[2])
        reg=Registry(self.path)
        try:
            self.assertEqual(reg.unresolved('bot_1'),0)
            reg.mark(self.gid,'sent')
            self.assertEqual(reg.transaction(self.gid)['state'],'closed_unknown')
            with self.assertRaisesRegex(RemoteError,'manually closed'):
                reg.wait('bot_1','3',0,session='session-one')
            self.assertEqual(reg.transaction(self.gid)['state'],'closed_unknown')
        finally: reg.close()

    def test_requires_explicit_acknowledgement_and_reason(self):
        for kwargs in ({'acknowledge':False},{'reason':''}):
            with self.subTest(kwargs=kwargs), self.assertRaises(ValueError):self.close(**kwargs)
        self.assertEqual(self.read(self.gid)['state'],'uncertain')

    def test_refuses_gateway_lock(self):
        with FileLock(self.root/'runtime/gateway.instance.lock',timeout=0):
            with self.assertRaises(TimeoutError):self.close()
        self.assertEqual(self.read(self.gid)['state'],'uncertain')

    def test_refuses_newer_than_24_hours_or_future(self):
        for created in (NOW-3600,NOW+3600):
            self.update('UPDATE transactions SET created=? WHERE gid=?',(created,self.gid))
            with self.assertRaises(ValueError):self.close()
        self.assertFalse((self.root/'runtime/recovery').exists())

    def test_refuses_other_bot_or_resolved_request(self):
        with self.assertRaises(ValueError):self.close(bot='bot_2')
        self.update("UPDATE transactions SET state='accepted' WHERE gid=?",(self.gid,))
        with self.assertRaises(ValueError):self.close()

    def test_refuses_any_active_order_even_from_another_bot(self):
        payload=json.dumps(dict(order_num=123,trans_id=self.other,flags=1))
        self.update('INSERT INTO orders VALUES (?,?,?,?,?)',('123','bot_2',payload,1,1))
        with self.assertRaisesRegex(ValueError,'active orders'):self.close()

    def test_refuses_known_order_or_cancellation(self):
        self.update("UPDATE transactions SET order_num='123' WHERE gid=?",(self.gid,))
        with self.assertRaises(ValueError):self.close()
        self.update("UPDATE transactions SET order_num=NULL,action='KILL_ORDER' WHERE gid=?",(self.gid,))
        with self.assertRaises(ValueError):self.close()

    def test_refuses_matching_inactive_order_and_preserves_large_number(self):
        number='2024400996377485842'
        self.update('INSERT INTO orders VALUES (?,?,?,?,?)',(number,'bot_1',json.dumps(dict(order_num=int(number),trans_id=self.gid,flags=28)),0,1))
        with self.assertRaisesRegex(ValueError,number):self.close()

    def test_second_close_is_not_applied_again(self):
        self.close()
        with self.assertRaises(ValueError):self.close()
        self.assertEqual(len(list((self.root/'runtime/recovery').glob('*.sqlite3'))),1)

    def test_new_request_intent_is_persisted_without_mutating_request(self):
        reg=Registry(self.path)
        try:
            request=dict(TRANS_ID='9',ACTION='NEW_ORDER',SECCODE='SPREAD',PRICE='870',QUANTITY='1')
            gid=reg.allocate('bot_3','9','NEW_ORDER',request=request)
            saved=json.loads(reg.transaction(gid)['request'])
            self.assertEqual(saved,dict(request,TRANS_ID=str(gid)))
            self.assertEqual(request['TRANS_ID'],'9')
        finally: reg.close()

    def test_late_actual_reply_is_recorded_without_erasing_resolution_audit(self):
        self.close()
        reg=Registry(self.path)
        try:
            reg.on_reply(dict(trans_id=self.gid,status=4,order_num=0,result_msg='actual refusal'))
            self.assertEqual(reg.transaction(self.gid)['state'],'rejected')
            self.assertEqual(reg.db.execute('SELECT COUNT(*) FROM transaction_resolutions').fetchone()[0],1)
        finally: reg.close()

class InstrumentDiagnosticsTests(unittest.TestCase):
    def test_missing_class_does_not_require_registering_or_trading(self):
        qp=Mock()
        qp.GetClassesList.return_value={'data':' SPBFUT, TQBR;'}
        qp.GetSecurityInfo.return_value={'data':None}
        qp.IsConnected.return_value={'data':1}
        cfg=SimpleNamespace(BOT_ID='bot_1',CLASS='FUTSPREAD',SECCODE='W4V6W4X6',BASE_ASSET_CODE='W4X6')
        result=instrument_status(qp,cfg)
        self.assertFalse(result['class_available'])
        self.assertFalse(result['instrument_found'])
        qp.GetClassSecurities.assert_not_called()
        qp.RegisterBot.assert_not_called()
        qp.SendTransaction.assert_not_called()

    def test_present_class_but_missing_instrument_is_reported_separately(self):
        qp=Mock()
        qp.GetClassesList.return_value={'data':'FUTSPREAD;SPBFUT;'}
        qp.GetClassSecurities.return_value={'data':'W4X6W4Z6,OTHER;'}
        qp.GetSecurityInfo.return_value={'data':None}
        qp.IsConnected.return_value={'data':1}
        cfg=SimpleNamespace(BOT_ID='bot_1',CLASS='FUTSPREAD',SECCODE='W4V6W4X6',BASE_ASSET_CODE='W4X6')
        result=instrument_status(qp,cfg)
        self.assertTrue(result['class_available'])
        self.assertFalse(result['instrument_found'])
        self.assertEqual(result['similar_instruments'],['W4X6W4Z6'])
        qp.RegisterBot.assert_not_called()

if __name__=='__main__':unittest.main()
