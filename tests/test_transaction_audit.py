"""Execute the read-only QUIK diagnostic with native Lua 5.3 integers."""
import tempfile
import unittest
from pathlib import Path
from lupa.lua53 import LuaRuntime

ROOT = Path(__file__).resolve().parents[1]

class TransactionAuditTests(unittest.TestCase):
    def test_snapshot_exports_inactive_orders_legs_and_exact_ids_without_trading(self):
        with tempfile.TemporaryDirectory(dir=ROOT/'runtime/tests') as folder:
            lua = LuaRuntime(unpack_returned_tuples=True, encoding="cp1251")
            lua.globals().getScriptPath = lambda: folder
            lua.execute("""
                calls = 0
                function sendTransaction() calls=calls+1; error('Trading forbidden') end
                function message() end
                data = {
                    orders={
                        {class_code='FUTSPREAD',sec_code='W4V6W4X6',account='DEMO',
                         trans_id=1000000002,order_num=1892960587504945213,flags=28,qty=1,balance=0,price=800},
                        {sec_code='OTHER',trans_id=5,order_num=9},
                        {sec_code='W4X6',trans_id=23,order_num=10,flags=1}
                    },
                    trades={{sec_code='W4V6',trans_id=23,order_num=10,trade_num=1892960587504251635}}
                }
                function getNumberOf(name) return #data[name] end
                function getItem(name,index) return data[name][index+1] end
            """)
            lua.execute((ROOT/'tools/quik_transaction_audit.lua').read_text(encoding='utf-8'))
            path = Path(lua.globals().main())
            text = path.read_text(encoding='utf-8')
            self.assertIn('1892960587504945213',text)
            self.assertIn('1892960587504251635',text)
            self.assertIn('1000000002',text)
            self.assertIn('W4X6',text)
            self.assertIn('W4V6',text)
            self.assertNotIn('OTHER',text)
            self.assertEqual(lua.globals().calls,0)
            self.assertEqual(text.count('orders\t'),2)

    def test_missing_target_never_changes_state_or_sends_an_order(self):
        with tempfile.TemporaryDirectory(dir=ROOT/'runtime/tests') as folder:
            lua = LuaRuntime(unpack_returned_tuples=True, encoding="cp1251")
            lua.globals().getScriptPath = lambda: folder
            lua.execute("""
                function getNumberOf() return 0 end
                function getItem() error('No rows') end
                function sendTransaction() error('Trading forbidden') end
            """)
            lua.execute((ROOT/'tools/quik_transaction_audit.lua').read_text(encoding='utf-8'))
            text = Path(lua.globals().main()).read_text(encoding='utf-8')
            self.assertIn('absence is not proof',text)
            self.assertIn('matching_rows_written=0',text)

if __name__ == '__main__':
    unittest.main()
