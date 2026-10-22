"""Execute the actual connector under Lua 5.3 with mock terminal and sockets."""
import unittest
from pathlib import Path
from lupa.lua53 import LuaRuntime

ROOT = Path(__file__).resolve().parents[1] / 'QuikPy' / 'QUIK' / 'lua'


class LuaTests(unittest.TestCase):
    def setUp(self):
        self.lua = LuaRuntime(unpack_returned_tuples=True)
        self.lua.execute("""
            script_path = 'MOCK'
            os.execute = function() return true end
            io.open = function()
                return {write=function() end, flush=function() end,
                        close=function() end}
            end
            function getInfoParam() return '13.2' end
            local socket = {gettime=function() return 1 end}
            send_calls = 0
            function client()
                return {closed=false, payload='', parts={},
                    settimeout=function(self, t) assert(t == 0) end,
                    close=function(self) self.closed=true end,
                    send=function(self, text, offset)
                        send_calls = send_calls + 1
                        local last = math.min(#text, offset+6)
                        self.payload = self.payload .. text:sub(offset,last)
                        if last < #text then return nil,'timeout',last end
                        return last
                    end,
                    receive=function(self, kind, prefix)
                        local item=table.remove(self.parts,1)
                        if not item then return nil,'timeout',prefix end
                        if item[2] == 'timeout' then
                            return nil,'timeout',prefix .. item[1]
                        elseif item[2] == 'closed' then
                            return nil,'closed',''
                        else return prefix .. item[1] end
                    end}
            end
            response=client(); callback=client()
            local n=0
            function socket.bind()
                n=n+1
                local value=n==1 and response or callback
                return {settimeout=function() end,close=function() end,
                    accept=function(self)
                        local result=value; value=nil; return result
                    end}
            end
            package.preload.socket=function() return socket end
        """)
        for name in ('dkjson', 'qsutils', 'qscallbacks'):
            code = (ROOT / f'{name}.lua').read_text(encoding='utf-8-sig')
            self.lua.execute(f'package.preload["{name}"]=function()\n{code}\nend')
        self.lua.execute("""
            util=require('qsutils'); require('qscallbacks')
            util.connect('127.0.0.1',34230,'127.0.0.1',34231)
            util.set_filters({{account='DEMO', class_code='FUTSPREAD',
                              sec_code='SPREAD',base_asset_code='LEG'}})
        """)

    def test_callbacks_never_write_sockets_and_filter(self):
        self.lua.execute("""
            OnOrder({account='OTHER',class_code='FUTSPREAD',sec_code='SPREAD'})
            OnAllTrade({}); OnQuote('A','B'); OnParam('A','B')
            OnOrder({account='DEMO',class_code='FUTSPREAD',sec_code='SPREAD',
                     order_num=1,flags=1})
            assert(send_calls==0)
            for i=1,100 do util.pump() end
            assert(callback.payload:find('"order_num":1',1,true))
            assert(not callback.payload:find('OTHER',1,true))
        """)

    def test_partial_send_and_fragmented_json(self):
        self.lua.execute("""
            response.parts={{'{"cmd":"echo",','timeout'},
                            {'"data":false,"id":8}'}}
            assert(receiveRequest()==nil)
            local request=receiveRequest()
            assert(request.cmd=='echo' and request.data==false)
            sendResponse(request)
            for i=1,100 do util.pump() end
            local restored=from_json(response.payload)
            assert(restored.id==8 and restored.data==false)
        """)

    def test_eof_and_invalid_json_disconnect(self):
        self.lua.execute("""
            response.parts={{'', 'closed'}}
            assert(receiveRequest()==nil and not is_connected)
            assert(response.closed and callback.closed)
        """)

    def test_queue_overflow_reconciles_and_stop_shared_flag(self):
        self.lua.execute("""
            for i=1,5000 do OnTransReply({trans_id=i,status=3}) end
            assert(send_calls==0)
            util.pump()
            assert(not is_connected and response.closed and callback.closed)
            assert(OnStop()==1000)
            assert(not util.running)
        """)


    def test_trailing_or_malformed_json_disconnects(self):
        for text in ('{"cmd":"echo"}broken', '{invalid'):
            self.setUp()
            self.lua.globals().bad_json = text
            self.lua.execute("""
                response.parts={{bad_json}}
                assert(receiveRequest()==nil and not is_connected)
            """)

    def test_nested_callback_bytes_and_cycles_are_bounded(self):
        for scenario in ('large', 'cycle'):
            self.setUp()
            self.lua.globals().scenario = scenario
            self.lua.execute("""
                local data = {}
                if scenario == 'large' then
                    data.nested = {text=string.rep('x', 5*1024*1024)}
                else data.nested = data end
                assert(sendCallback({cmd='OnTransReply',data=data}) == false)
                assert(send_calls == 0)
                util.pump()
                assert(not is_connected and response.closed and callback.closed)
            """)

    def test_continuous_callback_queue_memory_stays_bounded(self):
        self.lua.execute("""
            callback.send = function(self,text,offset) return #text end
            for i=1,64 do OnTransReply({trans_id=i,status=3}) end
            collectgarbage('collect')
            local before = collectgarbage('count')
            for round=1,1000 do
                for i=1,32 do OnTransReply({trans_id=i,status=3}) end
                util.pump()
            end
            collectgarbage('collect')
            local after = collectgarbage('count')
            assert(is_connected)
            assert(after - before < 256, 'Retained Lua memory grew: '..(after-before))
        """)

    def test_actual_main_stops_and_closes_resources(self):
        source = (ROOT/'qsfunctions.lua').read_text(encoding='utf-8-sig')
        self.lua.execute('package.preload.qsfunctions=function()\n'+source+'\nend')
        self.lua.execute("""
            getScriptPath=function() return 'MOCK' end
            io.open=function() return nil end
            sleep=function() OnStop() end
        """)
        self.lua.execute((ROOT/'QuikSharp.lua').read_text(encoding='utf-8-sig'))
        self.lua.execute("""
            main()
            assert(not util.running)
            assert(response.closed and callback.closed)
            assert(logfile==nil)
        """)

    def test_filtered_active_snapshot_and_recent_trade_limit(self):
        source = (ROOT/'qsfunctions.lua').read_text(encoding='utf-8-sig')
        self.lua.execute('qf=(function()\n'+source+'\nend)()')
        self.lua.execute("""
            getNumberOf=function(table_name) return 300 end
            getItem=function(table_name,i)
                return {order_num=i,trade_num=i,account='DEMO',
                        class_code='FUTSPREAD',sec_code='SPREAD',
                        flags=i%2}
            end
            local p={account='DEMO',class_code='FUTSPREAD',sec_code='SPREAD'}
            local orders=qf.get_active_orders({data=p}).data
            assert(#orders==150)
            for _,item in ipairs(orders) do assert(item.flags==1) end
            local trades=qf.get_recent_trades({data=p}).data
            assert(#trades==200 and trades[1].trade_num==100)
            p.account='OTHER'
            assert(#qf.get_active_orders({data=p}).data==0)
        """)

    def test_nil_security_and_empty_positions_have_explicit_json_types(self):
        source = (ROOT/'qsfunctions.lua').read_text(encoding='utf-8-sig')
        self.lua.execute('qf=(function()\n'+source+'\nend)()')
        self.lua.execute("""
            getSecurityInfo=function() return nil end
            getNumberOf=function() return 0 end
            local missing=qf.dispatch_and_process({cmd='getSecurityInfo',data='OTHER|LEG',id=1})
            assert(to_json(missing):find('"data":null',1,true))
            for _,cmd in ipairs({'getTradeAccounts','getFuturesClientHoldings'}) do
                local response=qf.dispatch_and_process({cmd=cmd,data='',id=2})
                assert(to_json(response):find('"data":[]',1,true))
            end
        """)

    def test_connector_sources_compile(self):
        compile_lua = self.lua.eval('function(text) local f,e=load(text); assert(f,e); return true end')
        for name in ('QuikSharp.lua','qsfunctions.lua','qsutils.lua','qscallbacks.lua'):
            self.assertTrue(compile_lua((ROOT/name).read_text(encoding='utf-8-sig')))

    def test_lua_log_size_rotation_and_age_cleanup(self):
        self.lua.execute("""
            closeLog()
            print=function() end
            fs={}
            io.open=function(path,mode)
                if mode=='rb' and not fs[path] then return nil end
                fs[path]=fs[path] or ''
                return {
                    close=function() end, flush=function() end,
                    seek=function() return #fs[path] end,
                    read=function(self,n) return fs[path]:sub(1,n) end,
                    write=function(self,text) fs[path]=fs[path]..text; return self end}
            end
            os.remove=function(path) fs[path]=nil; return true end
            os.rename=function(source,target)
                if not fs[source] then return nil end
                fs[target]=fs[source]; fs[source]=nil; return true
            end
            local path='MOCK\\\\logs\\\\NetDCA_lua.log'
            fs[path..'.3']='2000-01-01 old archive'
            logfile=openLog()
            assert(fs[path..'.3']==nil)
            for i=1,520 do log(string.rep('x',64000),0) end
            local count=0
            for name,value in pairs(fs) do
                count=count+1
                assert(#value<=5*1024*1024)
            end
            assert(count<=6 and count>1)
            assert(fs[path..'.1'])
        """)

    def test_lua_small_log_from_previous_day_rotates(self):
        self.lua.execute("""
            closeLog()
            local path='MOCK\\\\logs\\\\NetDCA_lua.log'
            fs={[path]='2000-01-01 old log'}
            io.open=function(name,mode)
                if mode=='rb' and not fs[name] then return nil end
                fs[name]=fs[name] or ''
                return {close=function() end, flush=function() end,
                        seek=function() return #fs[name] end,
                        read=function(self,n) return fs[name]:sub(1,n) end,
                        write=function(self,text) fs[name]=fs[name]..text; return self end}
            end
            os.remove=function(name) fs[name]=nil; return true end
            os.rename=function(source,target)
                if not fs[source] then return nil end
                fs[target]=fs[source]; fs[source]=nil; return true
            end
            logfile=openLog()
            assert(fs[path]=='')
            assert(fs[path..'.1']==nil) -- expired after daily rotation
        """)


if __name__ == '__main__':
    unittest.main()
