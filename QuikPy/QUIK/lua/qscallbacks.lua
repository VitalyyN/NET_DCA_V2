-- NetDCA_v2: QUIK callbacks only filter and enqueue short snapshots.
local util = require("qsutils")
local callbacks = {}

local function enqueue(name, data)
    sendCallback({cmd = name, t = timemsec(), data = data})
end

function OnQuikSharpDisconnected() end
function OnInit(path) end
function OnConnected() enqueue("OnConnected", "") end
function OnDisconnected() enqueue("OnDisconnected", "") end
function OnError(text) enqueue("lua_error", tostring(text)) end

function OnOrder(order)
    if order and util.matches("OnOrder", order) then enqueue("OnOrder", order) end
end

function OnTrade(trade)
    if trade and util.matches("OnTrade", trade) then enqueue("OnTrade", trade) end
end

function OnTransReply(reply)
    if is_connected and reply then enqueue("OnTransReply", reply) end
end

function OnFuturesClientHolding(holding)
    if holding and util.matches("OnFuturesClientHolding", holding) then
        enqueue("OnFuturesClientHolding", holding)
    end
end

-- These feeds are not used by the grid strategies.
function OnAllTrade(trade) end
function OnQuote(class_code, sec_code) end
function OnParam(class_code, sec_code) end

function OnStop()
    util.request_stop()
    return 1000
end

function OnClose()
    util.request_stop()
end

return callbacks
