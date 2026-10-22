--~ Copyright (c) 2014-2020 QUIKSharp Authors https://github.com/finsight/QUIKSharp/blob/master/AUTHORS.md. All rights reserved.
--~ Licensed under the Apache License, Version 2.0. See LICENSE.txt in the project root for license information.

local socket = require ("socket")
local json = require ("dkjson")
local qsutils = {}

--- Sleep that always works
function delay(msec)
    if sleep then
        pcall(sleep, msec)
    else
        -- pcall(socket.select, nil, nil, msec / 1000)
    end
end

-- high precision current time
function timemsec()
    local st, res = pcall(socket.gettime)
    if st then
        return (res) * 1000
    else
        log("unexpected error in timemsec", 3)
        error("unexpected error in timemsec")
    end
end

-- Returns the name of the file that calls this function (without extension)
function scriptFilename()
    -- Check that Lua runtime was built with debug information enabled
    if not debug or not debug.getinfo then
        return nil
    end
    local full_path = debug.getinfo(2, "S").source:sub(2)
    return string.gsub(full_path, "^.*\\(.*)[.]lua[c]?$", "%1")
end

-- when true will show QUIK message for log(...,0)
is_debug = false

-- Bounded connector logs: daily/size rotation, at most five archives.
local LOG_BYTES = 5 * 1024 * 1024
local LOG_BACKUPS = 5
local LOG_DAYS = 14
local log_path, log_day, log_bytes
local last_log_maintenance = 0

local function prefix(path)
    local file = io.open(path, "rb")
    if not file then return nil end
    local ok, text = pcall(function() return file:read(10) end)
    pcall(file.close, file)
    return ok and text or nil
end

local function purge_lua_logs(path)
    local cutoff = os.time() - LOG_DAYS * 86400
    for i = 1, LOG_BACKUPS do
        local name = path .. "." .. i
        local value = prefix(name)
        if value then
            local y, m, d = value:match("^(%d%d%d%d)%-(%d%d)%-(%d%d)$")
            if y then
                local ok, stamp = pcall(os.time, {
                    year = tonumber(y), month = tonumber(m), day = tonumber(d), hour = 12})
                if ok and stamp < cutoff then pcall(os.remove, name) end
            end
        end
    end
end

local function rotate_lua_log(path)
    pcall(os.remove, path .. "." .. LOG_BACKUPS)
    for i = LOG_BACKUPS - 1, 1, -1 do
        pcall(os.rename, path .. "." .. i, path .. "." .. (i + 1))
    end
    local ok, moved = pcall(os.rename, path, path .. ".1")
    return ok and moved
end

function openLog(force_rotate)
    local path = log_path or (script_path .. "\\logs\\NetDCA_lua.log")
    local file = io.open(path, "a")
    if not file and not log_path then
        path = script_path .. "\\NetDCA_lua.log"
        file = io.open(path, "a")
    end
    if not file then return nil end
    log_path = path
    local ok, size = pcall(function() return file:seek("end") end)
    log_bytes = ok and size or 0
    local today = os.date("%Y-%m-%d")
    local first = prefix(path)
    log_day = (first and first:match("^%d%d%d%d%-%d%d%-%d%d$")) and first or today
    if force_rotate or log_bytes >= LOG_BYTES or log_day ~= today then
        pcall(file.close, file)
        if not rotate_lua_log(path) then return nil end
        file = io.open(path, "a")
        log_bytes, log_day = 0, today
    end
    purge_lua_logs(path)
    return file
end

function qsutils.log_maintenance()
    local now = os.time()
    if now - last_log_maintenance < 60 then return end
    last_log_maintenance = now
    if log_path then
        if logfile and log_day ~= os.date("%Y-%m-%d") then
            closeLog()
            logfile = openLog(true)
        end
        purge_lua_logs(log_path)
    end
end

-- Returns contents of config.json file or nil if no such file exists
function readConfigAsJson()
    local conf = io.open (script_path.. "\\config.json", "r")
    if not conf then
        return nil
    end
    local content = conf:read "*a"
    conf:close()
    return from_json(content)
end

function paramsFromConfig(scriptName)
    local params = {}
    -- just default values
    table.insert(params, "127.0.0.1") -- responseHostname
    table.insert(params, 34230)       -- responsePort
    table.insert(params, "127.0.0.1") -- callbackHostname
    table.insert(params, 34231)       -- callbackPort

    local config = readConfigAsJson()
    if not config or not config.servers then
        return nil
    end
    local found = false
    for i=1,#config.servers do
        local server = config.servers[i]
        if server.scriptName == scriptName then
            found = true
            if server.responseHostname then
                params[1] = server.responseHostname
            end
            if server.responsePort then
                params[2] = server.responsePort
            end
            if server.callbackHostname then
                params[3] = server.callbackHostname
            end
            if server.callbackPort then
                params[4] = server.callbackPort
            end
        end
    end

    if found then
        return params
    else
        return nil
    end
end

-- closes log
function closeLog()
    if logfile then
        pcall(logfile.close, logfile)
        logfile = nil
    end
end

logfile = openLog()

--- Write to log file and to Quik messages
function log(msg, level)
    if not msg then msg = "" end
    msg = tostring(msg)
    if #msg > 64000 then msg = msg:sub(1, 64000) .. " ... [truncated]" end
    if level == 1 or level == 2 or level == 3 or is_debug then
        -- only warnings and recoverable errors to Quik
        if message then
            pcall(message, msg, level)
        end
    end
    if not level then level = 0 end
    local logLine = "LOG "..level..": "..msg
    print(logLine)
    local msecs = math.floor(math.fmod(timemsec(), 1000));
    local record = os.date("%Y-%m-%d %H:%M:%S").."."..msecs.." "..logLine.."\n"
    if logfile and (log_day ~= os.date("%Y-%m-%d") or log_bytes + #record > LOG_BYTES) then
        closeLog()
        logfile = openLog(true)
    end
    if logfile then
        local ok, written = pcall(logfile.write, logfile, record)
        if ok and written then log_bytes = log_bytes + #record end
        pcall(logfile.flush, logfile)
    end
end

-- Doesn't work if string contains empty values, eg. 'foo,,bar'. You get {'foo','bar'} instead of {'foo', '', 'bar'}
function split(inputstr, sep)
    if sep == nil then
        sep = "%s"
    end
    local t={}
    local i=1
    for str in string.gmatch(inputstr, "([^"..sep.."]+)") do
        t[i] = str
        i = i + 1
    end
    return t
end

-- https://stackoverflow.com/questions/1426954/split-string-in-lua#comment73602874_7615129
function split2(inputstr, sep)
    sep = sep or '%s'
    local t = {}
    for field, s in string.gmatch(inputstr, "([^"..sep.."]*)("..sep.."?)") do
        table.insert(t, field)
        if s == "" then
            return t
        end
    end
end

function from_json(str)
    local ok, value, position, err = pcall(json.decode, str, 1, json.null)
    if not ok then return nil, value end
    if err or not position or str:sub(position):find("%S") then
        return nil, err or "Unexpected trailing JSON data"
    end
    return value
end

function to_json(msg)
    local status, str= pcall(json.encode, msg, { indent = false }) -- dkjson
    if status then
        return str
    else
        error(str)
    end
end

-- All socket I/O belongs to main(), never to a QUIK callback.
is_connected = false
qsutils.running = true
local response_server, callback_server
local response_client, callback_client
local accepted_at
local request_partial = ""
local response_buffer, response_offset
local callback_buffer, callback_offset
local queue, head, tail, queue_bytes = {}, 1, 0, 0
local filters = {}
local overflow = false
local MAX_MESSAGES = 4096
local MAX_BYTES = 8 * 1024 * 1024
local MAX_FRAME = 4 * 1024 * 1024
local metrics = {queued = 0, sent = 0, dropped = 0, high_water = 0}
local last_metrics = 0

local function safe_close(client)
    if client then pcall(client.close, client) end
end

local function disconnect()
    is_connected = false
    safe_close(response_client)
    safe_close(callback_client)
    response_client, callback_client = nil, nil
    accepted_at = nil
    request_partial = ""
    response_buffer, callback_buffer = nil, nil
    response_offset, callback_offset = nil, nil
    queue, head, tail, queue_bytes = {}, 1, 0, 0
    filters = {}
    overflow = false
end

function qsutils.set_filters(profiles)
    filters = profiles or {}
end

function qsutils.matches(kind, data)
    if not is_connected then return false end
    for _, profile in ipairs(filters) do
        local account = data.account or data.trdaccid
        if kind == "OnFuturesClientHolding" then
            if account == profile.account and data.sec_code == profile.base_asset_code then
                return true
            end
        elseif account == profile.account
            and data.class_code == profile.class_code
            and data.sec_code == profile.sec_code then
            return true
        end
    end
    return false
end

function sendCallback(msg)
    if not is_connected or overflow then return false end
    if tail - head + 1 >= MAX_MESSAGES then
        metrics.dropped = metrics.dropped + 1
        overflow = true
        return false
    end
    -- Bound the complete nested snapshot before JSON encoding in main().
    local estimate = 128
    local visiting = {}
    local function snapshot_value(value, depth)
        if depth > 8 then error("Callback nesting limit") end
        if type(value) ~= "table" then
            estimate = estimate + #tostring(value) + 24
            if estimate > MAX_FRAME or queue_bytes + estimate > MAX_BYTES then
                error("Callback byte limit")
            end
            return value
        end
        if visiting[value] then error("Cyclic callback data") end
        visiting[value] = true
        local snapshot = {}
        estimate = estimate + 64
        for key, item in pairs(value) do
            if type(key) ~= "string" and type(key) ~= "number" then
                error("Unsupported callback key")
            end
            estimate = estimate + #tostring(key) + 24
            snapshot[key] = snapshot_value(item, depth + 1)
        end
        visiting[value] = nil
        return snapshot
    end
    local ok, snapshot = pcall(snapshot_value, msg.data, 0)
    if not ok or queue_bytes + estimate > MAX_BYTES then
        metrics.dropped = metrics.dropped + 1
        overflow = true  -- main closes the transport; gateway must reconcile.
        return false
    end
    msg.data = snapshot
    tail = tail + 1
    queue[tail] = {message = msg, estimate = estimate}
    queue_bytes = queue_bytes + estimate
    metrics.queued = metrics.queued + 1
    metrics.high_water = math.max(metrics.high_water, queue_bytes)
    return true
end

function qsutils.connect(response_host, response_port, callback_host, callback_port)
    if not qsutils.running then return end
    if not response_server then
        local err
        response_server, err = socket.bind(response_host, response_port, 1)
        if not response_server then error("Response port unavailable: " .. tostring(err)) end
        response_server:settimeout(0)
    end
    if not callback_server then
        local err
        callback_server, err = socket.bind(callback_host, callback_port, 1)
        if not callback_server then error("Callback port unavailable: " .. tostring(err)) end
        callback_server:settimeout(0)
    end
    if not response_client then
        response_client = response_server:accept()
        if response_client then
            response_client:settimeout(0)
            accepted_at = timemsec()
        end
    end
    if not callback_client then
        callback_client = callback_server:accept()
        if callback_client then
            callback_client:settimeout(0)
            accepted_at = accepted_at or timemsec()
        end
    end
    if response_client and callback_client and not is_connected then
        is_connected = true
        log("NetDCA_v2 gateway connected", 0)
    elseif not is_connected and accepted_at and timemsec() - accepted_at > 5000 then
        disconnect()
    end
end

function receiveRequest()
    if not is_connected or response_buffer then return nil end
    local ok, line, err, partial = pcall(
        response_client.receive, response_client, "*l", request_partial)
    if not ok then disconnect(); return nil end
    if line then
        request_partial = ""
        if #line > MAX_FRAME then disconnect(); return nil end
        local value = from_json(line)
        if type(value) ~= "table" or type(value.cmd) ~= "string" then
            disconnect()
            return nil
        end
        return value
    elseif err == "timeout" then
        request_partial = partial or ""
        if #request_partial > MAX_FRAME then disconnect() end
        return nil
    else
        disconnect()
        return nil
    end
end

function sendResponse(msg)
    if not is_connected then return false end
    response_buffer = to_json(msg) .. "\n"
    if #response_buffer > MAX_FRAME then disconnect(); return false end
    response_offset = 1
    return true
end

local function send_part(client, buffer, offset)
    local ok, last, err, partial = pcall(client.send, client, buffer, offset)
    if not ok then return nil, "closed" end
    if last then return last + 1 end
    if err == "timeout" then return (partial or (offset - 1)) + 1 end
    return nil, err
end

function qsutils.pump()
    qsutils.log_maintenance()
    if overflow then
        log("NetDCA_v2 event queue overflow: disconnecting for reconciliation", 2)
        disconnect()
        return
    end
    if not is_connected then return end
    if response_buffer then
        local offset = send_part(response_client, response_buffer, response_offset)
        if not offset then disconnect(); return end
        response_offset = offset
        if response_offset > #response_buffer then
            response_buffer, response_offset = nil, nil
        end
    end
    for _ = 1, 32 do
        if not callback_buffer then
            local entry = queue[head]
            if not entry then break end
            queue[head] = nil
            head = head + 1
            queue_bytes = queue_bytes - entry.estimate
            if head > tail then queue, head, tail = {}, 1, 0 end
            if head >= MAX_MESSAGES then
                local compact = {}
                for index = head, tail do compact[index - head + 1] = queue[index] end
                tail = tail - head + 1
                queue, head = compact, 1
            end
            callback_buffer = to_json(entry.message) .. "\n"
            callback_offset = 1
            if #callback_buffer > MAX_FRAME then disconnect(); return end
        end
        local offset = send_part(callback_client, callback_buffer, callback_offset)
        if not offset then disconnect(); return end
        callback_offset = offset
        if callback_offset > #callback_buffer then
            callback_buffer, callback_offset = nil, nil
            metrics.sent = metrics.sent + 1
        else
            break
        end
    end
    if timemsec() - last_metrics >= 60000 then
        last_metrics = timemsec()
        sendCallback({cmd = "OnBridgeMetrics", t = last_metrics, data = {
            lua_kb = collectgarbage("count"), queue_messages = tail - head + 1,
            queue_estimated_bytes = queue_bytes, queue_high_water = metrics.high_water,
            events_queued = metrics.queued, events_sent = metrics.sent,
            events_dropped = metrics.dropped, lua_version = _VERSION,
            quik_version = getInfoParam and getInfoParam("VERSION") or ""
        }})
    end
end

function qsutils.request_stop()
    qsutils.running = false
end

function qsutils.close()
    qsutils.running = false
    disconnect()
    safe_close(response_server)
    safe_close(callback_server)
    response_server, callback_server = nil, nil
end

return qsutils
