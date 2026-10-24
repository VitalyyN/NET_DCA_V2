-- Read-only, one-shot QUIK audit. No sockets, transactions or database edits.
-- Run as a separate Lua script beside QuikSharp.lua; do not replace the connector.
local target_id = "1000000002"
local codes = {W4V6W4X6=true, W4V6=true, W4X6=true}
local max_scan = 50000
local max_rows = 2000
local fields = {"table", "row_index", "class_code", "sec_code", "account",
                "trans_id", "trans_id_str", "order_num", "trade_num", "flags",
                "qty", "balance", "price", "datetime", "withdraw_datetime"}

local function clean(value)
    if value == nil then return "" end
    return tostring(value):gsub("[\t\r\n]", " ")
end

local function stamp(value)
    if type(value) ~= "table" then return clean(value) end
    return string.format("%04d-%02d-%02d %02d:%02d:%02d.%06d",
        value.year or 0, value.month or 0, value.day or 0,
        value.hour or 0, value.min or 0, value.sec or 0,
        value.mcs or (value.ms or 0)*1000)
end

function main()
    local path = getScriptPath() .. "/quik_transaction_audit_" .. tostring(os.time()) .. ".tsv"
    local file, err = io.open(path, "w")
    if not file then error("Cannot create audit file: " .. tostring(err)) end
    local ok, problem = pcall(function()
        file:write("# Read-only snapshot; absence is not proof of transaction rejection.\n")
        file:write("# Target TRANS_ID: " .. target_id .. "\n")
        file:write(table.concat(fields, "\t") .. "\n")
        for _, name in ipairs({"orders", "trades"}) do
            local total = getNumberOf(name)
            local first = math.max(0, total-max_scan)
            local written = 0
            for index=total-1,first,-1 do
                local row = getItem(name, index)
                if row then
                    local sec = row.sec_code or row.seccode
                    if codes[sec] or tostring(row.trans_id) == target_id
                            or tostring(row.trans_id_str) == target_id then
                        if written >= max_rows then break end
                        local values = {name, index, row.class_code, sec,
                            row.account or row.trdaccid, row.trans_id, row.trans_id_str,
                            row.order_num or row.ordernum, row.trade_num or row.tradenum,
                            row.flags, row.qty, row.balance, row.price,
                            stamp(row.datetime), stamp(row.withdraw_datetime)}
                        local text = {}
                        for i=1,#fields do text[i] = clean(values[i]) end
                        file:write(table.concat(text, "\t") .. "\n")
                        written = written+1
                    end
                end
            end
            file:write("# " .. name .. ": total=" .. total .. ", scan_first=" .. first
                .. ", matching_rows_written=" .. written .. ", row_cap=" .. max_rows .. "\n")
        end
    end)
    file:close()
    if not ok then error(problem) end
    if message then message("Read-only audit saved: " .. path, 1) end
    return path
end
