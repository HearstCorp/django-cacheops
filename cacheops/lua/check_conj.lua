local prefix = KEYS[1]
local key = KEYS[2]
local dnfs = cjson.decode(ARGV[1])

local conj_cache_key = function (db_table, conj)
    local parts = {}
    for field, val in pairs(conj) do
        table.insert(parts, field .. '=' .. tostring(val))
    end

    return prefix .. 'conj:' .. db_table .. ':' .. table.concat(parts, '&')
end

for db_table, disj in pairs(dnfs) do
    for _, conj in ipairs(disj) do
        local conj_key = conj_cache_key(db_table, conj)
        if redis.call('sismember', conj_key, key) == 1 then
            return true
        end
    end
end

return false
