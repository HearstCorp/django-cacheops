local db_table = ARGV[1]
local random = ARGV[2]
local obj = cjson.decode(ARGV[3])


-- Utility functions
local conj_cache_key = function (db_table, scheme, obj)
    local parts = {}
    for field in string.gmatch(scheme, "[^,]+") do
        table.insert(parts, field .. '=' .. tostring(obj[field]):upper())
    end

    return 'conj:' .. db_table .. ':' .. table.concat(parts, '&')
end

local rename_key = function (old_key, new_key)
    redis.call('rename', old_key, new_key)
end

-- Calculate conj keys
local renamed_keys = {}
local schemes = redis.call('smembers', 'schemes:' .. db_table)
for _, scheme in ipairs(schemes) do
    local key = conj_cache_key(db_table, scheme, obj)
    local new_key = key .. '.' .. random .. '.delete'
    pcall(rename_key, key, new_key)
    table.insert(renamed_keys, new_key)
end

return renamed_keys
