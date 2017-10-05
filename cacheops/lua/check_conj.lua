local key = KEYS[1]
local dnfs = cjson.decode(ARGV[1])


local conj_cache_key = function (db_table, conj)
    local parts = {}
    for _, eq in ipairs(conj) do
        table.insert(parts, eq[1] .. '=' .. tostring(eq[2]))
    end

    return 'conj:' .. db_table .. ':' .. table.concat(parts, '&')
end


for _, disj_pair in ipairs(dnfs) do
    local db_table = disj_pair[1]
    local disj = disj_pair[2]
    for _, conj in ipairs(disj) do
        local conj_key = conj_cache_key(db_table, conj)
        if redis.call('sismember', conj_key, key) == 1 then
            return true
        end
    end
end

return false
