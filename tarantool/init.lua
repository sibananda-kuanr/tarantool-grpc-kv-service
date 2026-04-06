-- Tarantool 3.2.x initialization script
-- Creates the KV space with the required schema

box.cfg{
    listen = 3301,
    log_level = 5,
    memtx_memory = 512 * 1024 * 1024, -- 512MB for 5M records
}

box.once('bootstrap', function()
    -- Create KV space with the exact required schema
    local kv = box.schema.space.create('kv', {
        if_not_exists = true,
        comment = 'Key-Value storage space'
    })

    kv:format({
        {name = 'key',   type = 'string'},
        {name = 'value', type = 'varbinary', is_nullable = true},
    })

    -- TREE index on key (ordered, supports range queries efficiently)
    kv:create_index('primary', {
        type   = 'TREE',
        parts  = {'key'},
        unique = true,
        if_not_exists = true,
    })

    -- Grant full access to guest user for the Java client
    box.schema.user.grant('guest', 'read,write,execute', 'universe', nil, {
        if_not_exists = true
    })

    print('KV space initialized successfully')
end)
