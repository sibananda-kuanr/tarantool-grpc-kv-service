-- Tarantool 3.2.x initialisation script.
-- Run with: tarantool init.lua
--
-- Creates a single 'kv' space with the schema required by the task:
--   key   string      (primary key)
--   value varbinary   (nullable)
--
-- The guest user is granted full access so the Java client can connect
-- without a password (matching the TarantoolConnection16Impl default).

box.cfg {
    listen    = '0.0.0.0:3301',
    log_level = 5,              -- verbose enough for development
    memtx_memory = 512 * 1024 * 1024,  -- 512 MB, comfortable for 5 M records
}

-- box.once ensures this block runs only on the very first startup.
-- On subsequent restarts the snapshot already contains the space.
box.once('schema_v1', function()

    local kv = box.schema.space.create('kv', { if_not_exists = true })

    kv:format({
        { name = 'key',   type = 'string'   },
        { name = 'value', type = 'varbinary', is_nullable = true },
    })

    -- TREE index keeps keys in sorted order — essential for efficient range scans.
    kv:create_index('primary', {
        type            = 'TREE',
        parts           = { 'key' },
        unique          = true,
        if_not_exists   = true,
    })

    -- Allow the guest user (no password) to read and write so the
    -- Java client can connect without credentials.
    box.schema.user.grant('guest', 'read,write,execute', 'universe', nil,
        { if_not_exists = true })

    print('[Tarantool] Schema initialised: space "kv" is ready.')
end)
