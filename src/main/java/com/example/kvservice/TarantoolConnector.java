package com.example.kvservice;

import org.tarantool.TarantoolConnection16;
import org.tarantool.TarantoolConnection16Impl;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Wraps the Tarantool connection and exposes typed KV operations.
 *
 * <p>The task requires using {@code org.tarantool:connector} version 1.6.x.
 * That legacy connector communicates with Tarantool over the binary IPROTO
 * protocol and identifies spaces/indexes by their numeric IDs — string names
 * are not supported. We resolve both IDs once at startup via Lua eval and
 * cache them for the lifetime of the connection.
 *
 * <p>Instances of this class are thread-safe: the underlying connector
 * serialises requests internally.
 */
public class TarantoolConnector implements AutoCloseable {

    // IPROTO iterator type codes (from Tarantool docs)
    private static final int ITER_EQ = 0;  // exact key match
    private static final int ITER_GE = 5;  // first key >= search key (ordered scan)

    // Number of tuples fetched per round-trip during a range scan.
    // Keeps memory usage predictable regardless of result-set size.
    private static final int BATCH_SIZE = 500;

    private final TarantoolConnection16 connection;
    private final int spaceId;
    private final int indexId;

    /**
     * Opens a connection to Tarantool and resolves the numeric IDs for the
     * {@code kv} space and its primary index.
     *
     * @throws IOException if the connection cannot be established or the
     *                     {@code kv} space does not exist yet
     */
    public TarantoolConnector(String host, int port) throws IOException {
        this.connection = new TarantoolConnection16Impl(host, port);
        System.out.printf("[Tarantool] Connected to %s:%d%n", host, port);

        // Resolve numeric space/index IDs required by the 1.6.x connector API.
        List<?> ids = connection.eval(
                "return box.space.kv.id, box.space.kv.index.primary.id"
        );

        if (ids == null || ids.size() < 2) {
            throw new IOException(
                    "Cannot resolve 'kv' space — is Tarantool fully initialised?"
            );
        }

        this.spaceId = ((Number) ids.get(0)).intValue();
        this.indexId = ((Number) ids.get(1)).intValue();
        System.out.printf("[Tarantool] Space 'kv' id=%d, primary index id=%d%n",
                spaceId, indexId);
    }

    /**
     * Inserts or replaces the record {@code (key, value)}.
     * Passing {@code null} as value stores a Tarantool {@code varbinary null}.
     */
    public void put(String key, byte[] value) {
        connection.replace(spaceId, Arrays.asList(key, value));
    }

    /**
     * Returns the full {@code [key, value]} tuple for the given key,
     * or {@code null} when the key does not exist.
     *
     * <p>The value element of the returned tuple (index 1) may itself be
     * {@code null} when the key was stored with a null value.
     */
    @SuppressWarnings("unchecked")
    public List<Object> get(String key) {
        List<?> rows = connection.select(
                spaceId, indexId, Arrays.asList(key), 0, 1, ITER_EQ
        );
        if (rows == null || rows.isEmpty()) {
            return null;
        }
        return (List<Object>) rows.get(0);
    }

    /**
     * Deletes the record for {@code key}.
     *
     * @return {@code true} if a record was deleted; {@code false} if the key
     *         was not found
     */
    public boolean delete(String key) {
        List<?> deleted = connection.delete(spaceId, Arrays.asList(key));
        return deleted != null && !deleted.isEmpty();
    }

    /**
     * Returns one page of tuples whose keys are {@code >= keySince}, ordered
     * ascending. The caller is responsible for stopping when it encounters a
     * key that exceeds the desired upper bound.
     *
     * <p>Using pages instead of a single unlimited select prevents loading
     * millions of records into the JVM heap at once.
     *
     * @param keySince  the lower bound (inclusive) for the range scan
     * @param offset    number of matching tuples to skip (for pagination)
     * @param limit     maximum number of tuples to return
     */
    @SuppressWarnings("unchecked")
    public List<List<Object>> selectPage(String keySince, int offset, int limit) {
        List<?> raw = connection.select(
                spaceId, indexId, Arrays.asList(keySince), offset, limit, ITER_GE
        );
        if (raw == null) {
            return new ArrayList<>();
        }
        List<List<Object>> page = new ArrayList<>(raw.size());
        for (Object item : raw) {
            page.add((List<Object>) item);
        }
        return page;
    }

    /**
     * Returns the total number of records stored in the KV space.
     */
    public long count() {
        List<?> result = connection.eval("return box.space.kv:len()");
        if (result == null || result.isEmpty()) {
            return 0L;
        }
        return ((Number) result.get(0)).longValue();
    }

    /** Exposes the configured batch size so callers can page correctly. */
    public int getBatchSize() {
        return BATCH_SIZE;
    }

    @Override
    public void close() {
        connection.close();
    }
}
