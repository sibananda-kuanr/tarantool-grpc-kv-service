package com.example.kvservice;

import org.tarantool.TarantoolConnection16;
import org.tarantool.TarantoolConnection16Impl;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Thread-safe Tarantool client wrapper.
 * Uses org.tarantool:connector:1.6.9 (1.6.x series required by task spec).
 *
 * <p>The legacy connector (1.6.x) uses integer-based space/index IDs.
 * We resolve the KV space ID and primary index ID at init time via eval.
 */
public class TarantoolConnector implements AutoCloseable {

    // Tarantool iterator constants (from iproto protocol)
    // EQ=0, REQ=1, ALL=2, LT=3, LE=4, GE=5, GT=6, BITS_ALL_SET=7, BITS_ANY_SET=8, BITS_ALL_NOT_SET=9
    private static final int ITER_EQ = 0;
    private static final int ITER_GE = 5;

    private final TarantoolConnection16 conn;
    private final int spaceId;
    private final int indexId;

    @SuppressWarnings("unchecked")
    public TarantoolConnector(String host, int port) throws IOException {
        this.conn = new TarantoolConnection16Impl(host, port);
        System.out.println("Connected to Tarantool at " + host + ":" + port);

        // Resolve space ID for 'kv'
        List<?> result = conn.eval(
                "local s = box.space.kv; return s.id, s.index.primary.id"
        );
        if (result == null || result.isEmpty()) {
            throw new RuntimeException("Could not resolve kv space/index IDs");
        }
        this.spaceId = ((Number) result.get(0)).intValue();
        this.indexId = ((Number) result.get(1)).intValue();
        System.out.println("KV space ID=" + spaceId + ", primary index ID=" + indexId);
    }

    /**
     * put(key, value) — insert or overwrite (replace).
     * value may be null (stored as varbinary null).
     */
    public void put(String key, byte[] value) {
        // replace handles both insert and overwrite on the primary key
        conn.replace(spaceId, Arrays.asList(key, value));
    }

    /**
     * get(key) — returns the tuple [key, value] or null if not found.
     */
    @SuppressWarnings("unchecked")
    public List<Object> get(String key) {
        List<?> result = conn.select(
                spaceId,
                indexId,
                Arrays.asList(key),
                0, 1, ITER_EQ
        );
        if (result == null || result.isEmpty()) {
            return null;
        }
        Object first = result.get(0);
        if (first instanceof List) {
            return (List<Object>) first;
        }
        return null;
    }

    /**
     * delete(key) — deletes the record. Returns true if something was deleted.
     */
    @SuppressWarnings("unchecked")
    public boolean delete(String key) {
        List<?> result = conn.delete(
                spaceId,
                Arrays.asList(key)
        );
        // Tarantool returns the deleted tuple; empty list means nothing was deleted
        return result != null && !result.isEmpty();
    }

    /**
     * range(keySince, keyTo) — returns all tuples with key in [keySince, keyTo].
     * TREE index GE iterator, stop when key > keyTo.
     */
    @SuppressWarnings("unchecked")
    public List<List<Object>> range(String keySince, String keyTo) {
        // GE on TREE index gives ordered results starting from keySince
        List<?> result = conn.select(
                spaceId,
                indexId,
                Arrays.asList(keySince),
                0, Integer.MAX_VALUE, ITER_GE
        );

        List<List<Object>> filtered = new ArrayList<>();
        if (result == null) return filtered;

        for (Object item : result) {
            if (!(item instanceof List)) continue;
            List<Object> tuple = (List<Object>) item;
            if (tuple.isEmpty()) continue;
            String tupleKey = (String) tuple.get(0);
            if (tupleKey.compareTo(keyTo) > 0) break; // past range end
            filtered.add(tuple);
        }
        return filtered;
    }

    /**
     * count() — returns total record count in the KV space.
     */
    @SuppressWarnings("unchecked")
    public long count() {
        List<?> result = conn.eval("return box.space.kv:len()");
        if (result == null || result.isEmpty()) return 0;
        Object first = result.get(0);
        if (first instanceof Number) {
            return ((Number) first).longValue();
        }
        return 0;
    }

    @Override
    public void close() {
        try {
            conn.close();
        } catch (Exception e) {
            System.err.println("Error closing Tarantool connection: " + e.getMessage());
        }
    }
}
