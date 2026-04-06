package com.example.kvservice;

import com.example.kvservice.proto.*;
import com.google.protobuf.ByteString;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Smoke test that exercises all five KV service operations against
 * a live gRPC server (must be running on localhost:9090).
 *
 * Run after starting Tarantool and the gRPC service:
 *   docker compose up -d
 *   java -jar target/kv-service-1.0.0.jar
 *   java -cp target/kv-service-1.0.0.jar com.example.kvservice.SmokeTest
 */
public class SmokeTest {

    private static int passed = 0;
    private static int failed = 0;

    public static void main(String[] args) throws Exception {
        ManagedChannel channel = ManagedChannelBuilder
                .forAddress("localhost", 9090)
                .usePlaintext()
                .build();

        KvServiceGrpc.KvServiceBlockingStub client = KvServiceGrpc.newBlockingStub(channel);

        try {
            System.out.println("=== KV Service Smoke Test ===\n");

            testPutAndGet(client);
            testOverwrite(client);
            testNullValue(client);
            testDelete(client);
            testRange(client);
            testCount(client);

        } finally {
            channel.shutdown().awaitTermination(5, TimeUnit.SECONDS);
        }

        System.out.printf("%n=== Results: %d passed, %d failed ===%n", passed, failed);
        if (failed > 0) System.exit(1);
    }

    // ---- Test methods -------------------------------------------------------

    static void testPutAndGet(KvServiceGrpc.KvServiceBlockingStub client) {
        // put("hello", "world")
        PutResponse put = client.put(PutRequest.newBuilder()
                .setKey("hello")
                .setValue(ByteString.copyFromUtf8("world"))
                .build());
        check("put returns success=true", put.getSuccess());

        // get("hello") → found=true, value="world"
        GetResponse get = client.get(GetRequest.newBuilder().setKey("hello").build());
        check("get: found=true",  get.getFound());
        check("get: value=world", "world".equals(get.getValue().toStringUtf8()));
        check("get: not null",    !get.getValueIsNull());
    }

    static void testOverwrite(KvServiceGrpc.KvServiceBlockingStub client) {
        // put("hello", "updated") — overwrite
        client.put(PutRequest.newBuilder()
                .setKey("hello")
                .setValue(ByteString.copyFromUtf8("updated"))
                .build());

        GetResponse get = client.get(GetRequest.newBuilder().setKey("hello").build());
        check("overwrite: found",          get.getFound());
        check("overwrite: value=updated",  "updated".equals(get.getValue().toStringUtf8()));
    }

    static void testNullValue(KvServiceGrpc.KvServiceBlockingStub client) {
        // put("nullkey", <empty>) — stored as null
        client.put(PutRequest.newBuilder().setKey("nullkey").build()); // no setValue → empty ByteString

        GetResponse get = client.get(GetRequest.newBuilder().setKey("nullkey").build());
        check("null value: found",         get.getFound());
        check("null value: valueIsNull",   get.getValueIsNull());
        check("null value: bytes empty",   get.getValue().isEmpty());

        // Cleanup
        client.delete(DeleteRequest.newBuilder().setKey("nullkey").build());
    }

    static void testDelete(KvServiceGrpc.KvServiceBlockingStub client) {
        // delete existing key
        DeleteResponse del = client.delete(DeleteRequest.newBuilder().setKey("hello").build());
        check("delete existing: deleted=true", del.getDeleted());

        // get deleted key → not found
        GetResponse get = client.get(GetRequest.newBuilder().setKey("hello").build());
        check("after delete: found=false", !get.getFound());

        // delete non-existent key
        DeleteResponse del2 = client.delete(DeleteRequest.newBuilder().setKey("no-such-key").build());
        check("delete missing: deleted=false", !del2.getDeleted());
    }

    static void testRange(KvServiceGrpc.KvServiceBlockingStub client) {
        // Seed data: apple, banana, cherry, date, elderberry
        for (String k : new String[]{"apple", "banana", "cherry", "date", "elderberry"}) {
            client.put(PutRequest.newBuilder()
                    .setKey(k).setValue(ByteString.copyFromUtf8(k + "_value")).build());
        }

        // range("banana", "date") → banana, cherry, date  (3 records)
        List<String> keys = new ArrayList<>();
        client.range(RangeRequest.newBuilder()
                .setKeySince("banana").setKeyTo("date").build())
                .forEachRemaining(r -> keys.add(r.getKey()));

        check("range returns 3 records",         keys.size() == 3);
        check("range[0] = banana",               "banana".equals(keys.get(0)));
        check("range[1] = cherry",               "cherry".equals(keys.get(1)));
        check("range[2] = date",                 "date".equals(keys.get(2)));

        // Cleanup
        for (String k : new String[]{"apple", "banana", "cherry", "date", "elderberry"}) {
            client.delete(DeleteRequest.newBuilder().setKey(k).build());
        }
    }

    static void testCount(KvServiceGrpc.KvServiceBlockingStub client) {
        // After cleanup above the space should be empty
        CountResponse count = client.count(CountRequest.newBuilder().build());
        check("count = 0 after cleanup", count.getCount() == 0);

        // Add 3 records and recount
        for (int i = 1; i <= 3; i++) {
            client.put(PutRequest.newBuilder()
                    .setKey("k" + i).setValue(ByteString.copyFromUtf8("v" + i)).build());
        }
        CountResponse count2 = client.count(CountRequest.newBuilder().build());
        check("count = 3 after inserts", count2.getCount() == 3);

        // Cleanup
        for (int i = 1; i <= 3; i++) {
            client.delete(DeleteRequest.newBuilder().setKey("k" + i).build());
        }
    }

    // ---- Assertion helper ---------------------------------------------------

    static void check(String label, boolean condition) {
        if (condition) {
            System.out.printf("  [PASS] %s%n", label);
            passed++;
        } else {
            System.out.printf("  [FAIL] %s%n", label);
            failed++;
        }
    }
}
