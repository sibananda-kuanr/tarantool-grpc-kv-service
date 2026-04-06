package com.example.kvservice;

import com.example.kvservice.proto.*;
import com.google.protobuf.ByteString;
import io.grpc.stub.StreamObserver;

import java.util.List;

/**
 * gRPC service implementation for the KV operations.
 * Delegates all data operations to TarantoolConnector.
 */
public class KvServiceImpl extends KvServiceGrpc.KvServiceImplBase {

    private final TarantoolConnector tarantool;

    public KvServiceImpl(TarantoolConnector tarantool) {
        this.tarantool = tarantool;
    }

    /**
     * put(key, value) — stores the value for new keys in the database and overwrites the value for existing ones.
     * Handles null values (empty ByteString treated as null in storage).
     */
    @Override
    public void put(PutRequest request, StreamObserver<PutResponse> responseObserver) {
        try {
            String key = request.getKey();
            // Convert protobuf bytes to byte[] — treat empty ByteString as null (varbinary null)
            ByteString valueBytes = request.getValue();
            byte[] value = valueBytes.isEmpty() ? null : valueBytes.toByteArray();

            tarantool.put(key, value);

            responseObserver.onNext(PutResponse.newBuilder().setSuccess(true).build());
            responseObserver.onCompleted();
        } catch (Exception e) {
            responseObserver.onError(
                    io.grpc.Status.INTERNAL
                            .withDescription("put failed: " + e.getMessage())
                            .withCause(e)
                            .asRuntimeException()
            );
        }
    }

    /**
     * get(key) — returns the value for the specified key.
     * Works correctly with null values in the value field.
     */
    @Override
    public void get(GetRequest request, StreamObserver<GetResponse> responseObserver) {
        try {
            String key = request.getKey();
            List<Object> tuple = tarantool.get(key);

            GetResponse.Builder builder = GetResponse.newBuilder();

            if (tuple == null) {
                // Key not found
                builder.setFound(false);
            } else {
                builder.setFound(true);
                Object rawValue = tuple.size() > 1 ? tuple.get(1) : null;
                if (rawValue == null) {
                    // Key exists but value is null
                    builder.setValueIsNull(true);
                    builder.setValue(ByteString.EMPTY);
                } else if (rawValue instanceof byte[]) {
                    builder.setValue(ByteString.copyFrom((byte[]) rawValue));
                } else {
                    // Fallback: convert to string bytes
                    builder.setValue(ByteString.copyFromUtf8(rawValue.toString()));
                }
            }

            responseObserver.onNext(builder.build());
            responseObserver.onCompleted();
        } catch (Exception e) {
            responseObserver.onError(
                    io.grpc.Status.INTERNAL
                            .withDescription("get failed: " + e.getMessage())
                            .withCause(e)
                            .asRuntimeException()
            );
        }
    }

    /**
     * delete(key) — deletes the value for the specified key.
     */
    @Override
    public void delete(DeleteRequest request, StreamObserver<DeleteResponse> responseObserver) {
        try {
            boolean deleted = tarantool.delete(request.getKey());
            responseObserver.onNext(DeleteResponse.newBuilder().setDeleted(deleted).build());
            responseObserver.onCompleted();
        } catch (Exception e) {
            responseObserver.onError(
                    io.grpc.Status.INTERNAL
                            .withDescription("delete failed: " + e.getMessage())
                            .withCause(e)
                            .asRuntimeException()
            );
        }
    }

    /**
     * range(key_since, key_to) — returns gRPC stream of key-value pairs from the requested range.
     * Uses server-streaming to efficiently handle large result sets (5M records).
     */
    @Override
    public void range(RangeRequest request, StreamObserver<RangeResponse> responseObserver) {
        try {
            String keySince = request.getKeySince();
            String keyTo = request.getKeyTo();

            List<List<Object>> results = tarantool.range(keySince, keyTo);

            for (List<Object> tuple : results) {
                if (tuple == null || tuple.isEmpty()) continue;

                String key = (String) tuple.get(0);
                Object rawValue = tuple.size() > 1 ? tuple.get(1) : null;

                RangeResponse.Builder rb = RangeResponse.newBuilder().setKey(key);
                if (rawValue == null) {
                    rb.setValueIsNull(true);
                    rb.setValue(ByteString.EMPTY);
                } else if (rawValue instanceof byte[]) {
                    rb.setValue(ByteString.copyFrom((byte[]) rawValue));
                } else {
                    rb.setValue(ByteString.copyFromUtf8(rawValue.toString()));
                }

                responseObserver.onNext(rb.build());
            }

            responseObserver.onCompleted();
        } catch (Exception e) {
            responseObserver.onError(
                    io.grpc.Status.INTERNAL
                            .withDescription("range failed: " + e.getMessage())
                            .withCause(e)
                            .asRuntimeException()
            );
        }
    }

    /**
     * count() — returns the number of records in the database.
     */
    @Override
    public void count(CountRequest request, StreamObserver<CountResponse> responseObserver) {
        try {
            long count = tarantool.count();
            responseObserver.onNext(CountResponse.newBuilder().setCount(count).build());
            responseObserver.onCompleted();
        } catch (Exception e) {
            responseObserver.onError(
                    io.grpc.Status.INTERNAL
                            .withDescription("count failed: " + e.getMessage())
                            .withCause(e)
                            .asRuntimeException()
            );
        }
    }
}
