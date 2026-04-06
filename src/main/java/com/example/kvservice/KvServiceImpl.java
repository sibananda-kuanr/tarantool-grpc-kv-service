package com.example.kvservice;

import com.example.kvservice.proto.CountRequest;
import com.example.kvservice.proto.CountResponse;
import com.example.kvservice.proto.DeleteRequest;
import com.example.kvservice.proto.DeleteResponse;
import com.example.kvservice.proto.GetRequest;
import com.example.kvservice.proto.GetResponse;
import com.example.kvservice.proto.KvServiceGrpc;
import com.example.kvservice.proto.PutRequest;
import com.example.kvservice.proto.PutResponse;
import com.example.kvservice.proto.RangeRequest;
import com.example.kvservice.proto.RangeResponse;
import com.google.protobuf.ByteString;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;

import java.util.List;

/**
 * Implements every RPC defined in {@code kv_service.proto}.
 *
 * <p>Each method follows the same structure:
 * <ol>
 *   <li>Extract parameters from the protobuf request.</li>
 *   <li>Call the corresponding {@link TarantoolConnector} method.</li>
 *   <li>Map the result back into a protobuf response and complete the call.</li>
 *   <li>On any error, propagate it as a gRPC {@code INTERNAL} status.</li>
 * </ol>
 */
public class KvServiceImpl extends KvServiceGrpc.KvServiceImplBase {

    private final TarantoolConnector tarantool;

    public KvServiceImpl(TarantoolConnector tarantool) {
        this.tarantool = tarantool;
    }

    // -------------------------------------------------------------------------
    // Put
    // -------------------------------------------------------------------------

    /**
     * Stores a value under the given key, overwriting any existing record.
     *
     * <p>An empty {@code ByteString} in the request is interpreted as a null
     * value and stored as a Tarantool {@code varbinary null}. This matches the
     * requirement that put/get must work correctly with null values.
     */
    @Override
    public void put(PutRequest request, StreamObserver<PutResponse> out) {
        try {
            // Proto3 defaults an absent bytes field to empty ByteString.
            // We treat empty-equals-null so callers can store a null value.
            byte[] value = request.getValue().isEmpty()
                    ? null
                    : request.getValue().toByteArray();

            tarantool.put(request.getKey(), value);

            out.onNext(PutResponse.newBuilder().setSuccess(true).build());
            out.onCompleted();
        } catch (Exception e) {
            out.onError(Status.INTERNAL
                    .withDescription("put failed: " + e.getMessage())
                    .withCause(e)
                    .asRuntimeException());
        }
    }

    // -------------------------------------------------------------------------
    // Get
    // -------------------------------------------------------------------------

    /**
     * Retrieves the value stored under the given key.
     *
     * <p>The response distinguishes three cases:
     * <ul>
     *   <li>Key not found → {@code found=false}</li>
     *   <li>Key found, value is null → {@code found=true, value_is_null=true}</li>
     *   <li>Key found, value present → {@code found=true, value=<bytes>}</li>
     * </ul>
     */
    @Override
    public void get(GetRequest request, StreamObserver<GetResponse> out) {
        try {
            List<Object> tuple = tarantool.get(request.getKey());
            GetResponse.Builder response = GetResponse.newBuilder();

            if (tuple == null) {
                response.setFound(false);
            } else {
                response.setFound(true);
                Object raw = tuple.size() > 1 ? tuple.get(1) : null;

                if (raw == null) {
                    // Key exists but the stored value is a varbinary null.
                    response.setValueIsNull(true).setValue(ByteString.EMPTY);
                } else if (raw instanceof byte[]) {
                    response.setValue(ByteString.copyFrom((byte[]) raw));
                } else {
                    // Defensive fallback — should not normally occur with varbinary.
                    response.setValue(ByteString.copyFromUtf8(raw.toString()));
                }
            }

            out.onNext(response.build());
            out.onCompleted();
        } catch (Exception e) {
            out.onError(Status.INTERNAL
                    .withDescription("get failed: " + e.getMessage())
                    .withCause(e)
                    .asRuntimeException());
        }
    }

    // -------------------------------------------------------------------------
    // Delete
    // -------------------------------------------------------------------------

    /** Removes the record for the given key. */
    @Override
    public void delete(DeleteRequest request, StreamObserver<DeleteResponse> out) {
        try {
            boolean deleted = tarantool.delete(request.getKey());
            out.onNext(DeleteResponse.newBuilder().setDeleted(deleted).build());
            out.onCompleted();
        } catch (Exception e) {
            out.onError(Status.INTERNAL
                    .withDescription("delete failed: " + e.getMessage())
                    .withCause(e)
                    .asRuntimeException());
        }
    }

    // -------------------------------------------------------------------------
    // Range (server-streaming)
    // -------------------------------------------------------------------------

    /**
     * Streams all records whose keys fall in the inclusive range
     * [{@code key_since}, {@code key_to}].
     *
     * <p>Records are fetched from Tarantool in small pages so that memory
     * consumption stays bounded even when the range spans millions of records.
     * The TREE index guarantees ascending key order, which lets us stop as soon
     * as we encounter a key beyond the requested upper bound.
     */
    @Override
    public void range(RangeRequest request, StreamObserver<RangeResponse> out) {
        try {
            String keySince = request.getKeySince();
            String keyTo    = request.getKeyTo();
            int batchSize   = tarantool.getBatchSize();
            int offset      = 0;
            boolean done    = false;

            while (!done) {
                List<List<Object>> page = tarantool.selectPage(keySince, offset, batchSize);
                if (page.isEmpty()) {
                    break; // no more data
                }

                for (List<Object> tuple : page) {
                    String key = (String) tuple.get(0);

                    if (key.compareTo(keyTo) > 0) {
                        // We've gone past the requested upper bound.
                        done = true;
                        break;
                    }

                    out.onNext(buildRangeResponse(key, tuple));
                }

                // If the page was smaller than the batch size, it was the last one.
                if (page.size() < batchSize) {
                    break;
                }
                offset += batchSize;
            }

            out.onCompleted();
        } catch (Exception e) {
            out.onError(Status.INTERNAL
                    .withDescription("range failed: " + e.getMessage())
                    .withCause(e)
                    .asRuntimeException());
        }
    }

    // -------------------------------------------------------------------------
    // Count
    // -------------------------------------------------------------------------

    /** Returns the total number of records currently stored in the KV space. */
    @Override
    public void count(CountRequest request, StreamObserver<CountResponse> out) {
        try {
            long total = tarantool.count();
            out.onNext(CountResponse.newBuilder().setCount(total).build());
            out.onCompleted();
        } catch (Exception e) {
            out.onError(Status.INTERNAL
                    .withDescription("count failed: " + e.getMessage())
                    .withCause(e)
                    .asRuntimeException());
        }
    }

    // -------------------------------------------------------------------------
    // Helpers
    // -------------------------------------------------------------------------

    /**
     * Converts a raw Tarantool tuple into a {@link RangeResponse} protobuf message.
     * Handles the nullable value field the same way as {@link #get}.
     */
    private RangeResponse buildRangeResponse(String key, List<Object> tuple) {
        RangeResponse.Builder item = RangeResponse.newBuilder().setKey(key);
        Object raw = tuple.size() > 1 ? tuple.get(1) : null;

        if (raw == null) {
            item.setValueIsNull(true).setValue(ByteString.EMPTY);
        } else if (raw instanceof byte[]) {
            item.setValue(ByteString.copyFrom((byte[]) raw));
        } else {
            item.setValue(ByteString.copyFromUtf8(raw.toString()));
        }

        return item.build();
    }
}
