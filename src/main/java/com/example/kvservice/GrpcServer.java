package com.example.kvservice;

import io.grpc.Server;
import io.grpc.ServerBuilder;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

/**
 * gRPC Server entry point.
 * Starts on port 9090 and registers the KV service implementation.
 */
public class GrpcServer {

    private static final int DEFAULT_PORT = 9090;
    private static final String TARANTOOL_HOST = System.getenv().getOrDefault("TARANTOOL_HOST", "localhost");
    private static final int TARANTOOL_PORT = Integer.parseInt(
            System.getenv().getOrDefault("TARANTOOL_PORT", "3301")
    );
    private static final int GRPC_PORT = Integer.parseInt(
            System.getenv().getOrDefault("GRPC_PORT", String.valueOf(DEFAULT_PORT))
    );

    private final Server server;
    private final TarantoolConnector tarantool;

    public GrpcServer() throws IOException {
        try {
            tarantool = new TarantoolConnector(TARANTOOL_HOST, TARANTOOL_PORT);
        } catch (IOException e) {
            throw new IOException("Failed to connect to Tarantool at " + TARANTOOL_HOST + ":" + TARANTOOL_PORT, e);
        }
        server = ServerBuilder
                .forPort(GRPC_PORT)
                .addService(new KvServiceImpl(tarantool))
                .build();
    }

    public void start() throws IOException {
        server.start();
        System.out.println("gRPC KV Service started on port " + GRPC_PORT);
        System.out.println("Tarantool: " + TARANTOOL_HOST + ":" + TARANTOOL_PORT);

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println("Shutting down gRPC server...");
            try {
                GrpcServer.this.stop();
            } catch (InterruptedException e) {
                e.printStackTrace(System.err);
            }
            System.out.println("Server shut down.");
        }));
    }

    public void stop() throws InterruptedException {
        if (server != null) {
            server.shutdown().awaitTermination(30, TimeUnit.SECONDS);
        }
        if (tarantool != null) {
            tarantool.close();
        }
    }

    public void blockUntilShutdown() throws InterruptedException {
        if (server != null) {
            server.awaitTermination();
        }
    }

    public static void main(String[] args) throws IOException, InterruptedException {
        System.out.println("Starting VK KV gRPC Service...");
        GrpcServer grpcServer = new GrpcServer();
        grpcServer.start();
        grpcServer.blockUntilShutdown();
    }
}
