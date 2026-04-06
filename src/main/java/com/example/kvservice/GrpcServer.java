package com.example.kvservice;

import io.grpc.Server;
import io.grpc.ServerBuilder;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

/**
 * Bootstraps and runs the KV gRPC server.
 *
 * <p>Configuration is read from environment variables so the service can be
 * deployed in a container without rebuilding:
 * <ul>
 *   <li>{@code TARANTOOL_HOST} – Tarantool hostname (default: {@code localhost})</li>
 *   <li>{@code TARANTOOL_PORT} – Tarantool port      (default: {@code 3301})</li>
 *   <li>{@code GRPC_PORT}      – gRPC listen port    (default: {@code 9090})</li>
 * </ul>
 */
public class GrpcServer {

    private static final String TARANTOOL_HOST = env("TARANTOOL_HOST", "localhost");
    private static final int    TARANTOOL_PORT = intEnv("TARANTOOL_PORT", 3301);
    private static final int    GRPC_PORT      = intEnv("GRPC_PORT", 9090);

    private final TarantoolConnector tarantool;
    private final Server server;

    public GrpcServer() throws IOException {
        this.tarantool = new TarantoolConnector(TARANTOOL_HOST, TARANTOOL_PORT);
        this.server = ServerBuilder
                .forPort(GRPC_PORT)
                .addService(new KvServiceImpl(tarantool))
                .build();
    }

    /** Starts the server and registers a JVM shutdown hook for clean teardown. */
    public void start() throws IOException {
        server.start();
        System.out.printf("[gRPC] Server listening on port %d%n", GRPC_PORT);

        // Clean shutdown when the JVM exits (Ctrl+C or SIGTERM).
        Runtime.getRuntime().addShutdownHook(
                new Thread(this::stopQuietly, "grpc-shutdown-hook")
        );
    }

    /** Blocks the calling thread until the server terminates. */
    public void awaitTermination() throws InterruptedException {
        server.awaitTermination();
    }

    private void stopQuietly() {
        System.out.println("[gRPC] Shutting down...");
        try {
            server.shutdown();
            if (!server.awaitTermination(30, TimeUnit.SECONDS)) {
                server.shutdownNow();
            }
        } catch (InterruptedException e) {
            server.shutdownNow();
            Thread.currentThread().interrupt();
        } finally {
            tarantool.close();
            System.out.println("[gRPC] Shutdown complete.");
        }
    }

    // -------------------------------------------------------------------------
    // Configuration helpers
    // -------------------------------------------------------------------------

    private static String env(String name, String defaultValue) {
        String value = System.getenv(name);
        return (value != null && !value.isBlank()) ? value : defaultValue;
    }

    private static int intEnv(String name, int defaultValue) {
        String value = System.getenv(name);
        if (value == null || value.isBlank()) {
            return defaultValue;
        }
        try {
            return Integer.parseInt(value.trim());
        } catch (NumberFormatException e) {
            System.err.printf("[config] Invalid value for %s='%s', using default %d%n",
                    name, value, defaultValue);
            return defaultValue;
        }
    }

    // -------------------------------------------------------------------------
    // Entry point
    // -------------------------------------------------------------------------

    public static void main(String[] args) throws IOException, InterruptedException {
        System.out.println("=== VK KV gRPC Service ===");

        GrpcServer app = new GrpcServer();
        app.start();
        app.awaitTermination();
    }
}
