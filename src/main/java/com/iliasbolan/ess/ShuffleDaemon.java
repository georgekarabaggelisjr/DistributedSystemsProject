package com.iliasbolan.ess;

import com.iliasbolan.ess.gc.StorageJanitor;
import com.iliasbolan.ess.grpc.ShuffleServiceImpl;
import io.grpc.Context;
import io.grpc.Contexts;
import io.grpc.Metadata;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import io.grpc.ServerInterceptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * The primary entry point for the standalone External Shuffle Service (ESS) Daemon.
 * <p>
 * The ESS is a critical infrastructure component in the MapReduce architecture,
 * decoupled from the compute workers to enable stateless and ephemeral compute pods.
 * This daemon manages the physical persistence of intermediate shuffle data and
 * provides a high-performance gRPC streaming interface for Peer-to-Peer data
 * retrieval by Reducer pods.
 * </p>
 * <p>
 * <b>Architecture Update: Secure Data Plane (Token Authentication).</b><br>
 * This version implements a Zero-Trust security model. It utilizes a global gRPC
 * {@link ServerInterceptor} to intercept every incoming request, extract a
 * Manager-issued HMAC token from the headers, and bind it to a thread-local
 * {@link Context}. This allows downstream services to perform cryptographic
 * origin validation without polluting the method signatures.
 * </p>
 *
 * @author Ilias Bolanakis
 * @version 1.0
 * @since 2026-04-25
 * @see com.iliasbolan.ess.gc.StorageJanitor
 * @see com.iliasbolan.ess.grpc.ShuffleServiceImpl
 */
public class ShuffleDaemon {

    private static final Logger logger = LoggerFactory.getLogger(ShuffleDaemon.class);

    /**
     * Thread-local gRPC Context Key used to propagate the authorization token securely
     * across the request lifecycle.
     */
    public static final Context.Key<String> AUTH_TOKEN_KEY = Context.key("jobToken");

    /**
     * The ASCII metadata key used to extract the authorization header from
     * the incoming gRPC network request.
     */
    private static final Metadata.Key<String> AUTH_HEADER_KEY =
            Metadata.Key.of("authorization", Metadata.ASCII_STRING_MARSHALLER);

    /**
     * Bootstraps the ESS Daemon, initializes storage maintenance, and activates
     * the secure gRPC server.
     *
     * @param args Command line arguments (Configuration is derived from Environment Variables).
     * @throws IOException If the gRPC server fails to bind or storage is inaccessible.
     * @throws InterruptedException If the server process is interrupted while running.
     */
    public static void main(String[] args) throws IOException, InterruptedException {
        // 1. Configuration Loading (Cloud-Native Pattern)
        // Configuration is externalized to support containerized deployment (e.g., Kubernetes DaemonSets).
        int port = Integer.parseInt(System.getenv().getOrDefault("ESS_PORT", "7337"));
        String baseShuffleDir = System.getenv().getOrDefault("SHUFFLE_DIR", "/mnt/mapreduce-shuffle");

        logger.info("====== [ ESS DAEMON BOOTSTRAP ] ======");
        logger.info("Port: {} | Shuffle Path: {}", port, baseShuffleDir);

        // 2. Initialize the Storage Maintenance Service
        // The Janitor prevents physical disk exhaustion by purging stale job data
        // using a high-watermark reclamation algorithm.
        StorageJanitor.start(baseShuffleDir);

        // --- 3. SECURE INTERCEPTOR ---
        // Implementation of the security gateway that extracts headers before message parsing.
        ServerInterceptor authInterceptor = new ServerInterceptor() {
            @Override
            public <ReqT, RespT> ServerCall.Listener<ReqT> interceptCall(
                    ServerCall<ReqT, RespT> call, Metadata headers, ServerCallHandler<ReqT, RespT> next) {

                // Extract the token from the network layer
                String token = headers.get(AUTH_HEADER_KEY);

                // Bind the token to the gRPC Thread-Local Context for downstream validation
                Context context = Context.current().withValue(AUTH_TOKEN_KEY, token);

                // Transfer control to the service implementation with the enriched context
                return Contexts.interceptCall(context, call, headers, next);
            }
        };

        // 4. Configure and start the gRPC Service Plane
        // We apply the interceptor globally to ensure all service methods are protected.
        Server server = ServerBuilder.forPort(port)
                .addService(new ShuffleServiceImpl(baseShuffleDir))
                .intercept(authInterceptor)
                .build();

        server.start();
        logger.info("ESS Daemon successfully bound. Secure Interceptor ACTIVE.");

        // 5. Graceful Teardown hook
        // Ensures that physical storage managers and active streams are closed before process termination.
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            logger.warn("Shutdown signal received. Terminating ESS Daemon...");
            server.shutdown();
            StorageJanitor.stop();
        }));

        // Block the main thread until the server is shut down
        server.awaitTermination();
    }
}