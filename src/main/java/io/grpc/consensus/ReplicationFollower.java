package io.grpc.consensus;

import io.grpc.ManagedChannelBuilder;
import io.grpc.Server;
import io.grpc.ServerBuilder;

import java.io.IOException;
import java.net.URL;
import java.util.Collection;
import java.util.logging.Logger;

public class ReplicationFollower {

    private static final Logger logger = Logger.getLogger(ReplicationFollower.class.getName());

    private final int port;
    private final Server server;

    /** Construct a replication follower, which is taking the role of the "server". */
    public ReplicationFollower(int port) throws IOException {
        this(port, ReplicationUtil.getDefaultReplicationLogFile());
    }

    /** Construct a replication follower, which listens on {@code port} using {@code replicationLog}. */
    public ReplicationFollower(int port, URL replicationLogFile) throws IOException {
        this(ServerBuilder.forPort(port), port, ReplicationUtil.parseLogs(replicationLogFile));
   }

    /** Create a replication follower using serverBuilder as a base and replication logs as data. */
    public ReplicationFollower(ServerBuilder<?> serverBuilder, int port, Collection<LogItem> rlogs) {
        this.port = port;
        server = serverBuilder.addService(new ReplicationService(rlogs))
                .build();
    }

    /**
     * Our implementation of Replication Service
     */
    private static class ReplicationService extends ReplicationGrpc.ReplicationImplBase {

        private final Collection<LogItem> rlogs;

        ReplicationService(Collection<LogItem> rep_logs) {
            this.rlogs = rep_logs;
        }
    }

    /** Start serving requests. */
    public void start() throws IOException {
        server.start();
        logger.info("Server started, listening on " + port);
        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                // Use stderr here since the logger may has been reset by its JVM shutdown hook.
                System.err.println("*** shutting down gRPC server since JVM is shutting down");
                ReplicationFollower.this.stop();
                System.err.println("*** server shut down");
            }
        });
    }

    /** Stop serving requests and shutdown resources. */
    public void stop() {
        if (server != null) {
            server.shutdown();
        }
    }


}
