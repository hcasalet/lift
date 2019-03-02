package io.grpc.nodes;

import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.replication.ReplicationClient;
import io.grpc.replication.ReplicationServer;
import io.grpc.transprocessing.KV;
import io.grpc.transprocessing.TransactionProcessor;
import io.grpc.transprocessing.TransactionUtil;

import java.io.IOException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;

public class ServerReplica {
    private static final Logger logger = Logger.getLogger(ServerReplica.class.getName());

    private final String hostIP;
    private final int port;
    private final Server server;

    private static final Map<String, String> dataStore = new HashMap<>();
    private List<ReplicationClient> replicationClients = new ArrayList<>();

    /** create a server at {@code hostIP} and {@code port} */
    public ServerReplica(String hostIp, int port, Map<String, Integer> replicationServers) throws IOException {
        this(hostIp, port, TransactionUtil.getExistingDataFile(), replicationServers);
    }

    /** create a server with installing the data from the last check-pointed consistent state, and
     *  also installing the replication log for changes after the checkpoint.
     * */
    public ServerReplica(String hostIp, int port, URL consistentStateDatafile, Map<String, Integer> replicationServers) throws IOException {
        this(ServerBuilder.forPort(port), hostIp, port, TransactionUtil.parseData(consistentStateDatafile), replicationServers);
    }

    /** create a server using serverBuilder */
    public ServerReplica(ServerBuilder<?> serverBuilder, String hostIp, int port, Collection<KV> kvPairs, Map<String, Integer> replicationServers) {
        this.hostIP = hostIp;
        this.port = port;
        for (Map.Entry<String, Integer> item : replicationServers.entrySet()) {
            this.replicationClients.add(new ReplicationClient(item.getKey(), item.getValue(), item.getKey().concat("-").concat(Integer.toString(item.getValue()))));
        }
        server = serverBuilder.addService(new TransactionProcessor.TransactionProcessingService(kvPairs, dataStore, this.replicationClients)).addService(
                new ReplicationServer.ReplicationService(hostIp, kvPairs, dataStore)).build();
    }

    /** server starts serving requests. */
    public void start() throws IOException {
        server.start();
        logger.info("Server started, listening on " + port);
        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                // Use stderr here since the logger may has been reset by its JVM shutdown hook.
                System.err.println("*** shutting down gRPC server since JVM is shutting down");
                ServerReplica.this.stop();
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

    /**
     * Await termination on the main thread since the grpc library uses daemon threads.
     */
    private void blockUntilShutdown() throws InterruptedException {
        if (server != null) {
            server.awaitTermination();
        }
    }

    /**
     * Main method.  This comment makes the linter happy.
     */
    public static void main(String[] args) throws Exception {
        Map<String, Integer> replicationServers = new HashMap<>();

        for (int i=2; i < args.length; i++) {
            replicationServers.put(args[i], Integer.parseInt(args[++i]));
        }

        ServerReplica replica = new ServerReplica(args[0], Integer.parseInt(args[1]), replicationServers);
        replica.start();
        replica.blockUntilShutdown();

    }

}
