package io.grpc.replication;

import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.stub.StreamObserver;
import io.grpc.transprocessing.KV;
import io.grpc.transprocessing.Operation;
import io.grpc.transprocessing.Transaction;
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

public class ReplicationServer {

    private static final Logger logger = Logger.getLogger(ReplicationServer.class.getName());
    private static final Map<String, String> dataStore = new HashMap<>();
    private static int logPosition = 0;

    private final int port;
    private final Server server;
    private final String hostIP;

    public ReplicationServer(String hostIP, int port) throws IOException {
        this(port, TransactionUtil.getExistingDataFile(), hostIP);
    }

    /** create a transaction processing server listening on {@code port} using {@code dataFile} */
    public ReplicationServer(int port, URL dataFile, String hostIP) throws IOException {
        this(ServerBuilder.forPort(port), port, TransactionUtil.parseData(dataFile), hostIP);
    }

    /** Create a transaction processing server using serverBuilder as a base and key-value pair as data. */
    public ReplicationServer(ServerBuilder<?> serverBuilder, int port, Collection<KV> kvPairs, String hostIP) {
        this.port = port;
        this.hostIP = hostIP;
        server = serverBuilder.addService(new ReplicationService(this.hostIP.concat("-").concat(Integer.toString(this.port)), kvPairs, this.dataStore)).build();
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
                ReplicationServer.this.stop();
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
     * Main
     */
    public static void main(String[] args) throws Exception {
        ReplicationServer server = new ReplicationServer(args[0], Integer.parseInt(args[1]));
        server.start();
        server.blockUntilShutdown();
    }

    /**
     * Our implementation of Replication Service
     */
    public static class ReplicationService extends ReplicationGrpc.ReplicationImplBase {

        private static final List<AlreadyAccepted> votedList = new ArrayList<>();
        private static final List<Transaction> replog = new ArrayList<>();

        private final Map<String, String> datastore;
        private final String hostname;

        public ReplicationService(String hostName, Collection<KV> kvPairs, Map<String, String> dataStore) {
            this.hostname = hostName;
            this.datastore = dataStore;
            for (KV kvPair : kvPairs) {
                this.datastore.put(kvPair.getKey(), kvPair.getValue());
            }
        }

        /**
         * Leader election. Paxos is used here. Follower has to consent to a leader
         * request if it has the highest sequence number that follower has ever seen; or
         * else it responds with the highest number that it saw.
         */
        @Override
        public void electLeader(RequestToPrepare request, StreamObserver<ReplyOfPrepared> responseObserver) {
            responseObserver.onNext(replyToPotentialLeader(request));
            responseObserver.onCompleted();
        }

        private ReplyOfPrepared replyToPotentialLeader(RequestToPrepare request) {

            ReplyOfPrepared.Builder builder = ReplyOfPrepared.newBuilder();
            if (request.getSequenceId() < this.votedList.get(this.votedList.size() - 1).getHighestSeenSequenceId()) {
                builder.setAccepted(this.votedList.get(this.votedList.size() - 1));
            } else {
                AlreadyAccepted.Builder acceptBuilder = AlreadyAccepted.newBuilder();
                AlreadyAccepted.newBuilder().setHighestSeenSequenceId(request.getSequenceId()).setAcceptedCandidate(request.getCandidate());
                AlreadyAccepted alreadyAccepted = acceptBuilder.build();

                this.votedList.add(alreadyAccepted);
                builder.setAccepted(alreadyAccepted);
            }
            builder.setVoter(hostname);


            return builder.build();
        }

        /**
         * Proposing a value for log position
         */
        @Override
        public void proposeValue(ValueProposed request, StreamObserver<ValueAccepted> responseObserver) {
            responseObserver.onNext(replicateTransaction(request));
            responseObserver.onCompleted();
        }

        private ValueAccepted replicateTransaction(ValueProposed request) {
            ValueAccepted.Builder valueBuilder = ValueAccepted.newBuilder();

            if (logPosition >= request.getLogPosition()) {
                return null;
            }

            Transaction transaction = request.getTrans();
            for (Operation operation : transaction.getOperationList()) {
                if (operation.getType() == Operation.Type.WRITE) {
                    this.datastore.put(operation.getKvPair().getKey(), operation.getKvPair().getValue());
                }
            }
            logPosition = request.getLogPosition();
            logger.info("Log position " + logPosition + " applied with transaction " + request.getTrans().getTransactionID());

            valueBuilder.setLogPosition(request.getLogPosition()).setAcceptor(this.hostname);
            return valueBuilder.build();
        }
    }

}