package io.grpc.replication;

import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.stub.StreamObserver;
import io.grpc.transprocessing.KV;
import io.grpc.transprocessing.Operation;
import io.grpc.transprocessing.Transaction;
import io.grpc.transprocessing.TransactionUtil;

import java.io.IOException;
import java.net.InetAddress;
import java.net.URL;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;

/**
 * A replication server is an independent process
 * To invoke a replication server, do:
 *
 * % replication-server <hostIP> <port>
 *
 * Need to pass the hostIP because replication server needs it
 * in its reply to indicate the acceptor name.
 */
public class ReplicationServer {

    private static final Logger logger = Logger.getLogger(ReplicationServer.class.getName());
    private static final Map<String, String> dataStore = new HashMap<>();
    private static int logPosition = 0;

    private final int port;
    private final Server server;

    public ReplicationServer(int port, List<ReplicationClient> replicationClients) throws IOException {
        this(port, TransactionUtil.getExistingDataFile(), replicationClients);
    }

    /** create a transaction processing server listening on {@code port} using {@code dataFile} */
    public ReplicationServer(int port, URL dataFile, List<ReplicationClient> replicationClients) throws IOException {
        this(ServerBuilder.forPort(port), port, TransactionUtil.parseData(dataFile), replicationClients);
    }

    /** Create a transaction processing server using serverBuilder as a base and key-value pair as data. */
    public ReplicationServer(ServerBuilder<?> serverBuilder, int port, Collection<KV> kvPair, List<ReplicationClient> replicationClients) throws UnknownHostException {
        this.port = port;
        server = serverBuilder.addService(new ReplicationService(InetAddress.getLocalHost().toString().concat("-").concat(Integer.toString(this.port)),
                kvPair, this.dataStore, replicationClients)).build();
    }

    /** Start serving requests. */
    public void start() throws IOException {
        server.start();
        logger.info("Server started, listening on " + port);

        new Thread() {
            @Override
            public void run() {
                long timeThen = System.currentTimeMillis();
                while (true) {
                    long timeNow = System.currentTimeMillis();
                    if ((timeNow - timeThen) > 100) {
                        System.out.println("Yay!");
                        timeThen = timeNow;
                    }
                }

            }
        }.start();

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
        List<ReplicationClient> replClients = new ArrayList<>();
        ReplicationServer server = new ReplicationServer(Integer.parseInt(args[0]), replClients);
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
        private final List<ReplicationClient> replicationClients;

        public ReplicationService(String hostName, Collection<KV> kvPairs, Map<String, String> dataStore,
                                  List<ReplicationClient> replicationClients) {
            this.hostname = hostName;
            this.datastore = dataStore;
            for (KV kvPair : kvPairs) {
                this.datastore.put(kvPair.getKey(), kvPair.getValue());
            }
            this.replicationClients = replicationClients;
        }

        /**
         * At the init time, a server that joins after this one will send a handshaking message
         * to register the replication service on the host with other replicas
         */
        @Override
        public void handShaking(LocalIdentity otherServerInfo, StreamObserver<Welcome> responseObserver) {
            responseObserver.onNext(registerOtherReplica(otherServerInfo));
            responseObserver.onCompleted();
        }

        private Welcome registerOtherReplica(LocalIdentity otherReplica) {
            Welcome.Builder welcomeBuilder = Welcome.newBuilder();
            welcomeBuilder.setReplyingHostIp(this.hostname);

            ReplicationClient client = new ReplicationClient(otherReplica.getLocalhostIp(),
                    otherReplica.getListeningPort(), this.hostname);

            if (this.replicationClients.add(client)) {
                welcomeBuilder.setWelcome2JoinMessage("Welcome to join!");
                return welcomeBuilder.build();
            } else {
                return welcomeBuilder.setWelcome2JoinMessage("Failed to register").build();
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
