package io.grpc.replication;

import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.stub.StreamObserver;
import io.grpc.transprocessing.KV;
import io.grpc.transprocessing.Operation;
import io.grpc.transprocessing.Transaction;
import io.grpc.transprocessing.TransactionUtil;

import java.io.IOException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Vector;
import java.util.logging.Logger;

public class ReplicationFollower {

    private static final Logger logger = Logger.getLogger(ReplicationFollower.class.getName());

    private final int port;
    private final Server server;
    private final String hostname;


    /** Construct a replication follower, which is taking the role of the "server". */
    public ReplicationFollower(int port, String hostName) throws IOException {
        this(port, hostName, ReplicationUtil.getDefaultReplicationLogFile(),
                TransactionUtil.getExistingDataFile());
    }

    /** Construct a replication follower, which listens on {@code port} using {@code replicationLog}. */
    public ReplicationFollower(int port, String hostName, URL replicationLogFile, URL initDBStateFile) throws IOException {
        this(ServerBuilder.forPort(port), port, hostName, ReplicationUtil.parseLogs(replicationLogFile),
                TransactionUtil.parseData(initDBStateFile));
   }

    /** Create a replication follower using serverBuilder as a base and replication logs as data. */
    public ReplicationFollower(ServerBuilder<?> serverBuilder, int port, String hostname, List<Transaction> rlogs,
                               List<KV> initState) {
        this.port = port;
        this.hostname = hostname;
        server = serverBuilder.addService(new ReplicationService(this.hostname, rlogs, initState))
                .build();
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

    /**
     * Our implementation of Replication Service
     */
    public static class ReplicationService extends ReplicationGrpc.ReplicationImplBase {

        private static final Map<String, String> dataStore = new HashMap<>();
        private static final List<AlreadyAccepted> leaderList = new ArrayList<>();
        private final List<Transaction> trlogs;
        private final String hostname;

        public ReplicationService(String hostname, List<Transaction> rep_logs, Collection<KV> initState) {
            this.trlogs = rep_logs;
            this.hostname = hostname;

            for (KV kv : initState) {
                this.dataStore.put(kv.getKey(), kv.getValue());
            }

            for (Transaction transaction : rep_logs) {
                for (Operation operation : transaction.getOperationList()) {
                    if (operation.getType() == Operation.Type.WRITE) {
                        this.dataStore.put(operation.getKvPair().getKey(), operation.getKvPair().getValue());
                    }
                }
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
            if (request.getSequenceId() < this.leaderList.get(this.leaderList.size() - 1).getHighestSeenSequenceId()) {
                builder.setAccepted(this.leaderList.get(this.leaderList.size() - 1));
            } else {
                AlreadyAccepted.Builder acceptBuilder = AlreadyAccepted.newBuilder();
                AlreadyAccepted.newBuilder().setHighestSeenSequenceId(request.getSequenceId()).setAcceptedCandidate(request.getCandidate());
                AlreadyAccepted alreadyAccepted = acceptBuilder.build();

                this.leaderList.add(alreadyAccepted);
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

            if ((request.getProposer() == this.leaderList.get(this.leaderList.size() - 1).getAcceptedCandidate()) &&
                    (request.getLogPosition() >= this.trlogs.size())) {
                this.trlogs.add(request.getLogPosition(), request.getTrans());

                ValueAccepted.Builder acceptValueBuilder = ValueAccepted.newBuilder();
                acceptValueBuilder.setAcceptor(hostname);
                acceptValueBuilder.setLogPosition(request.getLogPosition());

            }
        }


    }

}
