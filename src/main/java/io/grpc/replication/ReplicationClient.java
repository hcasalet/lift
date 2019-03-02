package io.grpc.replication;

import com.google.common.annotations.VisibleForTesting;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;

import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;
import com.google.protobuf.Message;
import io.grpc.transprocessing.Transaction;

public class ReplicationClient {

    private static final Logger logger = Logger.getLogger(ReplicationServer.class.getName());
    private static int logPosition = 1;

    private final ManagedChannel channel;
    private final ReplicationGrpc.ReplicationBlockingStub blockingStub;
    private final ReplicationGrpc.ReplicationStub asyncStub;
    private final String hostname;

    private Random random = new Random();

    /** Construct a replication leader, which is taking the role of the "client" in terms of the communication. */
    public ReplicationClient(String peerHost, int peerPort, String leaderName) {
        this(ManagedChannelBuilder.forAddress(peerHost, peerPort).usePlaintext(), leaderName);
    }

    /** Construct a replication leader for accessing a follower through the existing channel */
    public ReplicationClient(ManagedChannelBuilder<?> channelBuilder, String leaderName) {
        channel = channelBuilder.build();
        blockingStub = ReplicationGrpc.newBlockingStub(channel);
        asyncStub = ReplicationGrpc.newStub(channel);
        hostname = leaderName;
    }

    public void shutdown() throws InterruptedException {
        channel.shutdown().awaitTermination(5, TimeUnit.SECONDS);
    }

    /**
     * A leader sends out request to be elected
     */
    public void electLeader() {
        RequestToPrepare.Builder requestBuilder = RequestToPrepare.newBuilder();
        requestBuilder.setCandidate(hostname);
        long currentTime = System.currentTimeMillis();
        requestBuilder.setSequenceId(currentTime);
        RequestToPrepare request = requestBuilder.build();

        ReplyOfPrepared reply = this.blockingStub.electLeader(request);
        if ((reply.getAccepted().getHighestSeenSequenceId() == request.getSequenceId()) &&
                (reply.getAccepted().getAcceptedCandidate() == request.getCandidate())) {
            String votingNode = reply.getVoter();
            logger.info("Node " + votingNode + "voted for " + hostname);
        }
    }

    /**
     * Request for replication
     */
    public void proposeValue(Transaction transaction) {
        ValueProposed.Builder valueBuilder = ValueProposed.newBuilder();
        valueBuilder.setLogPosition(logPosition++).setTrans(transaction).setProposer(this.hostname);

        this.blockingStub.proposeValue(valueBuilder.build());
    }

    /**
     * main to start a leader
     */
    public static void main(String[] args) {
        ReplicationClient replicationClient = new ReplicationClient(args[0], Integer.parseInt(args[1]), args[2]);
        replicationClient.electLeader();
    }

    @VisibleForTesting
    interface TestHelper {

        /** Used for verifying the incoming message received from the follower */
        void onMessage(Message message);

        /** Used for inspecting the error received from the follower. */
        void onRpcError(Throwable exception);
    }

}
