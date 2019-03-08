package io.grpc.replication;

import com.google.common.annotations.VisibleForTesting;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;

import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;
import com.google.protobuf.Message;
import io.grpc.netty.shaded.io.netty.util.internal.StringUtil;
import io.grpc.transprocessing.Transaction;

/**
 * Replication Client is embedded in a transaction processor.
 * It does not require a main since it is not an independent entity.
 */
public class ReplicationClient {

    private static final Logger logger = Logger.getLogger(ReplicationServer.class.getName());
    private static final String WELCOME_2_JOIN = "Welcome to join!";
    private static int logPosition = 1;

    private final ManagedChannel channel;
    private final ReplicationGrpc.ReplicationBlockingStub blockingStub;
    private final ReplicationGrpc.ReplicationStub asyncStub;
    private final String hostname;

    private final String serverHost;
    private final int serverPort;

    private Random random = new Random();

    /** Construct a replication leader, which is taking the role of the "client" in terms of the communication. */
    public ReplicationClient(String peerHost, int peerPort, String leaderName) {
        this(ManagedChannelBuilder.forAddress(peerHost, peerPort).usePlaintext(), peerHost, peerPort, leaderName);
    }

    /** Construct a replication leader for accessing a follower through the existing channel */
    public ReplicationClient(ManagedChannelBuilder<?> channelBuilder, String peerHost, int peerPort, String leaderName) {
        channel = channelBuilder.build();
        blockingStub = ReplicationGrpc.newBlockingStub(channel);
        asyncStub = ReplicationGrpc.newStub(channel);
        hostname = leaderName;
        this.serverHost = peerHost;
        this.serverPort = peerPort;
    }

    public void shutdown() throws InterruptedException {
        channel.shutdown().awaitTermination(5, TimeUnit.SECONDS);
    }

    /**
     * When joining the system, register the replication service on the host
     * to other replics in the system
     */
    public String handShaking(LocalIdentity myIdentity) {
        Welcome welcomeMsg = this.blockingStub.handShaking(myIdentity);
        if (welcomeMsg.getWelcome2JoinMessage().contains(WELCOME_2_JOIN)) {
            return welcomeMsg.getReplyingHostIp();
        } else {
            return StringUtil.EMPTY_STRING;
        }
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

    public String getServerHost() {
        return this.serverHost;
    }

    public int getServerPort() {
        return this.serverPort;
    }

    @VisibleForTesting
    interface TestHelper {

        /** Used for verifying the incoming message received from the follower */
        void onMessage(Message message);

        /** Used for inspecting the error received from the follower. */
        void onRpcError(Throwable exception);
    }

}
