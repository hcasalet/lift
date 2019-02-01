package io.grpc.consensus;

import com.google.common.annotations.VisibleForTesting;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;

import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;
import com.google.protobuf.Message;

public class ReplicationLeader {

    private static final Logger logger = Logger.getLogger(ReplicationFollower.class.getName());

    private final ManagedChannel channel;
    private final ReplicationGrpc.ReplicationBlockingStub blockingStub;
    private final ReplicationGrpc.ReplicationStub asyncStub;

    private Random random = new Random();

    /** Construct a replication leader, which is taking the role of the "client" in terms of the communication. */
    public ReplicationLeader(String host, int port) {
        this(ManagedChannelBuilder.forAddress(host, port).usePlaintext());
    }

    /** Construct a replication leader for accessing a follower through the existing channel */
    public ReplicationLeader(ManagedChannelBuilder<?> channelBuilder) {
        channel = channelBuilder.build();
        blockingStub = ReplicationGrpc.newBlockingStub(channel);
        asyncStub = ReplicationGrpc.newStub(channel);
    }

    public void shutdown() throws InterruptedException {
        channel.shutdown().awaitTermination(5, TimeUnit.SECONDS);
    }

    @VisibleForTesting
    interface TestHelper {

        /** Used for verifying the incoming message received from the follower */
        void onMessage(Message message);

        /** Used for inspecting the error received from the follower. */
        void onRpcError(Throwable exception);
    }

}
