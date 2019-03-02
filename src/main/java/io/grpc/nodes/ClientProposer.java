package io.grpc.nodes;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.replication.Peer;
import io.grpc.replication.PeerList;
import io.grpc.replication.ReplicationGrpc;
import io.grpc.replication.ReplyOfPrepared;
import io.grpc.replication.RequestToPrepare;
import io.grpc.transprocessing.KV;
import io.grpc.transprocessing.Operation;
import io.grpc.transprocessing.ProcessingResult;
import io.grpc.transprocessing.Transaction;
import io.grpc.transprocessing.TransactionProcessingGrpc;
import io.grpc.transprocessing.TransactionUtil;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.Random;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Logger;

public class ClientProposer {

    private static final Logger logger = Logger.getLogger(ClientProposer.class.getName());
    private static final int DURATION_SECONDS = 2;

    private static final int NUMBER_OF_OPERATIONS_RANGE = 10;
    private static final int OPERATION_KEY_RANDOM_RANGE = 1000;
    private static final Random random = new Random();
    private static int submissionSerialNumber = 1;

    private final String clientname;
    private final PeerList serverList;
    private final List<ChannelStubs> channelStubList = new ArrayList<>();

    /** construct a client that is a transaction processing client, as well as
     *  a replication proposer to become a leader
     */
    public ClientProposer(String clientHostname, PeerList serverList) {

        this.clientname = clientHostname;
        this.serverList = serverList;

        for (Peer peer : serverList.getPeerList()) {
            ManagedChannel channel = ManagedChannelBuilder.forAddress(peer.getHostIp(), peer.getPort()).usePlaintext().build();
            this.channelStubList.add(new ChannelStubs(peer.getHostIp(),
                    peer.getPort(),
                    channel,
                    TransactionProcessingGrpc.newBlockingStub(channel),
                    TransactionProcessingGrpc.newStub(channel),
                    ReplicationGrpc.newBlockingStub(channel),
                    ReplicationGrpc.newStub(channel)));
        }
    }

    /** shutting down the client */
    public void shutdown() throws InterruptedException {
        for (ChannelStubs stub : this.channelStubList) {
            stub.channel.shutdown().awaitTermination(5, TimeUnit.SECONDS);
        }
    }

    private class ChannelStubs {
        private final String serverIp;
        private final int serverPort;
        private final ManagedChannel channel;

        private final TransactionProcessingGrpc.TransactionProcessingBlockingStub transProcessingBlockingStub;
        private final TransactionProcessingGrpc.TransactionProcessingStub transProcessingAsyncStub;
        private final ReplicationGrpc.ReplicationBlockingStub replicationBlockingStub;
        private final ReplicationGrpc.ReplicationStub replicationStub;

        ChannelStubs(String serverIp,
                     int serverPort,
                     ManagedChannel channel,
                     TransactionProcessingGrpc.TransactionProcessingBlockingStub transProcessingBlockingStub,
                     TransactionProcessingGrpc.TransactionProcessingStub transProcessingAsyncStub,
                     ReplicationGrpc.ReplicationBlockingStub replicationBlockingStub,
                     ReplicationGrpc.ReplicationStub replicationStub) {
            this.serverIp = serverIp;
            this.serverPort = serverPort;
            this.channel = channel;
            this.transProcessingBlockingStub = transProcessingBlockingStub;
            this.transProcessingAsyncStub = transProcessingAsyncStub;
            this.replicationBlockingStub = replicationBlockingStub;
            this.replicationStub = replicationStub;
        }
    }

    /**
     * Client does its work until {@code done.get()} returns true
     */
    void doWork(AtomicBoolean done) {

        while (!done.get()) {

            /** assert that an input of servers was given to the client */
            if (serverList.getPeerCount() == 0) {
                throw new RuntimeException("This client was not given any knowledge about servers!");
            }

            if (!serverList.getPeerList().get(0).getIsLeader()) {
                proposeToBecomeLeader();
            }

            //submitTransaction(prepareTransaction());
        }
    }

    /**
     * If finding no leader in the system, propose itself
     */
    void proposeToBecomeLeader() {

        int channelToUse = -1;
        for (int i=0; i < this.channelStubList.size(); i++) {
            ChannelStubs stub = this.channelStubList.get(i);
            if (stub.serverIp.concat("-").concat(Integer.toString(stub.serverPort)).equals(this.clientname)) {
                continue;
            }
            ReplyOfPrepared electResult = electLeader(stub);
        }


    }

    /**
     * A leader sends out request to be elected
     */
    public ReplyOfPrepared electLeader(ChannelStubs channelStub) {
        RequestToPrepare.Builder requestBuilder = RequestToPrepare.newBuilder();
        requestBuilder.setCandidate(this.clientname);
        long currentTime = System.currentTimeMillis();
        requestBuilder.setSequenceId(currentTime);
        RequestToPrepare request = requestBuilder.build();

        ReplyOfPrepared reply = channelStub.replicationBlockingStub.electLeader(request);
        if ((reply.getAccepted().getHighestSeenSequenceId() == request.getSequenceId()) &&
                (reply.getAccepted().getAcceptedCandidate() == request.getCandidate())) {
            String votingNode = reply.getVoter();
            logger.info("Node " + votingNode + "voted for " + this.clientname);
        }
        return reply;
    }

    /**
     * Blocking unary call. Submits a transaction
     */
    public void submitTransaction(Transaction trans, ChannelStubs channelStub) {
        logger.info("Submitting client transaction id = " + trans.getTransactionID());

        ProcessingResult result;
        try {
            result = channelStub.transProcessingBlockingStub.submitTransaction(trans);
            System.out.println("Transaction submitted\n");

            /*if (testHelper != null) {
                testHelper.onMessage(result);
            } */

            TransactionUtil.inspectProcessingResult(result, trans);
        } catch (Exception e) {
            logger.info("RPC failed: "+e.getMessage());
        }
    }

    /**
     * Prepare a random transaction
     */
    private Transaction prepareTransaction() {

        /** Start a transaction with setting its ID and the submission time */
        Transaction.Builder transBuilder = Transaction.newBuilder();
        transBuilder.setTransactionID(this.clientname.concat(Integer.toString(submissionSerialNumber++)));

        Calendar calendar = Calendar.getInstance();
        calendar.setTime(new Date());
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");
        transBuilder.setSubmissionTime(calendar.getTime().toString());

        int numberOfOperations = random.nextInt(NUMBER_OF_OPERATIONS_RANGE);
        for (int i=0; i<numberOfOperations; i++) {
            Operation operation;
            int command = random.nextInt(4);
            if (command == 0) {

                int newKeyInt = random.nextInt(OPERATION_KEY_RANDOM_RANGE);
                String newKey = Integer.toString(newKeyInt);
                String newValue = Integer.toString(newKeyInt + 1000);
                operation = makeCreateOperation(newKey, newValue);

            } else if (command == 1) {

                String searchKey = Integer.toString(random.nextInt(OPERATION_KEY_RANDOM_RANGE));
                operation = makeRetrieveOperation(searchKey);

            } else if (command == 2) {

                int updateKeyInt = random.nextInt(OPERATION_KEY_RANDOM_RANGE);
                String updateKey = Integer.toString(updateKeyInt);
                String updateValue = Integer.toString(updateKeyInt + 20000);
                operation = makeUpdateOperation(updateKey, updateValue);

            } else if (command == 3) {

                String deleteKey = Integer.toString(random.nextInt(OPERATION_KEY_RANDOM_RANGE));
                operation = makeDeleteOperation(deleteKey);

            } else {
                throw new AssertionError("Command is not one of the CRUD operations in prepareTransaction.");
            }
            transBuilder.addOperation(operation);
        }

        return transBuilder.build();
    }

    /** Make an operation that creates a key/value pair */
    private Operation makeCreateOperation(String newKey, String newValue) {
        KV kvPair = KV.newBuilder().setKey(newKey).setValue(newValue).build();
        Operation operation = Operation.newBuilder().setType(Operation.Type.WRITE).setKvPair(kvPair).build();
        return operation;
    }

    /** Make an operation that reads the value of a key/value pair */
    private Operation makeRetrieveOperation(String searchKey) {
        KV kvPair = KV.newBuilder().setKey(searchKey).build();
        Operation operation = Operation.newBuilder().setType(Operation.Type.READ).setKvPair(kvPair).build();
        return operation;
    }

    /** Make an operation that replaces the value of an existing key/value pair */
    private Operation makeUpdateOperation(String updateKey, String updateValue) {
        KV kvPair = KV.newBuilder().setKey(updateKey).setValue(updateValue).build();
        Operation operation = Operation.newBuilder().setType(Operation.Type.WRITE).setKvPair(kvPair).build();
        return operation;
    }

    /** Make an operation that deletes a key/value pair */
    private Operation makeDeleteOperation(String deleteKey) {
        KV kvPair = KV.newBuilder().setKey(deleteKey).build();
        Operation operation = Operation.newBuilder().setType(Operation.Type.WRITE).setKvPair(kvPair).build();
        return operation;
    }

    /**
     * main to start clients
     */
    public static void main(String[] args) {

        /** get the server info from the command line */
        PeerList.Builder peerList = PeerList.newBuilder();
        for (int i=2, j=1; i < Integer.parseInt(args[1])*2 + 1; i++) {
            Peer.Builder peer = Peer.newBuilder();
            peer.setHostIp(args[i]).setPort(Integer.parseInt(args[++i])).setLastHeard(System.currentTimeMillis()).setIsLeader(false);
            peerList.setPeer(j++, peer);
        }
        ClientProposer clientProposer = new ClientProposer(args[0], peerList.build());

        AtomicBoolean done = new AtomicBoolean();
        ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
        scheduler.schedule(() -> done.set(true), DURATION_SECONDS, TimeUnit.MILLISECONDS);
        clientProposer.doWork(done);
        System.out.println("Job all done!\n");
    }

}
