package io.grpc.transprocessing;

import com.google.common.annotations.VisibleForTesting;
import com.google.protobuf.Message;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;

import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.Random;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Logger;

public class TransactionSubmitter {

    private static final Logger logger = Logger.getLogger(TransactionSubmitter.class.getName());
    private static final int EXISTING_MAX_KEY = 201;
    private static final int MINIMUM_NUMBER_OF_OPERATIONS = 5;
    private static final int NUMBER_OF_OPERATIONS_SEED = 11;
    private static final long DURATION_SECONDS = 800;

    private final ManagedChannel channel;
    private final String submitterName;
    private final TransactionProcessingGrpc.TransactionProcessingBlockingStub transProcessingBlockingStub;
    private final TransactionProcessingGrpc.TransactionProcessingStub transProcessingAsyncStub;

    private Random random = new Random();
    private TestHelper testHelper;
    private int submissionSerialNumber = 1;

    /** Construct client for submitting transaction request to transaction processor at @code host:port}. */
    public TransactionSubmitter(String serverHost, int serverPort, String clientHostname) {
        this(ManagedChannelBuilder.forAddress(serverHost, serverPort).usePlaintext(), clientHostname);
    }

    /** Construct client for contacting the server using the existing channel. */
    public TransactionSubmitter(ManagedChannelBuilder<?> channelBuilder, String clientHostname) {
        this.channel = channelBuilder.build();
        this.submitterName = clientHostname;
        this.transProcessingBlockingStub = TransactionProcessingGrpc.newBlockingStub(channel);
        this.transProcessingAsyncStub = TransactionProcessingGrpc.newStub(channel);
    }

    public void shutdown() throws InterruptedException {
        channel.shutdown().awaitTermination(5, TimeUnit.SECONDS);
    }

    /**
     * Submitter does its work until {@code done.get()} returns true
     */
    void doSubmissions(AtomicBoolean done) {

        while (!done.get()) {
            submitTransaction(prepareTransaction());
        }
    }

    /**
     * Prepare a random transaction
     */
    private Transaction prepareTransaction() {

        /** Start a transaction with setting its ID and the submission time */
        Transaction.Builder transBuilder = Transaction.newBuilder();
        transBuilder.setTransactionID(this.submitterName.concat(Integer.toString(submissionSerialNumber++)));

        Calendar calendar = Calendar.getInstance();
        calendar.setTime(new Date());
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");
        transBuilder.setSubmissionTime(calendar.getTime().toString());

        int numberOfOperations = random.nextInt(NUMBER_OF_OPERATIONS_SEED) + MINIMUM_NUMBER_OF_OPERATIONS;
        for (int i=0; i<numberOfOperations; i++) {
            Operation operation;
            int command = random.nextInt(4);
            if (command == 0) {

                int newKeyInt = random.nextInt(EXISTING_MAX_KEY) + EXISTING_MAX_KEY;
                String newKey = Integer.toString(newKeyInt);
                String newValue = Integer.toString(newKeyInt + 1000);
                operation = makeCreateOperation(newKey, newValue);

            } else if (command == 1) {

                String searchKey = Integer.toString(random.nextInt(EXISTING_MAX_KEY));
                operation = makeRetrieveOperation(searchKey);

            } else if (command == 2) {

                int updateKeyInt = random.nextInt(EXISTING_MAX_KEY);
                String updateKey = Integer.toString(updateKeyInt);
                String updateValue = Integer.toString(updateKeyInt + 1000);
                operation = makeUpdateOperation(updateKey, updateValue);

            } else if (command == 3) {

                String deleteKey = Integer.toString(random.nextInt(EXISTING_MAX_KEY));
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
     * Blocking unary call. Submits a transaction
     */
    public void submitTransaction(Transaction trans) {
        logger.info("Submitting client transaction id = " + trans.getTransactionID());

        ProcessingResult result;
        try {
            result = this.transProcessingBlockingStub.submitTransaction(trans);
            System.out.println("Transaction submitted\n");

            if (testHelper != null) {
                testHelper.onMessage(result);
            }

            TransactionUtil.inspectProcessingResult(result, trans);
        } catch (Exception e) {
            logger.info("RPC failed: "+e.getMessage());
        }
    }

    /**
     * Asynchronous client streaming transaction requests.
     */

    public static void main(String[] args) throws InterruptedException {

        TransactionSubmitter transactionSubmitter = new TransactionSubmitter("localhost", 8980, args[0]);
        AtomicBoolean done = new AtomicBoolean();

        ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
        scheduler.schedule(() -> done.set(true), DURATION_SECONDS, TimeUnit.MILLISECONDS);
        transactionSubmitter.doSubmissions(done);
        System.out.println("All job done\n");
    }


    /**
     * Used for unit test, as unit test does not want to deal with randomness.
     */
    @VisibleForTesting
    void setRandom(Random random) { this.random = random; }

    @VisibleForTesting
    interface TestHelper {
        /** Used for verifying/inspecting message received from server */
        void onMessage(Message message);

        /** Used for verifying/inspecting error received from server */
        void onRpcError(Throwable exception);
    }

    @VisibleForTesting
    void setTestHelper(TestHelper testHelper) { this.testHelper = testHelper; }
}
