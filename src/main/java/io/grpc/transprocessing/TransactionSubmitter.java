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

/**
 * TransactionSubmitter belongs to the application layer, so it is also
 * an independent entity.
 *
 * To invoke a transactionSubmitter, do:
 * % trans-submitter [serverIP] [serverPort[ [clientIP-clientPort]
 *                   [minimum_number_of_operations] [range_of_number_of_operations]
 *                   [transaction_submission_duration]
 */
public class TransactionSubmitter {

    private static final Logger logger = Logger.getLogger(TransactionSubmitter.class.getName());
    private static final int EXISTING_MAX_KEY = 201;

    private final ManagedChannel channel;
    private final String submitterName;
    private final TransactionProcessingGrpc.TransactionProcessingBlockingStub transProcessingBlockingStub;
    private final TransactionProcessingGrpc.TransactionProcessingStub transProcessingAsyncStub;

    private final int minimum_number_of_operations;
    private final int range_of_number_of_operations;
    private final long transaction_submission_duration;

    private Random random = new Random();
    private TestHelper testHelper;
    private int submissionSerialNumber = 1;

    /** Construct client for submitting transaction request to transaction processor at @code host:port}. */
    public TransactionSubmitter(String serverHost, int serverPort, String clientHostname,
                                int minimum_number_of_operations,
                                int range_of_number_of_operations,
                                int transaction_submission_duration) {
        this(ManagedChannelBuilder.forAddress(serverHost, serverPort).usePlaintext(), clientHostname,
                minimum_number_of_operations, range_of_number_of_operations, transaction_submission_duration);
    }

    /** Construct client for contacting the server using the existing channel. */
    public TransactionSubmitter(ManagedChannelBuilder<?> channelBuilder, String clientHostname,
                                int minimum_number_of_operations,
                                int range_of_number_of_operations,
                                int transaction_submission_duration) {
        this.channel = channelBuilder.build();
        this.submitterName = clientHostname;
        this.transProcessingBlockingStub = TransactionProcessingGrpc.newBlockingStub(channel);
        this.transProcessingAsyncStub = TransactionProcessingGrpc.newStub(channel);

        this.minimum_number_of_operations = minimum_number_of_operations;
        this.range_of_number_of_operations = range_of_number_of_operations;
        this.transaction_submission_duration = transaction_submission_duration;;
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

        int numberOfOperations = random.nextInt(this.range_of_number_of_operations) + this.minimum_number_of_operations;
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

        TransactionSubmitter transactionSubmitter = new TransactionSubmitter(args[0], Integer.parseInt(args[1]), args[2],
                                                                             Integer.parseInt(args[3]),
                                                                             Integer.parseInt(args[4]),
                                                                             Integer.parseInt(args[5]));
        AtomicBoolean done = new AtomicBoolean();

        ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
        scheduler.schedule(() -> done.set(true), transactionSubmitter.transaction_submission_duration, TimeUnit.MILLISECONDS);
        long timeStart = System.currentTimeMillis();
        transactionSubmitter.doSubmissions(done);
        long timeEnd = System.currentTimeMillis();
        System.out.println("All job done in" + (timeEnd - timeStart) + "\n");
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
