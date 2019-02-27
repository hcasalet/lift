package io.grpc.transprocessing;

import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.replication.ReplicationFollower;
import io.grpc.replication.ReplicationUtil;
import io.grpc.stub.StreamObserver;

import java.io.IOException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

public class TransactionProcessor {
    private static final Logger logger = Logger.getLogger(TransactionProcessor.class.getName());

    private final int port;
    private final Server server;
    private final String hostname;

    public TransactionProcessor(int port, String hostName) throws IOException {
        this(port, hostName, TransactionUtil.getExistingDataFile(), ReplicationUtil.getDefaultReplicationLogFile());
    }

    /** create a transaction processing server listening on {@code port} using {@code dataFile} */
    public TransactionProcessor(int port, String hostName, URL dataFile, URL replicationLogFile) throws IOException {
        this(ServerBuilder.forPort(port), port, hostName, TransactionUtil.parseData(dataFile), ReplicationUtil.parseLogs(replicationLogFile));
    }

    /** Create a transaction processing server using serverBuilder as a base and key-value pair as data. */
    public TransactionProcessor(ServerBuilder<?> serverBuilder, int port, String hostName, Collection<KV> kvPairs,
                                List<Transaction> rlogs) {
        this.port = port;
        this.hostname = hostName;
        server = serverBuilder.addService(new TransactionProcessingService(kvPairs)).addService(
                new ReplicationFollower.ReplicationService(hostName, rlogs, kvPairs)).build();
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
                TransactionProcessor.this.stop();
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
        TransactionProcessor server = new TransactionProcessor(8980, "localhost");
        server.start();
        server.blockUntilShutdown();
    }

    /**
     * My implementation of Transaction Processing service.
     *
     * <p>See transprocessing.proto for details of the methods.
     */
    private static class TransactionProcessingService extends TransactionProcessingGrpc.TransactionProcessingImplBase {

        private static final long SLEEP_BEFORE_INQUIRING_AGAIN = 10;
        private static final long MAXIMUM_NUMBER_OF_TRIES_ALLOWED = 3;

        private final Collection<KV> kvPairs;
        private final static Map<String, String> dataStore = new HashMap<>();
        private final static ConcurrentMap<String, List<Lock>> readWriteLocks = new ConcurrentHashMap<>();

        TransactionProcessingService(Collection<KV> kvPairs) {
            this.kvPairs = kvPairs;
            for (KV kvPair : kvPairs) {
                this.dataStore.put(kvPair.getKey(), kvPair.getValue());
            }
        }

        @Override
        public void submitTransaction(Transaction request, StreamObserver<ProcessingResult> responseObserver) {
            responseObserver.onNext(processTransaction(request));
            responseObserver.onCompleted();
            return;
        }

        public ProcessingResult processTransaction(Transaction request) {
            /** Builder for building up the response */
            ProcessingResult.Builder builder = ProcessingResult.newBuilder();
            builder.setTransactionID(request.getTransactionID());

            try {
                if (acquireLocks(request)) {
                    doOperations(request.getOperationList(), builder);
                    releaseLocks(request.getOperationList());
                    builder.setResultType(ProcessingResult.Type.COMMIT).setResultMessage("Transaction successfully committed.");
                    logger.info("Transaction " + request.getTransactionID() + " finished successfully.");
                } else {
                    /** Could not acquire the needed locks. Transaction aborted. Has to resubmit later. */
                    builder.setResultType(ProcessingResult.Type.ABORT).setResultMessage(
                            "Transaction on hold for acquiring locks. Please re-submit later.");
                    logger.info("Transaction " + request.getTransactionID() + " failed to acquire locks to proceed.");

                }
            } catch (TransactionProcessingException exp) {
                builder.setResultType(ProcessingResult.Type.UNEXPECTED).setResultMessage("Exception occurred and transaction canceled.");
                logger.warning("Transaction " + request.getTransactionID() + " ran into inconsistent situation with locks.");
            }

            return builder.build();
        }

        /**
         * To acquire the locks required to execute the transaction. It has two parts. The
         * first part checks if getting locks for all operations of the transaction is
         * achievable; if not, it waits 10 milliseconds and retries for 3 times. The second
         * part actually reserves those locks.
         * @param request
         * @return boolean value whether locks are all granted or not.
         */
        private synchronized boolean acquireLocks(Transaction request) throws TransactionProcessingException {

            /**
             * Checks if it can acquire locks within reasonable number of tries. Having a testLocks step
             * is necessary to help prevent reserving some of the locks and then find that cannot reserve
             * all of the locks and thus having to rollback all the lock reservations.
             * */
            synchronized(readWriteLocks) {
                if (!testLocks(request)) {
                    return false;
                }

                /** Get the locks now */
                for (Operation operation : request.getOperationList()) {
                    String datem = operation.getKvPair().getKey();
                    Lock obtainedLock = Lock.newBuilder().setTransactionID(request.getTransactionID())
                            .setKey(datem).setType(operation.getType()).build();

                    if (readWriteLocks.get(datem) != null) {
                        readWriteLocks.get(datem).add(obtainedLock);
                    } else {
                        List<Lock> newLockList = new ArrayList<>();
                        newLockList.add(obtainedLock);
                        readWriteLocks.put(datem, newLockList);
                    }
                }
            }

            return true;
        }

        private boolean testLocks(Transaction transaction) {

            boolean keepTrying = true;
            int timesTried = 0;
            int operationsGranted = 0;
            List<Operation> operationList = transaction.getOperationList();
            int totalOperations = operationList.size();
            String transactionID = transaction.getTransactionID();

            while (keepTrying && (timesTried < MAXIMUM_NUMBER_OF_TRIES_ALLOWED)) {

                /** Locks are not granted from the previous try, so we sleep and try again. */
                if (timesTried > 0) {
                    wait4Locks(SLEEP_BEFORE_INQUIRING_AGAIN);
                }

                timesTried++;
                operationsGranted = 0;
                System.out.println("Before checking locks, lock map should be empty : " + readWriteLocks.size());

                for (Operation operation : operationList) {

                    String datem = operation.getKvPair().getKey();

                    List<Lock> lockList = readWriteLocks.get(datem);

                    if (lockList != null) { /** lock found on the data item */
                        if ((lockList.get(0).getTransactionID() != transactionID)
                                && ((lockList.get(0).getType() == Operation.Type.WRITE) ||
                                (operation.getType() == Operation.Type.WRITE))) {
                            /**
                             * If there is a write lock on either end, the transaction needs to wait.
                             * */
                            break;
                        }
                    }
                    operationsGranted++;
                }

                /** If all operations in the transaction have been granted locks, we need to try no more. */
                if (operationsGranted >= totalOperations) {
                    keepTrying = false;
                }
            }

            return (!keepTrying);
        }

        /** Sleep before checking for locks after running into a conflict */
        private static void wait4Locks(long millis) {
            try {
                TimeUnit.MILLISECONDS.sleep(millis);
            } catch (InterruptedException e) {

            }
        }

        /**
         * Executing operations one by one in the transaction. While executing the operations also
         * build the ProcessingResult response.
         * @param operations
         * @param builder
         */
        private void doOperations(List<Operation> operations, ProcessingResult.Builder builder) {

            int i = 0;
            for (Operation operation : operations) {
                switch(operation.getType()) {
                    case READ:
                        String readV = this.dataStore.get(operation.getKvPair().getKey());
                        doOneOperation(operation, Operation.Type.READ, readV, builder, i++);
                        break;
                    case READWRITE:
                        /**
                         * If read returns a value, the updated value is the oldV + Value_in_KV_pair;
                         * otherwise readwrite resorts to the same as a pure write.
                         */
                        String oldV = this.dataStore.get(operation.getKvPair().getKey());
                        if (oldV != null) {
                            this.dataStore.put(operation.getKvPair().getKey(), oldV + operation.getKvPair().getValue());
                            doOneOperation(operation, Operation.Type.READWRITE,
                                    oldV.concat(":old, ").concat(operation.getKvPair().getValue()).concat(":new"), builder, i++);
                            break;
                        }
                    case WRITE:
                        this.dataStore.put(operation.getKvPair().getKey(), operation.getKvPair().getValue());
                        doOneOperation(operation, operation.getType(), operation.getKvPair().getValue(), builder, i++);
                        break;

                }
            }
        }

        /** Building the execution details for one operation in ProcessingResult */
        private void doOneOperation(Operation operation, Operation.Type operType, String updatedValue,
                                    ProcessingResult.Builder builder, int ind) {
            OperationExecution.Builder operBuilder = OperationExecution.newBuilder();
            operBuilder.setType(operType);
            operBuilder.setKey(operation.getKvPair().getKey());
            if (updatedValue != null) {
                operBuilder.setValue(updatedValue);
            }
            builder.addOperExec(operBuilder.build());
        }

        /** Release locks after the operations in the transaction are done. */
        private void releaseLocks(List<Operation> operations) throws TransactionProcessingException {

            synchronized(readWriteLocks) {
                System.out.println("Releasing locks: lock size : " + readWriteLocks.size() + ", Operations :" + operations.size());
                for (Operation operation : operations) {
                    List<Lock> locks = readWriteLocks.get(operation.getKvPair().getKey());
                    if (locks.size() == 0) {
                        /**
                         * Case when no locks found to be released. This is an error case.
                         */
                        throw new TransactionProcessingException(String.format(
                                "The finished operation found no lock to release on date item %s.",
                                operation.getKvPair().getKey()));
                    } else if (locks.size() == 1) {
                        /**
                         * Case to remove the entire lock item when only one lock is on the data item
                         */
                        readWriteLocks.remove(operation.getKvPair().getKey());
                    } else {
                        /**
                         * case to remove a read lock from the lock list
                         */
                        for (Lock lock: locks) {
                            if (lock.getType() == operation.getType()) {
                                readWriteLocks.get(operation.getKvPair().getKey()).remove(lock);
                                break;
                            }
                        }
                    }
                }
            }
        }
    }

}
