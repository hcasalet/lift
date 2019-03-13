package io.grpc.transprocessing;

import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.StatusRuntimeException;
import io.grpc.replication.LocalIdentity;
import io.grpc.replication.ReplicationClient;
import io.grpc.replication.ReplicationServer;
import io.grpc.stub.StreamObserver;

import java.io.IOException;
import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.URL;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

/**
 * TransactionProcessor is also a replicationClient.
 * To invoke a transactionProcessor, do:
 *
 * % trans-processor <transprocessingOrReplication> <port> <hostIP1> <port1> .... <hostIPn> <portn>
 *     transprocessingOrReplication: boolean
 *     port: the port where the processor is run
 *     hostIP and port pairs are for the replication servers.
 */
public class TransactionProcessor {
    private static final Logger logger = Logger.getLogger(TransactionProcessor.class.getName());
    private static final Map<String, String> dataStore = new HashMap<>();

    private final int port;
    private final Server server;
    private List<ReplicationClient> replicationClients = new ArrayList<>();

    public TransactionProcessor(int port, List<String> replicationServers, boolean leader) throws IOException {
        this(port, TransactionUtil.getExistingDataFile(), replicationServers, leader);
    }

    /** create a transaction processing server listening on {@code port} using {@code dataFile} */
    public TransactionProcessor(int port, URL dataFile, List<String> replicationServers, boolean leader) throws IOException {
        this(ServerBuilder.forPort(port), port, TransactionUtil.parseData(dataFile), replicationServers, leader);
    }

    /** Create a transaction processing server using serverBuilder as a base and key-value pair as data. */
    public TransactionProcessor(ServerBuilder<?> serverBuilder, int port, Collection<KV> kvPairs, List<String> replicationServers, boolean leader)
    throws UnknownHostException {
        this.port = port;
        for (String repServer : replicationServers) {
            String repServerHostIp = repServer.substring(0, repServer.lastIndexOf('/'));
            int repServerHostPort = Integer.parseInt(repServer.substring(repServer.lastIndexOf('/')+1));
            this.replicationClients.add(new ReplicationClient(repServerHostIp, repServerHostPort, repServer));
        }

       if (leader) {
            server = serverBuilder.addService(new TransactionProcessingService(kvPairs, this.dataStore, this.replicationClients))
                    .addService(new ReplicationServer.ReplicationService(InetAddress.getLocalHost().toString(), new ArrayList<KV>(), this.dataStore, this.replicationClients))
                    .build();
       } else {
            server = serverBuilder.addService(new TransactionProcessingService(new ArrayList<KV>(), this.dataStore, this.replicationClients))
                    .addService(new ReplicationServer.ReplicationService(InetAddress.getLocalHost().toString(), kvPairs, this.dataStore, this.replicationClients))
                    .build();
       }
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
        List<String> replicationServers = new ArrayList<>();

        for (int i=2; i < args.length; i++) {
            replicationServers.add(args[i].concat("/").concat(args[++i]));
        }
        TransactionProcessor server = new TransactionProcessor(Integer.parseInt(args[1]), replicationServers, Boolean.parseBoolean(args[0]));
        server.start();

        for(ReplicationClient client : server.replicationClients) {
            LocalIdentity.Builder identityBuilder = LocalIdentity.newBuilder();
            String localHost = Inet4Address.getLocalHost().toString();
            identityBuilder.setLocalhostIp(localHost.substring(localHost.lastIndexOf("/")+1)).setListeningPort(server.port);
            String otherReplicaIp = client.handShaking(identityBuilder.build());
            if (otherReplicaIp.isEmpty()) {
                logger.info("Registering at " + client.getServerHost() + "/" + client.getServerPort() + "failed");
            }
        }
        server.blockUntilShutdown();
    }

    /**
     * My implementation of Transaction Processing service.
     *
     * <p>See transprocessing.proto for details of the methods.
     */
    public static class TransactionProcessingService extends TransactionProcessingGrpc.TransactionProcessingImplBase {

        private static final long SLEEP_BEFORE_INQUIRING_AGAIN = 10;
        private static final long MAXIMUM_NUMBER_OF_TRIES_ALLOWED = 3;
        private static int logPosition = 1;

        private final Collection<KV> kvPairs;
        private final static ConcurrentMap<String, List<Lock>> readWriteLocks = new ConcurrentHashMap<>();
        private Map<String, String> datastore;
        private List<ReplicationClient> replicationClients;

        public TransactionProcessingService(Collection<KV> kvPairs, Map<String, String> dataStore, List<ReplicationClient> replicationClients) {
            this.kvPairs = kvPairs;
            this.datastore = dataStore;
            for (KV kvPair : kvPairs) {
                this.datastore.put(kvPair.getKey(), kvPair.getValue());
            }
            this.replicationClients = replicationClients;
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
                    doReplication(request, logPosition++);
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
                        String readV = this.datastore.get(operation.getKvPair().getKey());
                        doOneOperation(operation, Operation.Type.READ, readV, builder, i++);
                        break;
                    case WRITE:
                        this.datastore.put(operation.getKvPair().getKey(), operation.getKvPair().getValue());
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

        private void doReplication(Transaction trans, int logPos) {
            List<ReplicationClient> toBeRemovedClients = new ArrayList<>();
            for (ReplicationClient client : this.replicationClients) {
                try {
                    client.proposeValue(trans, logPos);
                } catch (Exception exp) {
                    // If server is gone
                    if ((exp instanceof StatusRuntimeException) &&
                            (exp.getMessage().contains("UNAVAILABLE: io exception"))) {
                        logger.info("Server " + client.getServerHost() + "/" + client.getServerPort() + "is gone, removing client to it...");
                        toBeRemovedClients.add(client);
                    }
                }
            }

            /**
             * Handles if a server replica has left the system.
             */
            if (!toBeRemovedClients.isEmpty()) {
                for (ReplicationClient client : toBeRemovedClients) {
                    this.replicationClients.remove(client);
                }
            }
        }
    }

}
