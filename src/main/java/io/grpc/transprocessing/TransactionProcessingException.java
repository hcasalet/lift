package io.grpc.transprocessing;

public class TransactionProcessingException extends Exception {

    String errorMessage;

    TransactionProcessingException(String msg) {
        this.errorMessage = msg;
    }

}
