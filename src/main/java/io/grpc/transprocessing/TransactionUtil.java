package io.grpc.transprocessing;

import com.google.protobuf.util.JsonFormat;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.net.URL;
import java.nio.charset.Charset;
import java.util.List;
import java.util.logging.Logger;

public class TransactionUtil {

    private static final Logger logger = Logger.getLogger(TransactionUtil.class.getName());

    /** Get the existing data file */
    public static URL getExistingDataFile() {
        return TransactionProcessor.class.getResource("kvstore.json");
    }

    /** Parse the input JSON file containing the existing data items */
    public static List<KV> parseData(URL file) throws IOException {
        System.out.println("The file is: " + file.toString());
        InputStream input = file.openStream();

        try {
            Reader reader = new InputStreamReader(input, Charset.forName("UTF-8"));
            try {
                Datastore.Builder datastore = Datastore.newBuilder();
                JsonFormat.parser().merge(reader, datastore);
                return datastore.getKVList();
            } finally {
                reader.close();
            }
        } finally {
            input.close();
        }
    }

    /** Inspect the processing result for client */
    public static void inspectProcessingResult(ProcessingResult result, Transaction trans) {
        if (result.getResultType() == ProcessingResult.Type.COMMIT) {
            logger.info("Transaction id = " + trans.getTransactionID() + "committed.");
        } else if (result.getResultType() == ProcessingResult.Type.ABORT) {
            logger.info("Transaction id = " + trans.getTransactionID() + "needs re-submission.");
        } else if (result.getResultType() == ProcessingResult.Type.UNEXPECTED) {
            logger.info("Server encountered severe consistency issue. ");
        }
    }
}
