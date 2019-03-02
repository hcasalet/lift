package io.grpc.replication;

import com.google.protobuf.util.JsonFormat;
import io.grpc.transprocessing.Datastore;
import io.grpc.transprocessing.KV;
import io.grpc.transprocessing.Transaction;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.net.URL;
import java.nio.charset.Charset;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Common utilities for replication between leader and follower
 */
public class ReplicationUtil {

    /** Get the default log file from the class path */
    /*public static URL getDefaultReplicationLogFile() {
        return ReplicationServer.class.getResource("replicate_log.json");
    }*/

    /**
     * Parses the JSON input file containing the list of features.
     */
    public static List<Transaction> parseLogs(URL file) throws IOException {
        InputStream input = file.openStream();
        try {
            Reader reader = new InputStreamReader(input, Charset.forName("UTF-8"));
            try {
                ReplicationLog.Builder logfile = ReplicationLog.newBuilder();
                JsonFormat.parser().merge(reader, logfile);
                return logfile.getTransItemList();
            } finally {
                reader.close();
            }
        } finally {
            input.close();
        }
    }

}
