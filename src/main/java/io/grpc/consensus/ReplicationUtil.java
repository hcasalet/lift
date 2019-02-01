package io.grpc.consensus;

import com.google.protobuf.util.JsonFormat;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.net.URL;
import java.nio.charset.Charset;
import java.util.List;

/**
 * Common utilities for consensus between leader and follower
 */
public class ReplicationUtil {

    /** Get the default log file from the class path */
    public static URL getDefaultReplicationLogFile() {
        return ReplicationFollower.class.getResource("route_guide_db.json");
    }

    /**
     * Parses the JSON input file containing the list of features.
     */
    public static List<LogItem> parseLogs(URL file) throws IOException {
        InputStream input = file.openStream();
        try {
            Reader reader = new InputStreamReader(input, Charset.forName("UTF-8"));
            try {
                ReplicationLog.Builder logfile = ReplicationLog.newBuilder();
                JsonFormat.parser().merge(reader, logfile);
                return logfile.getLogItemList();
            } finally {
                reader.close();
            }
        } finally {
            input.close();
        }
    }

}
