package com.jt;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.json.simple.JSONObject;

/**
 * This is a demonstration to reference the documents stored within HDFS and
 * store the indexes into ElasticSearch.
 * 
 * @author dewee
 * 
 */
public class ReferenceHdfs {

    private static Configuration configuration = new Configuration();
    private static FileSystem filesystem = null;
    private static Path path = null;

    public static final String GUTENBERG_HDR_END = "*END*";
    public static final String GUTENBERG_COMMENT = "*";
    public static final String BY = "by";

    private static boolean haveTitle = false;
    private static boolean haveAuthor = false;
    private static boolean foundHdr = false;

    private JSONObject jsonObject = new JSONObject();

    public static final char SLASH = '/';
    
    public void run(String directory) throws FileNotFoundException, IOException {

        // Initialize HDFS
        configuration.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
        configuration.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());
        configuration.addResource(new Path("/etc/hadoop/conf/core-site.xml"));
        configuration.addResource(new Path("/etc/hadoop/conf/hdfs-site.xml"));
        configuration.addResource(new Path("/etc/hadoop/conf/mapred-site.xml"));

        filesystem = FileSystem.get(configuration);

        String line = null;
        BufferedReader br = null;

        // Initialize ES Client
        Client client = new TransportClient().addTransportAddress(new InetSocketTransportAddress("192.168.3.140", 9300))
                .addTransportAddress(new InetSocketTransportAddress("192.168.3.141", 9300))
                .addTransportAddress(new InetSocketTransportAddress("192.168.3.142", 9300))
                .addTransportAddress(new InetSocketTransportAddress("192.168.3.143", 9300));
        
        path = new Path(directory);

        RemoteIterator<LocatedFileStatus> lfsIterator = filesystem.listFiles(path, true);
        LocatedFileStatus lfs = null;
        String parsedPath = null;
        while (lfsIterator.hasNext()) {
            lfs = lfsIterator.next();
            parsedPath = lfs.getPath().toUri().getPath();

            jsonObject.clear();
            haveAuthor = false;
            haveTitle = false;
            foundHdr = false;

            jsonObject.put("HDFSPath", parsedPath.substring(0, parsedPath.lastIndexOf(SLASH)));
            jsonObject.put("FILENAME", lfs.getPath().getName());

            try {
                br = new BufferedReader(new InputStreamReader(filesystem.open(lfs.getPath())));
                line = br.readLine();
                while (line != null) {

                    // read the gutenberg head info up to the beginning of the
                    // doc
                    if (!foundHdr || !line.trim().startsWith(GUTENBERG_HDR_END) && !line.trim().endsWith(GUTENBERG_HDR_END)) {
                        foundHdr = true;
                    }

                    // skip down to the document info
                    if (!foundHdr || line.trim().startsWith(GUTENBERG_COMMENT) || line.trim().isEmpty()) {
                        line = br.readLine();
                        continue;
                    }

                    if (!haveTitle) {
                        jsonObject.put("TITLE", line);
                        haveTitle = true;
                    }

                    if (line.startsWith(BY) && !haveAuthor) {
                        jsonObject.put("AUTHOR", line);
                        haveAuthor = true;
                    }

                    // if I have what I need, break out of the inner loop
                    if (haveTitle && haveAuthor) {
                        break;
                    }

                    // read each line until we get to the actual content
                    line = br.readLine();
                } // outer while;
                br.close();


//                System.out.println("Writing message to elasticsearch: " + jsonObject.toJSONString());

                IndexResponse response = client.prepareIndex("hadoop", "gutenberg").setSource(jsonObject.toJSONString()).execute()
                        .actionGet();

                System.out.println("Response Info");
                System.out.println("  ID created by es: " + response.getId());
                System.out.println("  Index create by es: " + response.getIndex());
                System.out.println("  Type: " + response.getType());
                System.out.println("  Version: " + response.getVersion() + "\n\n");

            } catch (Exception e) {
                e.printStackTrace();
                // done with file;
            }

        }
        client.close();

    }

    public static void main(String args[]) {
        if (args.length < 1) {
            System.err.println("Problem here.  Syntax requires an hdfs path.");
        }

        ReferenceHdfs rh = new ReferenceHdfs();
        try {
            rh.run(args[0]);
        } catch (FileNotFoundException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }
}
