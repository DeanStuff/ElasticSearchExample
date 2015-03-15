package com.jt;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Properties;
import java.util.logging.Logger;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
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
public class ElasticSearchIndex {
    
    private static Logger log = Logger.getLogger(ElasticSearchIndex.class.getName());


    private static Configuration configuration = new Configuration();
    private static FileSystem filesystem = null;
    private static Path path = null;

    public static final String GUTENBERG_HDR_END = "*END*";
    public static final String GUTENBERG_COMMENT = "*";
    public static final String BY = "by";
    public static final String OF = "of";
    public static final String AUTHOR = "Author";
    public static final String TITLE = "Title";
    
    public static final String FIRST_LINE_PRIMARY_TITLE_TAG = "The Project Gutenberg Etext of";
    public static final String FIRST_LINE_SECONDARY_TITLE_TAG = "Project Gutenberg Etext";
    
    public static final char SLASH_CHAR = '/';
    public static final char COLON_CHAR = ':';

    private static boolean haveTitle = false;
    private static boolean haveAuthor = false;
    private static boolean foundHdr = false;
    private static boolean firstLine = false;

    private JSONObject jsonObject = new JSONObject();

    
    private static Properties prop = null;
    
    private void loadProperties() {
        String propFileName = "config.properties";  // static property file included in the jar.  Excluded here for privacy
 
        InputStream inputStream = getClass().getClassLoader().getResourceAsStream(propFileName);
        if (inputStream != null) {
            prop = new Properties();
            try {
                prop.load(inputStream);
            } catch (IOException e) {
                log.info("Problem loading property file: " + propFileName + ".  Exception caught: " + e.getMessage());
            }
        }
        
    }
    
    public void run(String directory) throws FileNotFoundException, IOException {

        loadProperties();
        
        // Initialize HDFS
        configuration.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
        configuration.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());
        configuration.addResource(new Path(prop.getProperty(Constants.HADOOP_CORE_SITE, "/etc/hadoop/conf/core-site.xml")));
        configuration.addResource(new Path(prop.getProperty(Constants.HADOOP_HDFS_SITE, "/etc/hadoop/conf/hdfs-site.xml")));
        configuration.addResource(new Path(prop.getProperty(Constants.HADOOP_MAPRED_SITE, "/etc/hadoop/conf/mapred-site.xml")));

        filesystem = FileSystem.get(configuration);

        String line = null;
        BufferedReader br = null;

        // Initialize ES Client
        Client client = new TransportClient().addTransportAddress(new InetSocketTransportAddress(prop.getProperty(Constants.HADOOP_NODE_1), 9300))
                .addTransportAddress(new InetSocketTransportAddress(prop.getProperty(Constants.HADOOP_NODE_2), 9300))
                .addTransportAddress(new InetSocketTransportAddress(prop.getProperty(Constants.HADOOP_NODE_3), 9300))
                .addTransportAddress(new InetSocketTransportAddress(prop.getProperty(Constants.HADOOP_NODE_4), 9300));
        
        // include a bulk loader
        // this will be used to process directories worth of files in each elasticsearch call.
        BulkRequestBuilder bulkRequest = client.prepareBulk();

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
            firstLine = false;

            jsonObject.put("HDFSPath", parsedPath.substring(0, parsedPath.lastIndexOf(SLASH_CHAR)));
            jsonObject.put("FILENAME", lfs.getPath().getName());

            try {
                br = new BufferedReader(new InputStreamReader(filesystem.open(lfs.getPath())));
                line = br.readLine();
                while (line != null) {

                	// set the Gutenberg title information as a default.  Override later
                	// if formally presented
                	if (!firstLine) {
                		firstLine = true;
                		if (line.trim().contains(FIRST_LINE_SECONDARY_TITLE_TAG)) {
                			String tmp = line.substring(line.indexOf(FIRST_LINE_SECONDARY_TITLE_TAG) + FIRST_LINE_SECONDARY_TITLE_TAG.length()).trim();
                			if (tmp.startsWith(OF)) {
                				tmp = tmp.substring(OF.length()).trim();
                			}
                			if (tmp.contains(BY)) {
                                jsonObject.put(TITLE, tmp.substring(0, tmp.lastIndexOf(BY)).trim());
                                jsonObject.put(AUTHOR, tmp.substring(tmp.lastIndexOf(BY) + BY.length()).trim());
                                haveAuthor = true;
                                haveTitle = true;
                                break;
                			} else {
                                jsonObject.put(TITLE, tmp.trim());
                                haveTitle = true;
                			}
                			
                		}
                	}
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

                    if (line.startsWith(TITLE+COLON_CHAR) && !haveTitle) {
                        jsonObject.put(TITLE, line.substring(TITLE.length()+1).trim());
                        haveTitle = true;
                    }

                    if (line.startsWith(AUTHOR+COLON_CHAR) && !haveAuthor) {
                        jsonObject.put(AUTHOR, line.substring(AUTHOR.length()+1).trim());
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


//                log.info("Writing message to elasticsearch: " + jsonObject.toJSONString());

                // swap this line out with appending all the request into the bulkRequester
//                IndexResponse response = client.prepareIndex("hadoop", "gutenberg").setSource(jsonObject.toJSONString()).execute().actionGet();
                bulkRequest.add(client.prepareIndex("hadoop", "gutenberg").setSource(jsonObject.toJSONString()));

            } catch (Exception e) {
                log.warning(e.getMessage());
                // done with file;
            }

        }
        
        BulkResponse bulkResponse = bulkRequest.execute().actionGet();
        if (bulkResponse.hasFailures()) {
        	log.warning("Bulk ingest failed: " + bulkResponse.buildFailureMessage());
        }
        
        client.close();

    }

    public static void main(String args[]) {
        if (args.length < 1) {
            System.err.println("Problem here.  Syntax requires an output path.");
        }

        ElasticSearchIndex rh = new ElasticSearchIndex();
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
