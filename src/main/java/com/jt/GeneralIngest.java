package com.jt;

import java.io.IOException;
import java.io.InputStream;
import java.io.StringReader;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Properties;
import java.util.logging.Logger;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

import javax.ws.rs.core.MediaType;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.mortbay.log.Log;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;

import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.WebResource;

/**
 * This class is developed as an example to retrieve files from the Gutenberg
 * library via REsTClient and store the information in either HDFS or local file
 * system. Either way, I have developed this program to retrieve lots of data to
 * experiment within the Hadoop Cluster.
 * 
 * @author dewee
 * 
 */

public class GeneralIngest {

    private static Logger log = Logger.getLogger(GeneralIngest.class.getName());

    private static String APPLICATION_ZIP = "application/zip";

    private static String inputUri = "";
    private static String outputPath = "";
    private static boolean useUriPath = false;
    private static boolean unzip = false;

    // Setup Hadoop properties to write to the file system if available.
    private static Configuration configuration = new Configuration();
    private static FileSystem filesystem = null;
    private static Path path = null;

    private static Properties prop = null;
    
    private static HashMap<String, ArrayList<String>> hrefMap = new HashMap<String, ArrayList<String>>();
    
    boolean bypassDownload = false;
    

    public GeneralIngest() {
    }

    
    /**
     * This method loads the provided or used properties in the job call into the system here.
     * The properties are later used to assist in the run of the program.
     */
    private void loadProperties() {
        String propFileName = "config.properties"; // static property file
                                                   // included in the jar.
                                                   // Excluded here for privacy

        InputStream inputStream = getClass().getClassLoader().getResourceAsStream(propFileName);
        if (inputStream != null) {
            prop = new Properties();
            try {
                prop.load(inputStream);
            } catch (IOException e) {
                log.info("Problem loading property file: " + propFileName + ".  Exception caught: " + e.getMessage());
            }
        } else {
            log.warning("No property file found.  Set your own properties");
        }

    }

    public void setInputUri(String inputUri) {
        this.inputUri = inputUri;
    }

    public void setInputUri(String inputUri, String appender) {
        // no need to check for null. If anything is null, then bubble up the
        // NPE and exit.
        this.inputUri = inputUri + appender;
    }

    public void setOutputPath(String outputPath) {

        this.outputPath = outputPath.endsWith(Constants.SLASH) ? outputPath.substring(0, outputPath.length() - 1) : outputPath;

        if (outputPath.startsWith(Constants.HDFS_PREFIX)) {
            // Initialize HDFS
            configuration.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
            configuration.addResource(new Path(prop.getProperty(Constants.HADOOP_CORE_SITE, "/etc/hadoop/conf/core-site.xml")));
            configuration.addResource(new Path(prop.getProperty(Constants.HADOOP_HDFS_SITE, "/etc/hadoop/conf/hdfs-site.xml")));
            configuration.addResource(new Path(prop.getProperty(Constants.HADOOP_MAPRED_SITE, "/etc/hadoop/conf/mapred-site.xml")));
        } else {
            configuration.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());
        }

        try {
            filesystem = FileSystem.get(configuration);
        } catch (IOException e) {
            log.warning("Problem access file system: " + e.getMessage());
            e.printStackTrace();
        }
    }

    /**
     * This method issues the rest call to the provided url
     * @param uri
     * @param mediaType
     * @return
     */
    public static Object clientRequest(String uri, MediaType mediaType) {
        Object results = null;

        Client client = Client.create();

        WebResource webResource = client.resource(uri);

        ClientResponse response = webResource.accept(mediaType).get(ClientResponse.class);

        if (response.getStatus() != 200) {
            log.info("Problem with request: " + response.getStatus());
            return null;
        }

        if (response.getType().isCompatible(MediaType.TEXT_HTML_TYPE)) {
            log.info("using string");
            results = response.getEntity(String.class);
        } else if (response.getType().toString().equals(APPLICATION_ZIP)) {
            log.fine("using app zip");
            log.fine("response have an entity: " + response.hasEntity());
            log.fine("response toString: " + response.toString());
            log.fine("response entity tag: " + response.getEntityTag());
            results = response.getEntity(InputStream.class);
        } else {
            log.info("Don't know this type: " + response.getType());
        }

        log.fine("MediaType retrieved: " + response.getType());
        // log.info("Response is: " + results);

        return results;
    }



    private void processProperties() {
        setOutputPath(prop.getProperty(Constants.OUTPUT_PATH));
        setInputUri(prop.getProperty(Constants.REST_ENDPOINT), prop.getProperty(Constants.REST_FILETYPE));
        useUriPath = Boolean.parseBoolean(prop.getProperty(Constants.PRESERVE_PATH, "false"));
        unzip = Boolean.parseBoolean(prop.getProperty(Constants.STORE_UNZIPPED, "false"));

        log.info("Set output path property: " + outputPath);
        log.info("Set uri endpoint property: " + inputUri);
        log.info("Use the uri path: " + useUriPath);
        log.info("Unzip: " + unzip);
    }

    /**
     * This is the main method which retrieves the properties, makes the rest
     * call for file from Gutenberg rest service, store the files to the local
     * or hadoop file system.
     */
    public void run() {
        loadProperties();
        processProperties();

        // lets setup the url and find the number of iterations we will like to
        // use

        int iterations = Integer.parseInt(prop.getProperty("jt.input.iterations", "0"));

        // process over the number of iterations to gather flies from the
        // Gutenberg repository

        ArrayList<String>fileUris = null;
        for (int a = 0; a < iterations; a++) {

            if (fileUris != null) {
                fileUris.clear();
            }
            hrefMap.clear();
            // Call request from service
            log.info("Sending request as: " + inputUri);
            String htmlResults = (String) clientRequest(inputUri, MediaType.TEXT_HTML_TYPE);

            // process results and store in hdfs
            log.fine("results: " + htmlResults);
            try {
                hrefMap = GutenbergParser.parseResults(htmlResults);
                
                if (hrefMap == null) {
                    log.info("Nothing parsed for further processing");
                }
                
                byte[] buffer = new byte[1024];
                int len = 0;
                URI uri = null;
                FSDataOutputStream fileOutputStream = null;
                
                // Ok, here is were we will download, unpackage, and store each file.

                fileUris = hrefMap.get(Constants.HTTP_FLG);
                if (!bypassDownload) {
                for (String fileUri : fileUris) {

                    log.fine("Want to process file: " + fileUri);

                    // setup the input stream for the file
                    InputStream is = (InputStream) clientRequest(fileUri, MediaType.WILDCARD_TYPE);

                    // build the URI for the file so we can parse the object for
                    // the path
                    try {
                        uri = new URI(fileUri);
                    } catch (URISyntaxException e) {
                        log.warning("Problem creating uri: " + e.getMessage());
                    }

                    // build up the output path
                    path = new Path(outputPath + Constants.SLASH
                            + (useUriPath ? uri.getPath() : fileUri.substring(fileUri.lastIndexOf(Constants.SLASH) + 1)));

                    // if the file exists, remove it
                    if (filesystem.exists(path)) {
                        filesystem.delete(path, true);
                    }

                    if (unzip) {
                        ZipInputStream zis = new ZipInputStream(is);
                        ZipEntry ze = null;
                        String modifiedPath = path.toString().substring(0, path.toString().lastIndexOf(Constants.SLASH));
                        log.fine("output path: " + path.toString());
                        log.fine("modified path: " + modifiedPath);

                        while ((ze = zis.getNextEntry()) != null) {
                            // create the file and write the contents
                            // filesystem.create(new Path("/in/test/etext00/marbo10/2/12/12"));
                            if (ze.isDirectory()) {
                                continue;
                            }
                            try {
                                fileOutputStream = filesystem.create(new Path(modifiedPath + Constants.SLASH + ze.getName()));
                                while ((len = zis.read(buffer)) >= 0) {
                                    fileOutputStream.write(buffer, 0, len);
                                }
                                fileOutputStream.close();
                            } catch (Exception e) {
                                log.warning("Problem processing file: " + modifiedPath + Constants.SLASH + ze.getName() + " skipping...");
                            }
                        }

                    } else {
                        // create the file and write the contents
                        fileOutputStream = filesystem.create(path);
                        while ((len = is.read(buffer)) >= 0) {
                            fileOutputStream.write(buffer, 0, len);
                        }
                        fileOutputStream.close();
                    }
                }
                }
            } catch (ParserConfigurationException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            } catch (SAXException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            } catch (IOException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
            
            // the next next inputURI to the values retrieved from the Next Page tag.
            // since there is only one next page in the list, pop the first one off the arraylist
            setInputUri(prop.getProperty(Constants.REST_ENDPOINT), hrefMap.get(Constants.HARVEST_TAG).get(0).toString());
        }

    }

    public static void main(String args[]) {

        GeneralIngest gi = new GeneralIngest();
        gi.run();
    }

}
