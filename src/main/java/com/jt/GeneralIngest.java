package com.jt;

import java.io.IOException;
import java.io.InputStream;
import java.io.StringReader;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.logging.Logger;

import javax.ws.rs.core.MediaType;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
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
 * This class is developed as an example to retrieve files from the Gutenberg library via REsTClient 
 * and store the information in either HDFS or local file system.  Either way, I have developed this
 * program to retrieve lots of data to experiment within the Hadoop Cluster.
 * 
 * @author dewee
 *
 */

public class GeneralIngest {
    
    private static Logger log = Logger.getLogger(GeneralIngest.class.getName());

    private static String BODY_START = "<body>";
    private static String BODY_END = "</body>";
    private static String HTTP_FLG = "http";
    private static String PARAGRAPH_TAG = "p";
    private static String ANCHOR_TAG = "a";
    private static String SLASH = "/";
    private static String HDFS_PREFIX = "hdfs";
    
    private static String APPLICATION_ZIP = "application/zip";
    
    private static String inputUri = "";
    private static String outputPath = "";
    private static boolean useUriPath = false;

    // Setup Hadoop properties to write to the file system if available.
    private static Configuration configuration = new Configuration();
    private static FileSystem filesystem = null;
    private static Path path = null;

    public void setInputUri(String inputUri) {
        this.inputUri = inputUri;
    }

    public void setOutputPath(String outputPath) {
        
        this.outputPath = outputPath.endsWith(SLASH) ? outputPath.substring(0, outputPath.length()-1) : outputPath;
        
        if (outputPath.startsWith(HDFS_PREFIX)) {
            // Initialize HDFS
            configuration.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
            configuration.addResource(new Path("/etc/hadoop/conf/core-site.xml"));
            configuration.addResource(new Path("/etc/hadoop/conf/hdfs-site.xml"));
            configuration.addResource(new Path("/etc/hadoop/conf/mapred-site.xml"));
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
            log.info("using app zip");
            log.info("response have an entity: " + response.hasEntity());
            log.info("response toString: " + response.toString());
            log.info("response entity tag: " + response.getEntityTag());
            results = response.getEntity(InputStream.class);
        } else {
            log.info("Don't know this type: " + response.getType());
        }
        
        log.info("MediaType retrieved: " + response.getType());
//        log.info("Response is: " + results);

        return results;
    }

    private static ArrayList<String> getGutenbergFiles(String htmlResults) throws ParserConfigurationException, SAXException,
            IOException {
        String uri = null;
        ArrayList<String> uris = new ArrayList<String>();

        DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
        DocumentBuilder db = dbf.newDocumentBuilder();
        InputSource is = new InputSource();
        is.setCharacterStream(new StringReader(htmlResults));

        Document doc = db.parse(is);
        NodeList nodes = doc.getElementsByTagName(PARAGRAPH_TAG);

        // iterate the employees
        for (int i = 0; i < nodes.getLength(); i++) {
            Element element = (Element) nodes.item(i);

            NodeList name = element.getElementsByTagName(ANCHOR_TAG);
            Element line = (Element) name.item(0);
            uri = getCharacterDataFromElement(line);
            if (uri.startsWith(HTTP_FLG)) {
                uris.add(uri);
            }
        }

        return uris;
    }

    public static String getCharacterDataFromElement(Element e) {
        Node child = e.getFirstChild();
        return child.getNodeValue();
    }
    
    private void processProperties() {
        setOutputPath(System.getProperty(Properties.OUTPUT_PATH));
        setInputUri(System.getProperty(Properties.REST_ENDPOINT));
        useUriPath = Boolean.parseBoolean(System.getProperty(Properties.PRESERVE_PATH, "false"));
        
        log.info("Set output path property: " + outputPath);
        log.info("Set uri endpoint property: " + inputUri);
    }
    
    /**
     * This is the main method which retrieves the properties, makes the rest call for file from Gutenberg rest service,
     * store the files to the local or hadoop file system.
     */
    public void run() {
        processProperties();
        
        String htmlResults = (String) clientRequest(inputUri, MediaType.TEXT_HTML_TYPE);

        log.finest("results: " + htmlResults);
        try {
            ArrayList<String> fileUris = getGutenbergFiles(htmlResults.substring(htmlResults.indexOf(BODY_START), htmlResults.indexOf(BODY_END)+BODY_END.length()));
            byte[] buffer = new byte[1024];
            int len = 0;
            URI uri = null;
            FSDataOutputStream fileOutputStream = null;
            for (String fileUri : fileUris) {
                log.info("Want to process file: " + fileUri);
                InputStream is  = (InputStream) clientRequest(fileUri, MediaType.WILDCARD_TYPE);

                // parse out the filepath from the url
                try {
                    uri = new URI(fileUri);
                } catch (URISyntaxException e) {
                    log.warning("Problem creating uri: " + e.getMessage());
                }
                
                // build up the output path
                path = new Path(outputPath + SLASH + (useUriPath ? uri.getPath() : fileUri.substring(fileUri.lastIndexOf(SLASH)+1)));
                
                // if the file exists, remove it
                if (filesystem.exists(path)) {
                    filesystem.delete(path, true);
                }

                // create the file and write the contents
                fileOutputStream = filesystem.create(path);
                while ( (len=is.read(buffer)) >= 0) {
                    fileOutputStream.write(buffer, 0, len);
                }
                fileOutputStream.close();
                break;
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
    }

    public static void main(String args[]) {

        GeneralIngest gi = new GeneralIngest();
        gi.run();
    }

}
