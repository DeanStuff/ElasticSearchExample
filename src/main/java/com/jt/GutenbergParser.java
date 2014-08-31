package com.jt;

import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.HashMap;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;

import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;

public class GutenbergParser {

    private static HashMap<String, ArrayList<String>> groupMap = new HashMap<String, ArrayList<String>>();
    
    public static String getHrefInfo(Element e) {
        return e.getAttribute(Constants.HREF_TAG);
    }

    public static String getNodeValue(Element e) {
        Node child = e.getFirstChild();
        return child.getNodeValue();
    }


    /**
     * Populate the goupMap map by adding the href type and the value.
     * Later on it will pull the sorted list out to process info individually where needed.
     * Note: This removes repetitive processing over the html results, but uses memory to store
     * the information.  This can be resolved by clearing the html results after populating the map
     * @param key - Store the href type like file or harvest
     * @param value - the url or value
     */
    private static void populateHtmlRefs(String key, String value) {
        ArrayList<String> hrefInfo;
        if (groupMap.containsKey(key)) {
            hrefInfo = groupMap.get(key);
        } else {
            hrefInfo = new ArrayList<String>();
        }
        hrefInfo.add(value);
        groupMap.put(key, hrefInfo);
    }
    
    /**
     * This method's responsibility is to parse the URLs returned from the
     * client and return a simple ArrayList of them
     * 
     * @param htmlResults
     * @return 
     * @return
     * @throws ParserConfigurationException
     * @throws SAXException
     * @throws IOException
     */
    public static HashMap<String, ArrayList<String>> parseResults(String htmlResults) throws ParserConfigurationException, SAXException,
            IOException {
        
        if (htmlResults == null || htmlResults.trim().isEmpty()) {
            return null;
        }
        
        String uri = null;
        
        // Only parse out the body text of the html results
        String bodyString = htmlResults.substring(htmlResults.indexOf(Constants.BODY_START), htmlResults.indexOf(Constants.BODY_END) + Constants.BODY_END.length());


        DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
        DocumentBuilder db = dbf.newDocumentBuilder();
        InputSource is = new InputSource();
        is.setCharacterStream(new StringReader(bodyString));

        Document doc = db.parse(is);
        NodeList nodes = doc.getElementsByTagName(Constants.PARAGRAPH_TAG);

        // iterate the returned list
        for (int i = 0; i < nodes.getLength(); i++) {
            Element element = (Element) nodes.item(i);

            NodeList name = element.getElementsByTagName(Constants.ANCHOR_TAG);
            Element line = (Element) name.item(0);
            uri = getNodeValue(line);

            // replacing individual list of zip files with internal map of references retrieved from the html
            populateHtmlRefs(uri.startsWith(Constants.HTTP_FLG) ? Constants.HTTP_FLG : Constants.HARVEST_TAG, line.getAttribute(Constants.HREF_TAG));
        }
        
        return groupMap;

    }

}
