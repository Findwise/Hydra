package com.findwise.hydra.stage.tika.utils;

import java.io.IOException;
import java.io.InputStream;
import java.io.StringWriter;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.http.client.utils.URLEncodedUtils;
import org.apache.tika.exception.TikaException;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.parser.ParseContext;
import org.apache.tika.parser.Parser;
import org.apache.tika.sax.BodyContentHandler;
import org.xml.sax.SAXException;

import com.findwise.hydra.common.Logger;
import com.findwise.hydra.local.LocalDocument;

public class TikaUtils {

    public static void enrichDocumentWithFileContents(LocalDocument doc,
            String fieldPrefix, InputStream stream, Parser parser, boolean addMetaData) throws IOException,
            SAXException, TikaException {
        Metadata metadata = new Metadata();
        ParseContext parseContext = new ParseContext();
        parseContext.set(Parser.class, parser);
        StringWriter textData = new StringWriter();
        parser.parse(stream, new BodyContentHandler(textData), metadata,
                parseContext);

        addTextToDocument(doc, fieldPrefix, textData);
        if (addMetaData) {
            addMetadataToDocument(doc, fieldPrefix, metadata);
        }

    }

    public static void addTextToDocument(LocalDocument doc, String fieldPrefix,
            StringWriter textData) {
        doc.putContentField(fieldPrefix + "content", textData.toString());
    }

    public static void addMetadataToDocument(LocalDocument doc, String fieldPrefix,
            Metadata metadata) {
        for (String name : metadata.names()) {
            if (metadata.getValues(name).length > 1) {
                doc.putContentField(fieldPrefix + name,
                        Arrays.asList(metadata.getValues(name)));
            } else {
                doc.putContentField(fieldPrefix + name, metadata.get(name));
            }
        }
    }

    public static List<URL> getUrlsFromObject(Object urlsObject) throws MalformedURLException, URISyntaxException {
        if (urlsObject instanceof String) {
            return Arrays.asList(uriFromString((String) urlsObject).toURL());
        }
        if (urlsObject instanceof Iterable<?>) {
            List<URL> urls = new ArrayList<URL>();
            for (Object urlObj : (Iterable<?>) urlsObject) {
                urls.addAll(getUrlsFromObject(urlObj));
            }
            return urls;
        }
        return new ArrayList<URL>();
    }

    public static Map<String, Object> getFieldMatchingPattern(LocalDocument doc,
            String pattern) {
        Map<String, Object> fieldToUrl = new HashMap<String, Object>();

        for (String field : doc.getContentFields()) {
            Pattern p = Pattern.compile(pattern);
            Matcher m = p.matcher(field);
            if (m.matches()) {
                String toField;
                if (m.groupCount() >= 1) {
                    toField = m.group(1);
                } else {
                    toField = m.group();
                }
                Logger.debug("Added " + doc.getContentField(field) + " to "
                        + toField);
                fieldToUrl.put(toField, doc.getContentField(field));
            }
        }

        return fieldToUrl;
    }
    
    public static URI uriFromString(String s) throws URISyntaxException {
    	String scheme = null;
    	int port = -1;
    	String userinfo = null;
    	String host;
    	String path = "";
    	String query = null;
    	String fragment = null;
    	URLEncodedUtils.parse(s, Charset.defaultCharset());
    	Matcher m = Pattern.compile("([^:]+)://.*").matcher(s);
    	if(m.matches()) {
    		scheme = m.group(1); 
    	} 
    	
    	m = Pattern.compile("[^:]+://([^@]+)@.*").matcher(s);
    	if(m.matches()) {
    		userinfo = m.group(1);
        	m = Pattern.compile("[^:]+://"+userinfo+"@([^:/]+).*").matcher(s);
    	} else {
        	m = Pattern.compile("[^:]+://([^:/]+).*").matcher(s);
    	}
    	
    	if(m.matches()) {
    		host = m.group(1);
    	} else {
    		throw new URISyntaxException(s, "No host specified");
    	}
    	
    	
    	m = Pattern.compile("[^:]+://.*"+host+":([0-9]+).*").matcher(s);
    	if(m.matches()) {
    		port = Integer.parseInt(m.group(1));
    	} 
    	
    	m = Pattern.compile("[^:]+://[^/]+(/[^?#]*).*").matcher(s);
    	if(m.matches()) {
    		path = m.group(1);
    	} 
    	
    	m = Pattern.compile("[^:]+://[^?]+\\?([^#]*).*").matcher(s);
    	if(m.matches()) {
    		query = m.group(1);
    	} 
    	
    	m = Pattern.compile("[^:]+://[^#]+#(.*)").matcher(s);
    	if(m.matches()) {
    		fragment = m.group(1);
    	} 

    	return new URI(scheme, userinfo, host, port, path, query, fragment);
    }
}
