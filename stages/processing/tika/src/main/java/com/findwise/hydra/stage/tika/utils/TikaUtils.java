package com.findwise.hydra.stage.tika.utils;

import java.io.IOException;
import java.io.InputStream;
import java.io.StringWriter;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

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
			String fieldPrefix, InputStream stream, Parser parser) throws IOException,
			SAXException, TikaException {
		Metadata metadata = new Metadata();
		ParseContext parseContext = new ParseContext();
		parseContext.set(Parser.class, parser);
		StringWriter textData = new StringWriter();
		parser.parse(stream, new BodyContentHandler(textData), metadata,
				parseContext);

		addTextToDocument(doc, fieldPrefix, textData);
		addMetadataToDocument(doc, fieldPrefix, metadata);

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
	

	public static Set<URL> getUrlsFromObject(Object urlsObject) throws MalformedURLException {
		if (urlsObject instanceof String) {
			return new HashSet<URL>(Arrays.asList(new URL((String)urlsObject)));
		} 
		if (urlsObject instanceof Iterable<?>) {
			Set<URL> urls = new HashSet<URL>();
			for (Object urlObj : (Iterable<?>)urlsObject) {
				urls.addAll(getUrlsFromObject(urlObj));
			}
			return urls;
		}
		return new HashSet<URL>();
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

}
