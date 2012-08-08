package com.findwise.hydra.stage.tika;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.Map;

import org.apache.tika.exception.TikaException;
import org.apache.tika.parser.AutoDetectParser;
import org.apache.tika.parser.Parser;
import org.xml.sax.SAXException;

import com.findwise.hydra.common.Logger;
import com.findwise.hydra.local.LocalDocument;
import com.findwise.hydra.stage.AbstractProcessStage;
import com.findwise.hydra.stage.Parameter;
import com.findwise.hydra.stage.ProcessException;
import com.findwise.hydra.stage.RequiredArgumentMissingException;
import com.findwise.hydra.stage.Stage;
import com.findwise.hydra.stage.tika.utils.TikaUtils;

@Stage(description = "A stage that fetches the content from a given url and appends it the the document")
public class SimpleFetchingTikaStage extends AbstractProcessStage {

	@Parameter(description = "The field name pattern that should be matched where urls will be found. First group plus \"_\" will be used as field prefix. Example: \"attachment_(.*)\" will match for example attachment_a and will use \"a_\" as prefix")
	private String urlFieldPattern = null;
        @Parameter(name = "addMetaData", description = "Add the metadata to the document or not. Defaults to true")
        private boolean addMetaData = true;
        
	static private Parser parser = new AutoDetectParser();

	@Override
	public void process(LocalDocument doc) throws ProcessException {
		Map<String, Object> urls = TikaUtils.getFieldMatchingPattern(doc,
				urlFieldPattern);
		for (String field : urls.keySet()) {
			try {
				for (URL url : TikaUtils.getUrlsFromObject(urls.get(field))) {
					TikaUtils.enrichDocumentWithFileContents(doc, field + "_",
							url.openStream(), parser, addMetaData);
				}
			} catch (MalformedURLException e) {
				Logger.warn("A field matching the pattern " + field
						+ " contained a malformed url", e);
			} catch (IOException e) {
				Logger.warn("Failed opening or reading from stream", e);
			} catch (SAXException e) {
				Logger.warn("Failed parsing document", e);
			} catch (TikaException e) {
				Logger.warn("Got exception from Tika", e);
			}
		}

	}

	@Override
	public void init() throws RequiredArgumentMissingException {
		if (urlFieldPattern == null) {
			throw new RequiredArgumentMissingException(
					"Missing parameter urlFieldPattern");
		}

		Logger.debug("Initiated SimpleTikaStage");
	}

	/* For testing purposes */

	void setUrlFieldPattern(String urlFieldPattern) {
		this.urlFieldPattern = urlFieldPattern;
	}

	static void setParser(Parser parser) {
		SimpleFetchingTikaStage.parser = parser;
	}

}
