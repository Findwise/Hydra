package com.findwise.hydra.stage.tika;

import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;
import java.net.URLConnection;
import java.util.Iterator;
import java.util.Map;

import org.apache.commons.codec.binary.Base64;
import org.apache.tika.exception.TikaException;
import org.apache.tika.parser.AutoDetectParser;
import org.apache.tika.parser.Parser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xml.sax.SAXException;

import com.findwise.hydra.local.LocalDocument;
import com.findwise.hydra.stage.AbstractProcessStage;
import com.findwise.hydra.stage.Parameter;
import com.findwise.hydra.stage.ProcessException;
import com.findwise.hydra.stage.RequiredArgumentMissingException;
import com.findwise.hydra.stage.Stage;
import com.findwise.hydra.stage.tika.utils.TikaUtils;

@Stage(description = "A stage that fetches the content from a given url and appends it the the document")
public class SimpleFetchingTikaStage extends AbstractProcessStage {
    private static Logger logger = LoggerFactory.getLogger(SimpleFetchingTikaStage.class);

	@Parameter(required = true, description = "The field name pattern that should be matched where " +
			"urls will be found. First group plus \"_\" will be used as field prefix. Example:" +
			" \"attachment_(.*)\" will match for example attachment_a and will use \"a_\" as prefix")
	private String urlFieldPattern = null;

	@Parameter(name = "addMetaData", description = "Add the metadata to the document or not. Defaults to true")
	private boolean addMetaData = true;

	@Parameter(description = "Set to true, will also do language detection and add the field 'prefix_language' according to the prefix rules. Defaults to true")
	private boolean addLanguage = true;

	@Parameter(description = "Username for basic authentication.")
	private String username = null;

	@Parameter(description = "Password for basic authentication.")
	private String password = null;

	private Parser parser = new AutoDetectParser();

	@Override
	public void process(LocalDocument doc) throws ProcessException {
		Map<String, Object> urls = TikaUtils.getFieldMatchingPattern(doc,
				urlFieldPattern);
		for (String field : urls.keySet()) {
			try {
				Iterator<URL> it = TikaUtils.getUrlsFromObject(urls.get(field))
						.iterator();
				for (int i = 1; it.hasNext(); i++) {
					String num = (i > 1) ? "" + i : "";
					URL url = it.next();
					URLConnection connection = createConnection(url);
					TikaUtils.enrichDocumentWithFileContents(doc, field + num
							+ "_", connection.getInputStream(), parser,
							addMetaData, addLanguage);
				}
			} catch (URISyntaxException e) {
				throw new ProcessException("A field matching the pattern "
						+ field + " contained a malformed url", e);
			} catch (IOException e) {
				throw new ProcessException(
						"Failed opening or reading from stream", e);
			} catch (SAXException e) {
				throw new ProcessException("Failed parsing document", e);
			} catch (TikaException e) {
				throw new ProcessException("Got exception from Tika", e);
			}
		}

	}

	private URLConnection createConnection(URL url) throws ProcessException,
			IOException {
		URLConnection connection = url.openConnection();
		if (useBasicAuthentication()) {
			String authString = username + ":" + password;
			byte[] authEncBytes = Base64.encodeBase64(authString
					.getBytes("UTF-8"));
			String authStringEnc = "Basic " + new String(authEncBytes, "UTF-8");
			connection.setRequestProperty("Authorization", authStringEnc);
		}
		return connection;
	}

	private boolean useBasicAuthentication() {
		return username != null && password != null;
	}

	@Override
	public void init() throws RequiredArgumentMissingException {
		if (urlFieldPattern == null) {
			throw new RequiredArgumentMissingException(
					"Missing parameter urlFieldPattern");
		}
		logger.debug("Initiated SimpleTikaStage");
	}

	/* For testing purposes */

	void setUrlFieldPattern(String urlFieldPattern) {
		this.urlFieldPattern = urlFieldPattern;
	}

	void setParser(Parser parser) {
		this.parser = parser;
	}
}
