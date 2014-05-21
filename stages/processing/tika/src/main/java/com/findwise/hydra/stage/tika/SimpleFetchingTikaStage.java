package com.findwise.hydra.stage.tika;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.net.URLConnection;
import java.util.Iterator;
import java.util.Map;

import com.findwise.utils.tika.InputStreamParser;
import com.findwise.utils.tika.ParsedData;
import org.apache.commons.codec.binary.Base64;
import org.apache.tika.parser.AutoDetectParser;
import org.apache.tika.parser.Parser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.findwise.hydra.local.LocalDocument;
import com.findwise.hydra.stage.AbstractProcessStage;
import com.findwise.hydra.stage.Parameter;
import com.findwise.hydra.stage.RequiredArgumentMissingException;
import com.findwise.hydra.stage.Stage;

/**
 * Downloads content which should be parsed by Tika. A pattern is given which
 * points out all the fields containing URLs to download and parsed.
 * <p/>
 * Notice that the stage will add a prefix to all parsed fields. The prefix has
 * the following format FIRST_CAPTURING_CROUP + _ + PARSED_FIELD_NAME
 * <p/>
 * e.g. If urlFieldPattern is set to crawl_(url) all parsed fields from tika
 * will start with url_.
 * <p/>
 * Sample configuration:
 * {@code
 * {
 * "stageClass": "com.findwise.hydra.stage.tika.SimpleFetchingTikaStage",
 * "query": {
 * "equals":{"type": "file" },
 * "touched":{"set-internal-type": true}
 * },
 * "urlFieldPattern" : "(url)"
 * }
 * }
 */
@Stage(description = "A stage that fetches the content from a given url and appends it the the document")
public class SimpleFetchingTikaStage extends AbstractProcessStage {
    private static Logger logger = LoggerFactory.getLogger(SimpleFetchingTikaStage.class);

    @Parameter(required = true, description = "The field name pattern that should be matched where " +
            "urls will be found. First group plus \"_\" will be used as field prefix. Example:" +
            " \"attachment_(.*)\" will match for example attachment_a and will use \"a_\" as prefix")
    public String urlFieldPattern = null;

    @Parameter(name = "addMetaData", description = "Add the metadata to the document or not. Defaults to true")
    public boolean addMetaData = true;

    @Parameter(description = "Set to true, will also do language detection and add the field 'prefix_language' according to the prefix rules. Defaults to true")
    public boolean addLanguage = true;

    @Parameter(description = "Username for basic authentication.")
    public String username = null;

    @Parameter(description = "Password for basic authentication.")
    public String password = null;

    private Parser parser = new AutoDetectParser();

    @Override
    public void process(LocalDocument doc) throws Exception {
        Map<String, Object> urls = FieldHelper.getFieldMatchingPattern(doc.getContentMap(),
                urlFieldPattern);
        UriParser uriParser = new UriParser();
        DocumentParserHelper documentParserHelper = new DocumentParserHelper(addMetaData, addLanguage);
        InputStreamParser inputStreamParser = new InputStreamParser(parser);
        for (String field : urls.keySet()) {
            Iterator<URL> it = uriParser.getUrlsFromObject(urls.get(field)).iterator();
            for (int i = 1; it.hasNext(); i++) {
                String num = (i > 1) ? "" + i : "";
                URL url = it.next();
                URLConnection connection = createConnection(url);
                final InputStream inputStream = connection.getInputStream();
                try {
                    String prefix = field + num + "_";
                    ParsedData parsedData = inputStreamParser.parse(inputStream);
                    documentParserHelper.addParsedDataToDocument(parsedData, doc, prefix);
                } finally {
                    inputStream.close();
                }
            }
        }
    }

    private URLConnection createConnection(URL url) throws IOException {
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
