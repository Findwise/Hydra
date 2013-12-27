package com.findwise.hydra.stage.tika;

import java.io.IOException;
import java.io.InputStream;
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
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.util.logging.Level;
import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.HttpsURLConnection;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSession;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;
import org.apache.http.HttpConnection;

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
					final InputStream inputStream = connection.getInputStream();
					try {
					TikaUtils.enrichDocumentWithFileContents(doc, field + num
							+ "_", inputStream, parser,
							addMetaData, addLanguage);
					} finally {
						inputStream.close();
					}
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
			} catch (NoSuchAlgorithmException ex) {
                            throw new ProcessException("Bad algo", ex);
                    } catch (KeyManagementException ex) {
                        throw new ProcessException("Key was stupid", ex);
                    }
		}

	}
        
    

	private URLConnection createConnection(URL url) throws ProcessException,
			IOException,
			NoSuchAlgorithmException,
			KeyManagementException {
              // Create a trust manager that does not validate certificate chains
		TrustManager[] trustAllCerts = new TrustManager[] {
                    new X509TrustManager() {
                                @Override
				public java.security.cert.X509Certificate[] getAcceptedIssuers() {
					return null;
				}
                                @Override
				public void checkClientTrusted(X509Certificate[] certs, String authType) {
				}
                                @Override
				public void checkServerTrusted(X509Certificate[] certs, String authType) {
				}

                    
		}};

		// Install the all-trusting trust manager
		SSLContext sc = SSLContext.getInstance("SSL");
		sc.init(null, trustAllCerts, new java.security.SecureRandom());
		HttpsURLConnection.setDefaultSSLSocketFactory(sc.getSocketFactory());

		// Create all-trusting host name verifier
		HostnameVerifier allHostsValid = new HostnameVerifier() {
                        @Override
			public boolean verify(String hostname, SSLSession session) {
				return true;
			}
		};

		// Install the all-trusting host verifier
		HttpsURLConnection.setDefaultHostnameVerifier(allHostsValid);

                
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
