package com.findwise.hydra.stage;

import com.findwise.hydra.local.IncorrectFieldTypeException;
import com.findwise.hydra.local.LocalDocument;
import com.findwise.utils.http.HttpFetchConfiguration;
import com.findwise.utils.http.HttpFetchConfigurationBuilder;
import com.findwise.utils.http.HttpFetcher;
import com.findwise.utils.http.RequestProvider;
import com.findwise.utils.http.UriProvider;
import org.apache.http.HttpEntity;
import org.apache.http.client.CookieStore;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpRequestBase;
import org.apache.http.util.EntityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Generic HTTP fetching stage. Subclasses operate on one fetched URL at a time.
 * This class takes care of opening and closing connections.
 *
 * Supports:
 * - Basic Auth
 * - HTTP Response caching
 * - SSL, with optional exceptions for trusted hosts
 * - Fetching of session cookies
 *
 * @author olof.nilsson@findwise.com
 */
public abstract class AbstractHttpFetchingProcessStage extends AbstractProcessStage
		implements UriProvider, RequestProvider {
	private final Logger logger = LoggerFactory.getLogger(
			AbstractHttpFetchingProcessStage.class);

	/**
	 * Subclasses can decide what an identifier is
	 */
	@Parameter(name = "identifierField",
			description = "Field name where the identifier or list of identifiers " +
					"to use can be found. Defaults to 'url'")
	protected String identifierField = "url";

	@Parameter(name = "basicAuthUsername", description = "Username for Basic Auth")
	protected String basicAuthUsername = null;

	@Parameter(name = "basicAuthPassword", description = "Password for Basic Auth")
	protected String basicAuthPassword = null;

	@Parameter(name = "basicAuthHost", description = "Host for Basic Auth")
	protected String basicAuthHost = null;

	@Parameter(name = "basicAuthPort", description = "Port for Basic Auth")
	protected int basicAuthPort = -1;

	@Parameter(name = "sessionCookieUri",
			description = "URI to retrieve session cookie from")
	protected String sessionCookieUri = null;

	@Parameter(
			description = "List of hostnames for which to accept invalid SSL certificates, default empty")
	protected List<String> sslHostExceptions = new ArrayList<String>();

	@Parameter(description = "Number of retries. May be to fallback URLs")
	protected int retries = 1;

	@Parameter(
			description = "Identifiers to map directly to output, skipping fetch. Map from identifier to field.")
	private Map<String, String> ignoredIdentifiers = new HashMap<String, String>();

	@Parameter(
			description = "Expiration time for cached responses, in seconds. Any positive value enables caching. Default -1")
	private long cacheExpiration = -1L;

	private HttpFetcher fetcher;

	private HttpFetchConfiguration getSettings() {
		HttpFetchConfigurationBuilder c = new HttpFetchConfigurationBuilder();
		c.setBasicAuthHost(basicAuthHost);
		c.setBasicAuthPassword(basicAuthPassword);
		c.setBasicAuthPort(basicAuthPort);
		c.setBasicAuthUsername(basicAuthUsername);
		c.setCacheExpiration(cacheExpiration);
		c.setRetries(retries);
		c.setSessionCookieUri(sessionCookieUri);
		c.setSslHostExceptions(sslHostExceptions);
		return c.build();
	}

	@Override
	public void init() throws RequiredArgumentMissingException, InitFailedException {
		super.init();
		if(fetcher == null) {
			fetcher = new HttpFetcher(getSettings());
		}
	}

	public void setFetcher(HttpFetcher fetcher) {
		this.fetcher = fetcher;
	}

	@SuppressWarnings("unchecked")
	@Override
	public void process(LocalDocument doc) throws ProcessException {
		List<String> identifiers = new ArrayList<String>();
		try {
			identifiers.add(doc.getContentFieldAsString(identifierField));
		} catch (IncorrectFieldTypeException e1) {
			try {
				identifiers.addAll(doc.getContentFieldAsStrings(identifierField));
			} catch (IncorrectFieldTypeException e2) {
				throw new ProcessException("Field '" + identifierField + "' was not a String or List", e2);
			}
		}
		try {
			fetcher.ensureCookie();
			logger.debug("Processing identifiers '{}'", identifiers.toString());
			for (String identifier : identifiers) {
				if (!identifier.isEmpty()) {
					processIdentifier(identifier, doc);
				}
			}
		} finally {
			fetcher.clearCookie();
		}
	}

	private void processIdentifier(String identifier, LocalDocument doc)
			throws ProcessException {
		if (ignoredIdentifiers.containsKey(identifier)) {
			String fieldName = ignoredIdentifiers.get(identifier);
			logger.debug("Ignoring identifier '{}', copying it to '{}'", identifier,
					fieldName);
			doc.appendToContentField(fieldName, identifier);
			return;
		}
		HttpEntity entity = fetcher.fetch(identifier, getAcceptedContentHeader(),
				this, this);

		try {
			processResponseEntity(entity, doc);
		} finally {
			EntityUtils.consumeQuietly(entity);
		}
	}

	/**
	 * Converts an identifier found in the identifierField to a URL string for
	 * fetching
	 *
	 * @return converted identifiers
	 */
	public abstract URI getUriFromIdentifier(String identifier, int attempts)
			throws URISyntaxException;

	/**
	 * Process the response and do work on the document
	 * When this method returns, the superclass will consume the response enitity.
	 *
	 * @param responseEntity
	 */
	public abstract void processResponseEntity(HttpEntity responseEntity,
	                                           LocalDocument doc) throws ProcessException;

	/**
	 * Value of the HTTP header 'ACCEPT'. This will be set on all requests.
	 *
	 * @return accept header value
	 */
	public abstract String getAcceptedContentHeader();

	/**
	 * Request object to use for requests. Should return a new object.
	 * This method can be used to set headers on all requests.
	 *
	 * @return request object to use
	 */
	public abstract HttpRequestBase getRequest();

	public Map<String, String> getIgnoredIdentifiers() {
		return ignoredIdentifiers;
	}

	public void setIgnoredIdentifiers(Map<String, String> ignoredIdentifiers) {
		this.ignoredIdentifiers = ignoredIdentifiers;
	}
}
