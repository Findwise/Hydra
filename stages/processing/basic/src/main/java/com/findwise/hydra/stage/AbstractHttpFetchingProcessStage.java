package com.findwise.hydra.stage;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.http.HttpEntity;
import org.apache.http.client.CookieStore;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.findwise.hydra.local.LocalDocument;
import com.findwise.utils.http.HttpFetchConfiguration;
import com.findwise.utils.http.HttpFetchConfigurationBuilder;
import com.findwise.utils.http.HttpFetcher;
import com.findwise.utils.http.UriProvider;

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
        implements UriProvider {
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

    public AbstractHttpFetchingProcessStage() {
        fetcher = new HttpFetcher(getSettings());
    }

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
        fetcher = new HttpFetcher(getSettings());

    }

    public void setClient(HttpClient client) {
        fetcher = new HttpFetcher(fetcher.getCookieStore(), client, fetcher.getRequest(),
                fetcher.getConfiguration());
    }

    public void setCookieStore(CookieStore cookieStore) {
        fetcher = new HttpFetcher(cookieStore, fetcher.getClient(), fetcher.getRequest(),
                fetcher.getConfiguration());
    }

    public void setRequest(HttpGet request) {
        fetcher = new HttpFetcher(fetcher.getCookieStore(), fetcher.getClient(), request,
                fetcher.getConfiguration());
    }

    @SuppressWarnings("unchecked")
    @Override
    public void process(LocalDocument doc) throws ProcessException {
        List<String> identifiers = new ArrayList<String>();
        Object field = doc.getContentField(identifierField);
        if (field instanceof String) {
            identifiers.add((String) field);
        } else if (field instanceof List<?>) {
            identifiers.addAll((List<String>) field);
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
            List<String> output = getFieldAsStrings(doc, fieldName);
            output.add(identifier);
            doc.putContentField(fieldName, output);
            return;
        }
        HttpEntity entity = fetcher.fetch(identifier, getAcceptedContentHeader(),
                this);

        processResponseEntity(entity, doc);
    }

    @SuppressWarnings("unchecked")
    public List<String> getFieldAsStrings(LocalDocument doc, String field) {
        List<String> list = null;
        if (doc.hasContentField(field)) {
            Object previousFieldValue = doc.getContentField(field);
            if (previousFieldValue instanceof String) {
                list = new ArrayList<String>();
                list.add((String) previousFieldValue);
            } else if (previousFieldValue instanceof List<?>) {
                list = (List<String>) previousFieldValue;
            }
        }
        if (null == list) {
            list = new ArrayList<String>();
        }
        return list;
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


    public Map<String, String> getIgnoredIdentifiers() {
        return ignoredIdentifiers;
    }

    public void setIgnoredIdentifiers(Map<String, String> ignoredIdentifiers) {
        this.ignoredIdentifiers = ignoredIdentifiers;
    }
}
