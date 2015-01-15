package com.findwise.utils.http;

import java.util.List;
import java.util.Map;

public class HttpFetchConfiguration {
    private final AuthMethod authMethod;
    // Username for Basic Auth
    private final String basicAuthUsername;
    // Password for Basic Auth
    private final String basicAuthPassword;
    // Host for Basic Auth
    private final String basicAuthHost;
    // Port for Basic Auth
    private final int basicAuthPort;
    private final String formBasedLoginUrl;
    private final Map<String, String> formValues;
    // URI to retrieve session cookie from
    private final String sessionCookieUri;
    // List of hostnames for which to accept invalid SSL certificates, default
    // empty
    private final List<String> sslHostExceptions;
    // Number of retries. May be to fallback URLs
    private final int retries;
    // Expiration time for cached responses, in seconds. Any positive value
    // enables caching. Default -1
    private final long cacheExpiration;

    public HttpFetchConfiguration(AuthMethod authMethod,
            String basicAuthUsername, String basicAuthPassword,
            String basicAuthHost, int basicAuthPort, String formBasedLoginUrl,
            Map<String, String> formValues, String sessionCookieUri,
            List<String> sslHostExceptions, int retries, long cacheExpiration) {
        this.authMethod = authMethod;
        this.basicAuthUsername = basicAuthUsername;
        this.basicAuthPassword = basicAuthPassword;
        this.basicAuthHost = basicAuthHost;
        this.basicAuthPort = basicAuthPort;
        this.formBasedLoginUrl = formBasedLoginUrl;
        this.formValues = formValues;
        this.sessionCookieUri = sessionCookieUri;
        this.sslHostExceptions = sslHostExceptions;
        this.retries = retries;
        this.cacheExpiration = cacheExpiration;
    }

    public AuthMethod getAuthMethod() {
        return authMethod;
    }

    public String getBasicAuthUsername() {
        return basicAuthUsername;
    }

    public String getBasicAuthPassword() {
        return basicAuthPassword;
    }

    public String getBasicAuthHost() {
        return basicAuthHost;
    }

    public int getBasicAuthPort() {
        return basicAuthPort;
    }

    public String getFormBasedUrl() {
        return formBasedLoginUrl;
    }

    public Map<String, String> getFormValues() {
        return formValues;
    }

    public boolean hasCookieUri() {
        return sessionCookieUri != null;
    }

    public String getSessionCookieUri() {
        return sessionCookieUri;
    }

    public List<String> getSslHostExceptions() {
        return sslHostExceptions;
    }

    public int getRetries() {
        return retries;
    }

    public long getCacheExpiration() {
        return cacheExpiration;
    }
}
