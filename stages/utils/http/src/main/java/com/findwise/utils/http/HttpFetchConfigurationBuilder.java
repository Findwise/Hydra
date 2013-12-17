package com.findwise.utils.http;

import java.util.ArrayList;
import java.util.List;

public class HttpFetchConfigurationBuilder {
    private String basicAuthUsername = null;
    private String basicAuthPassword = null;
    private String basicAuthHost = null;
    private int basicAuthPort = -1;
    private String sessionCookieUri = null;
    private List<String> sslHostExceptions = new ArrayList<String>();
    private int retries = 3;
    private long cacheExpiration = -1L;

    public HttpFetchConfigurationBuilder setBasicAuthUsername(String basicAuthUsername) {
        this.basicAuthUsername = basicAuthUsername;
        return this;
    }

    public HttpFetchConfigurationBuilder setBasicAuthPassword(String basicAuthPassword) {
        this.basicAuthPassword = basicAuthPassword;
        return this;
    }

    public HttpFetchConfigurationBuilder setBasicAuthHost(String basicAuthHost) {
        this.basicAuthHost = basicAuthHost;
        return this;
    }

    public HttpFetchConfigurationBuilder setBasicAuthPort(int basicAuthPort) {
        this.basicAuthPort = basicAuthPort;
        return this;
    }

    public HttpFetchConfigurationBuilder setSessionCookieUri(String sessionCookieUri) {
        this.sessionCookieUri = sessionCookieUri;
        return this;
    }

    public HttpFetchConfigurationBuilder setSslHostExceptions(List<String> sslHostExceptions) {
        this.sslHostExceptions = sslHostExceptions;
        return this;
    }

    public HttpFetchConfigurationBuilder setRetries(int retries) {
        this.retries = retries;
        return this;
    }

    public HttpFetchConfigurationBuilder setCacheExpiration(long cacheExpiration) {
        this.cacheExpiration = cacheExpiration;
        return this;
    }

    public HttpFetchConfiguration build() {
        return new HttpFetchConfiguration(basicAuthUsername, basicAuthPassword,
                basicAuthHost, basicAuthPort, sessionCookieUri, sslHostExceptions,
                retries, cacheExpiration);
    }
}