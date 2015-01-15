package com.findwise.utils.http;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class HttpFetchConfigurationBuilder {
    private AuthMethod authMethod = AuthMethod.NONE;
    private String basicAuthUsername = null;
    private String basicAuthPassword = null;
    private String basicAuthHost = null;
    private int basicAuthPort = -1;
    private String formBasedLoginUrl = null;
    private Map<String, String> formValues = Collections.emptyMap();
    private String sessionCookieUri = null;
    private List<String> sslHostExceptions = new ArrayList<String>();
    private int retries = 3;
    private long cacheExpiration = -1L;

    public HttpFetchConfigurationBuilder setAuthMethod(AuthMethod authMethod) {
        this.authMethod = authMethod;
        return this;
    }
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

    public HttpFetchConfigurationBuilder setFormBasedLoginUrl(
            String formBasedLoginUrl) {
        this.formBasedLoginUrl = formBasedLoginUrl;
        return this;
    }

    public HttpFetchConfigurationBuilder setFormValues(Map<String, String> formValues) {
        this.formValues = formValues;
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
        return new HttpFetchConfiguration(authMethod, basicAuthUsername,
                basicAuthPassword, basicAuthHost, basicAuthPort,
                formBasedLoginUrl, formValues, sessionCookieUri,
                sslHostExceptions, retries, cacheExpiration);
    }
}