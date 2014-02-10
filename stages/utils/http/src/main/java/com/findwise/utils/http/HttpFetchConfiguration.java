package com.findwise.utils.http;

import java.util.List;

public class HttpFetchConfiguration {
	// Username for Basic Auth
	private final String basicAuthUsername;
	// Password for Basic Auth
	private final String basicAuthPassword;
	// Host for Basic Auth
	private final String basicAuthHost;
	// Port for Basic Auth
	private final int basicAuthPort;
	// URI to retrieve session cookie from
	private final String sessionCookieUri;
	// List of hostnames for which to accept invalid SSL certificates, default empty
	private final List<String> sslHostExceptions;
	// Number of retries. May be to fallback URLs
	private final int retries;
	// Expiration time for cached responses, in seconds. Any positive value enables caching. Default -1
	private final long cacheExpiration;

	public HttpFetchConfiguration(String basicAuthUsername,
	                              String basicAuthPassword,
	                              String basicAuthHost,
	                              int basicAuthPort,
	                              String sessionCookieUri,
	                              List<String> sslHostExceptions,
	                              int retries,
	                              long cacheExpiration) {
		this.basicAuthUsername = basicAuthUsername;
		this.basicAuthPassword = basicAuthPassword;
		this.basicAuthHost = basicAuthHost;
		this.basicAuthPort = basicAuthPort;
		this.sessionCookieUri = sessionCookieUri;
		this.sslHostExceptions = sslHostExceptions;
		this.retries = retries;
		this.cacheExpiration = cacheExpiration;
	}

	public boolean hasCookieUri() {
		return getSessionCookieUri() != null;
	}

	public boolean shouldUseBasicAuth() {
		return getBasicAuthUsername() != null && getBasicAuthPassword() != null;
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
