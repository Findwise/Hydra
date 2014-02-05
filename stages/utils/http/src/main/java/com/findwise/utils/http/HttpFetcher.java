package com.findwise.utils.http;

import org.apache.http.Header;
import org.apache.http.HttpEntity;
import org.apache.http.HttpHeaders;
import org.apache.http.HttpResponse;
import org.apache.http.HttpStatus;
import org.apache.http.StatusLine;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.CookieStore;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpHead;
import org.apache.http.client.methods.HttpRequestBase;
import org.apache.http.client.protocol.ClientContext;
import org.apache.http.conn.scheme.PlainSocketFactory;
import org.apache.http.conn.scheme.Scheme;
import org.apache.http.conn.scheme.SchemeRegistry;
import org.apache.http.conn.ssl.SSLSocketFactory;
import org.apache.http.conn.ssl.X509HostnameVerifier;
import org.apache.http.impl.client.BasicCookieStore;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.impl.client.cache.CacheConfig;
import org.apache.http.impl.client.cache.CachingHttpClient;
import org.apache.http.impl.conn.BasicClientConnectionManager;
import org.apache.http.protocol.BasicHttpContext;
import org.apache.http.protocol.HttpContext;
import org.apache.http.util.EntityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.SSLContext;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.List;

/**
 * Generic HTTP fetching utility.
 *
 * <p>
 * This class takes care of opening and closing connections.
 * </p>
 *
 * Supports:
 * <ul>
 * <li>Basic Auth</li>
 * <li>HTTP Response caching</li>
 * <li>SSL, with optional exceptions for trusted hosts</li>
 * <li>Fetching of session cookies</li>
 * </ul>
 *
 * Users of this class is responsible for releasing resources allocated by
 * returned objects of type {@link HttpEntity}.
 *
 * @author olof.nilsson@findwise.com
 * @author martin.nycander@findwise.com
 */
public class HttpFetcher {
	private static final PlainUriProvider PLAIN_URI_PROVIDER = new PlainUriProvider();
	private static final RequestProvider PLAIN_GET_REQUEST_PROVIDER = new PlainGetRequestProvider();
	private final Logger logger = LoggerFactory.getLogger(HttpFetcher.class);
	private final HttpFetchConfiguration settings;

	private final CookieStore cookieStore;
	private final HttpClient client;


	public HttpFetcher(CookieStore cookieStore,
	                   HttpClient client, HttpFetchConfiguration settings) {
		this.cookieStore = cookieStore;
		this.client = client;
		this.settings = settings;
	}

	public HttpFetcher(HttpFetchConfiguration settings) {
		this.settings = settings;
		this.cookieStore = new BasicCookieStore();
		this.client = newDefaultClient();
	}

	public void ensureCookie() throws FailedFetchingCookieException {
		if (settings.hasCookieUri()) {
			updateSessionCookie();
		}
	}

	public void clearCookie() {
		if (settings.hasCookieUri()) {
			cookieStore.clear();
		}
	}

	public HttpEntity fetch(String url, String acceptHeader) throws HttpFetchException {
		return fetch(url, acceptHeader, PLAIN_URI_PROVIDER, PLAIN_GET_REQUEST_PROVIDER);
	}

	public HttpEntity fetch(String identifier,
	                        String acceptHeader,
	                        UriProvider uriProvider, RequestProvider requestProvider) throws HttpFetchException {
		int attemptIndex = 0;
		while (true) {
			HttpResponse response = attemptFetch(identifier, acceptHeader, uriProvider, requestProvider, attemptIndex);
			StatusLine status = response.getStatusLine();
			if (HttpStatus.SC_OK == status.getStatusCode()) {
				return response.getEntity();
			} else if (attemptIndex < settings.getRetries()) { // If retries = 2, then we should attempt 3 times in total
				logger.debug("Retrying identifier '{}' due to response '{}'",
						identifier, status.getStatusCode());
				EntityUtils.consumeQuietly(response.getEntity());
				attemptIndex++;
				continue;
			} else {
				throw new HttpFetchException(
						String.format(
								"Could not process identifier '%s', got response '%s'",
								identifier,
								status.toString()
						)
				);
			}
		}
	}

	private HttpResponse attemptFetch(String identifier, String acceptHeader, UriProvider uriProvider, RequestProvider requestProvider, int attempts) {
		HttpRequestBase request = createRequest(identifier, acceptHeader, uriProvider, requestProvider, attempts);
		try {
			return performRequest(request);
		} catch (IOException e) {
			throw new HttpFetchException("Could not process identifier '"
					+ identifier + "', HTTPClient reported error", e);
		}
	}

	private HttpRequestBase createRequest(String identifier, String acceptHeader, UriProvider uriProvider, RequestProvider requestProvider, int attempts) {
		HttpRequestBase request = requestProvider.getRequest();
		URI uriFromIdentifier = getUri(identifier, uriProvider, attempts);
		request.setURI(uriFromIdentifier);
		request.addHeader(HttpHeaders.ACCEPT, acceptHeader);
		return request;
	}

	private URI getUri(String identifier, UriProvider uriProvider, int attempts) {
		try {
			return uriProvider.getUriFromIdentifier(identifier, attempts);
		} catch (URISyntaxException e) {
			throw new HttpFetchException(
					"Could not construct URI from identifier '" + identifier
							+ "'", e);
		}
	}

	private HttpResponse performRequest(HttpRequestBase request) throws IOException {
		logger.debug("Performing request, uriProvider:'{}', headers:'{}'",
				request.getURI(), request.getMethod(), request.getAllHeaders());
		HttpResponse response = client.execute(request);
		if (logger.isDebugEnabled()) {
			List<String> headers = new ArrayList<String>();
			for (Header header : response.getAllHeaders()) {
				headers.add(header.getName() + "=" + header.getValue());
			}
			logger.debug("Received response, status:'{}', headers:'{}'",
					response.getStatusLine().getStatusCode(), headers);
		}
		return response;
	}

	private HttpClient newDefaultClient() {
		try {
			// Set up SSL exceptions
			SchemeRegistry registry = new SchemeRegistry();
			SSLSocketFactory defaultSocketFactory = SSLSocketFactory.getSocketFactory();
			SSLContext context = SSLContext.getDefault();
			X509HostnameVerifier hostnameVerifier = new SelectiveHostnameVerifier(
					defaultSocketFactory.getHostnameVerifier(),
					settings.getSslHostExceptions());
			SSLSocketFactory socketFactory = new SSLSocketFactory(context,
					hostnameVerifier);
			registry.register(new Scheme("https", 443, socketFactory));
			registry.register(
					new Scheme("http", 80, PlainSocketFactory.getSocketFactory()));
			BasicClientConnectionManager mgr = new BasicClientConnectionManager(registry);
			DefaultHttpClient defaultClient = new DefaultHttpClient(mgr);
			// Set up Basic Auth
			if (settings.shouldUseBasicAuth()) {
				defaultClient.getCredentialsProvider()
						.setCredentials(new AuthScope(
								settings.getBasicAuthHost(),
								settings.getBasicAuthPort()),
								new UsernamePasswordCredentials(
										settings.getBasicAuthUsername(),
										settings.getBasicAuthPassword()));
			}
			// Set up cookies
			defaultClient.setCookieStore(cookieStore);
			// Set up caching
			if (settings.getCacheExpiration() >= 0L) {
				CacheConfig cacheConfig = new CacheConfig();
				cacheConfig.setMaxCacheEntries(1000);
				cacheConfig.setHeuristicCachingEnabled(true);
				cacheConfig.setHeuristicDefaultLifetime(
						settings.getCacheExpiration());
				cacheConfig.setHeuristicCoefficient(0.9f);
				CachingHttpClient cachingClient = new CachingHttpClient(
						defaultClient, cacheConfig);
				return cachingClient;
			} else {
				return defaultClient;
			}
		} catch (NoSuchAlgorithmException e) {
			throw new RuntimeException("Could not create HTTP client", e);
		}
	}

	private void updateSessionCookie() throws FailedFetchingCookieException {
		if (cookieStore.getCookies().isEmpty()) {
			try {
				// Fetch session cookie
				logger.debug("Fetching session cookie from '{}'",
						settings.getSessionCookieUri());
				HttpContext cookieContext = new BasicHttpContext();
				cookieContext.setAttribute(ClientContext.COOKIE_STORE, cookieStore);
				HttpHead cookieRequest = new HttpHead(
						settings.getSessionCookieUri());
				client.execute(cookieRequest, cookieContext);
				cookieRequest.reset();
			} catch (ClientProtocolException e) {
				throw new FailedFetchingCookieException(
						"Failed to fetch cookie at URI '" +
								settings.getSessionCookieUri() + "'", e);
			} catch (IOException e) {
				throw new FailedFetchingCookieException(
						"Failed to fetch cookie at URI '" +
								settings.getSessionCookieUri() + "'", e);
			}
		}
	}

	private RequestProvider newDefaultRequestProvider() {
		return new PlainGetRequestProvider();
	}

	public CookieStore getCookieStore() {
		return cookieStore;
	}

	public HttpClient getClient() {
		return client;
	}

	public HttpFetchConfiguration getConfiguration() {
		return settings;
	}
}
