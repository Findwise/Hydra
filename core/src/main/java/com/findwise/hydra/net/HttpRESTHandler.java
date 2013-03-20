package com.findwise.hydra.net;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;

import org.apache.http.HttpException;
import org.apache.http.HttpInetConnection;
import org.apache.http.HttpRequest;
import org.apache.http.HttpResponse;
import org.apache.http.protocol.ExecutionContext;
import org.apache.http.protocol.HttpContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.findwise.hydra.CachingDocumentNIO;
import com.findwise.hydra.DatabaseConnector;
import com.findwise.hydra.DatabaseType;
import com.findwise.hydra.NoopCache;
import com.findwise.hydra.PipelineReader;

public class HttpRESTHandler<T extends DatabaseType> implements
		ResponsibleHandler {
	private Logger logger = LoggerFactory.getLogger(HttpRESTHandler.class);

	private CachingDocumentNIO<T> documentIO;
	private PipelineReader pipelineReader;

	private boolean performanceLogging = false;

	private String restId;

	private List<String> allowedHosts;

	private List<InetAddress> resolvedHosts;

	private ResponsibleHandler[] handlers;

	private PingHandler pingHandler;

	private PingHandler getPingHandler() {
		if (pingHandler == null) {
			pingHandler = new PingHandler(restId);
		}
		return pingHandler;
	}
	
	public HttpRESTHandler(DatabaseConnector<T> dbc) {
		this(new CachingDocumentNIO<T>(dbc, new NoopCache<T>()), dbc
				.getPipelineReader(), null, false);
	}

	public HttpRESTHandler(CachingDocumentNIO<T> documentIO,
			PipelineReader pipelineReader, List<String> allowedHosts,
			boolean isPerformanceLogging) {
		this.documentIO = documentIO;
		this.pipelineReader = pipelineReader;
		this.setAllowedHosts(allowedHosts);
		this.performanceLogging = isPerformanceLogging;
	}

	private void createHandlers() {
		handlers = new ResponsibleHandler[] { new FileHandler<T>(documentIO),
				new PropertiesHandler<T>(pipelineReader),
				new MarkHandler<T>(documentIO, performanceLogging),
				new QueryHandler<T>(documentIO, performanceLogging),
				new ReleaseHandler<T>(documentIO),
				new WriteHandler<T>(documentIO, performanceLogging) };
	}

	private ResponsibleHandler[] getHandlers() {
		if (handlers == null) {
			createHandlers();
		}
		return handlers;
	}

	public void setRestId(String restId) {
		getPingHandler().setServerId(restId);
	}

	public boolean dispatch(HttpRequest request, HttpResponse response,
			HttpContext context, ResponsibleHandler... handlers)
			throws HttpException, IOException {
		for (ResponsibleHandler handler : handlers) {
			if (handler.supports(request)) {
				handler.handle(request, response, context);
				return true;
			}
		}
		return false;
	}

	@Override
	public void handle(final HttpRequest request, final HttpResponse response,
			final HttpContext context) {
		if (!accessAllowed(context)) {
			HttpResponseWriter.printAccessDenied(response);
			return;
		}
		try {
			logger.trace("Parsing incoming request");

			if (dispatch(request, response, context, getPingHandler())) {
				return;
			}

			if (dispatch(request, response, context, getHandlers())) {
				return;
			}

			HttpResponseWriter.printUnsupportedRequest(response);
		} catch (Exception e) {
			logger.error("Unhandled exception occurred", e);
			HttpResponseWriter.printUnhandledException(response, e);
			System.exit(1);
		}
	}

	public boolean accessAllowed(HttpContext context) {
		if (allowedHosts == null) {
			return true;
		}
		try {
			HttpInetConnection connection = (HttpInetConnection) context
					.getAttribute(ExecutionContext.HTTP_CONNECTION);
			InetAddress ia = connection.getRemoteAddress();

			if(resolvedHosts.contains(ia)) {
				return true;
			} else {
				logger.error("Caller adress ("+ia+") not in the list of allowed hosts ("+allowedHosts+"). Refusing the connection.");
				return false;
			}
		} catch (Exception e) {
			logger.error("Caught an exception while trying to determine remote address. Refusing the connection.");
		}
		return false;
	}

	@Override
	public boolean supports(HttpRequest request) {
		if (getPingHandler().supports(request)) {
			return true;
		}
		for (ResponsibleHandler handler : handlers) {
			if (handler.supports(request)) {
				return true;
			}
		}
		return false;
	}

	@Override
	public String[] getSupportedUrls() {
		List<String> urls = new ArrayList<String>();
		addArrayToList(getPingHandler().getSupportedUrls(), urls);
		for (ResponsibleHandler handler : getHandlers()) {
			addArrayToList(handler.getSupportedUrls(), urls);
		}
		return urls.toArray(new String[urls.size()]);
	}

	private static <T> void addArrayToList(T[] array, List<T> list) {
		for (T t : array) {
			list.add(t);
		}
	}

	public void setAllowedHosts(List<String> allowedHosts) {
		this.allowedHosts = allowedHosts;
		resolveAllowedHosts();
	}

	private void resolveAllowedHosts() {
		if( allowedHosts == null) {
			resolvedHosts = null;
		} else {
			resolvedHosts = new ArrayList<InetAddress>();
			for(String allowedHost: allowedHosts) {
				try {
					resolvedHosts.add(InetAddress.getByName(allowedHost));
				} catch (UnknownHostException e) {
					/* Fail early if configuration is wrong */
					throw new RuntimeException("Could not resolve host: " + allowedHost, e);
				}
			}
		}
	}

	public List<String> getAllowedHosts() {
		return allowedHosts;
	}
}
