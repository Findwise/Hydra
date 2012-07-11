package com.findwise.hydra.net;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.http.HttpException;
import org.apache.http.HttpRequest;
import org.apache.http.HttpResponse;
import org.apache.http.protocol.HttpContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.findwise.hydra.DatabaseConnector;
import com.findwise.hydra.DatabaseType;
import com.findwise.hydra.NodeMaster;
import com.google.inject.Inject;

public class HttpRESTHandler<T extends DatabaseType> implements ResponsibleHandler {
	private Logger logger = LoggerFactory.getLogger(HttpRESTHandler.class);
	
	private DatabaseConnector<T> dbc;
	
	private String restId;

	boolean localhostOnly = false;
	
	private ResponsibleHandler[] handlers;
	
	private PingHandler pingHandler;
	
	public boolean isLocalhostOnly() {
		return localhostOnly;
	}

	public void setLocalhostOnly(boolean localhostOnly) {
		this.localhostOnly = localhostOnly;
	}

	private PingHandler getPingHandler() {
		if(pingHandler == null) {
			pingHandler = new PingHandler(restId);
		}
		return pingHandler;
	}
	
	@Inject
    public HttpRESTHandler(NodeMaster nm) {
        this.dbc = (DatabaseConnector<T>)nm.getDatabaseConnector();
    }
	
    public HttpRESTHandler(DatabaseConnector<T> dbc) {
        this.dbc = dbc;
    }
	
	private void createHandlers() {
		handlers = new ResponsibleHandler[] { new FileHandler(dbc), new PropertiesHandler(dbc), new MarkHandler(dbc), new QueryHandler(dbc), new ReleaseHandler(dbc), new WriteHandler(dbc) };
	}
	
	private ResponsibleHandler[] getHandlers() {
		if(handlers==null) {
			createHandlers();
		}
		return handlers;
	}
	
	public void setRestId(String restId) {
		getPingHandler().setServerId(restId);
	}
	
	public boolean dispatch(HttpRequest request, HttpResponse response, HttpContext context, ResponsibleHandler ... handlers) throws HttpException, IOException {
		for(ResponsibleHandler handler : handlers) {
			if(handler.supports(request)) {
				handler.handle(request, response, context);
				return true;
			}
		}
		return false;
	}

	@Override
	public void handle(final HttpRequest request, final HttpResponse response, final HttpContext context) {
		if(!accessAllowed(request)) {
			HttpResponseWriter.printAccessDenied(response);
			return;
		}
		try {
			logger.debug("Parsing incoming request");
			
			if(dispatch(request, response, context, getPingHandler())) {
				return;
			}
			
			if(dispatch(request, response, context, getHandlers())) {
				return;
			}
			
			HttpResponseWriter.printUnsupportedRequest(response);
		} catch (Exception e) {
			logger.error("Unhandled exception occurred", e);
			HttpResponseWriter.printUnhandledException(response, e);
			System.exit(1);
		}
    }
	
	public boolean accessAllowed(HttpRequest request) {
		return true;
	}

	@Override
	public boolean supports(HttpRequest request) {
		if(getPingHandler().supports(request)) {
			return true;
		}
		for(ResponsibleHandler handler : handlers) {
			if(handler.supports(request)) {
				return true;
			}
		}
		return false;
	}

	@Override
	public String[] getSupportedUrls() {
		List<String> urls = new ArrayList<String>();
		addArrayToList(getPingHandler().getSupportedUrls(), urls);
		for(ResponsibleHandler handler : getHandlers()) {
			addArrayToList(handler.getSupportedUrls(), urls);
		}
		return urls.toArray(new String[urls.size()]);
	}
	
	private static <T> void addArrayToList(T[] array, List<T> list) {
		for(T t : array) {
			list.add(t);
		}
	}
}