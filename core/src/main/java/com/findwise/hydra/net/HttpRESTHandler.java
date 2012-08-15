package com.findwise.hydra.net;

import java.io.IOException;
import java.net.InetAddress;
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

import com.findwise.hydra.DatabaseConnector;
import com.findwise.hydra.DatabaseType;
import com.findwise.hydra.NodeMaster;
import com.google.inject.Inject;

public class HttpRESTHandler<T extends DatabaseType> implements ResponsibleHandler {
	private Logger logger = LoggerFactory.getLogger(HttpRESTHandler.class);
	
	private DatabaseConnector<T> dbc;
	
	private String restId;

	private List<String> allowedHosts;
	
	private ResponsibleHandler[] handlers;
	
	private PingHandler pingHandler;

	private PingHandler getPingHandler() {
		if(pingHandler == null) {
			pingHandler = new PingHandler(restId);
		}
		return pingHandler;
	}
	
	@SuppressWarnings("unchecked")
	@Inject
    public HttpRESTHandler(NodeMaster nm) {
        this((DatabaseConnector<T>)nm.getDatabaseConnector());
    }
	
    public HttpRESTHandler(DatabaseConnector<T> dbc) {
        this(dbc, null);
    }
    
    public HttpRESTHandler(DatabaseConnector<T> dbc, List<String> allowedHosts) {
        this.dbc = dbc;
        this.setAllowedHosts(allowedHosts);
    }
	
	private void createHandlers() {
		handlers = new ResponsibleHandler[] { new FileHandler<T>(dbc), new PropertiesHandler<T>(dbc), new MarkHandler<T>(dbc), new QueryHandler<T>(dbc), new ReleaseHandler<T>(dbc), new WriteHandler<T>(dbc) };
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
		if(!accessAllowed(context)) {
			HttpResponseWriter.printAccessDenied(response);
			return;
		}
		try {
			logger.trace("Parsing incoming request");
			
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
	
	public boolean accessAllowed(HttpContext context) {
		if(allowedHosts==null) {
			return true;
		}
		try {
			HttpInetConnection connection = (HttpInetConnection) context.getAttribute(ExecutionContext.HTTP_CONNECTION);
			InetAddress ia = connection.getRemoteAddress();
			if(allowedHosts.contains(ia.getHostName())) {
				return true;
			} else {
				logger.error("Caller adress ("+ia.getHostName()+") not in the list of allowed hosts ("+allowedHosts+"). Refusing the connection.");
				return false;
			}
		} catch (Exception e) {
			logger.error("Caught an exception while trying to determine remote address. Refusing the connection.");
		} 
		return false;
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

	public void setAllowedHosts(List<String> allowedHosts) {
		this.allowedHosts = allowedHosts;
	}

	public List<String> getAllowedHosts() {
		return allowedHosts;
	}
}