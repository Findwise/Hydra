package com.findwise.hydra.net;

import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.ArrayList;
import java.util.List;

import org.apache.http.HttpException;
import org.apache.http.HttpRequest;
import org.apache.http.HttpResponse;
import org.apache.http.nio.reactor.IOSession;
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
		if(!accessAllowed(context)) {
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
	
	public boolean accessAllowed(HttpContext context) {
		if(allowedHosts==null) {
			return true;
		}
		try {
			Field f = context.getClass().getDeclaredField("iosession");
			boolean accessible = f.isAccessible();
			Field modifiersField = Field.class.getDeclaredField("modifiers");
			int modifiers = f.getModifiers();
			modifiersField.setAccessible(true);
			modifiersField.set(f, f.getModifiers() & ~Modifier.FINAL);
			modifiersField.set(f, f.getModifiers() & ~Modifier.PRIVATE);
			f.setAccessible(true);
			IOSession io = (IOSession) f.get(context);
			f.setAccessible(accessible);
			modifiersField.set(f, modifiers);
			SocketAddress sa = io.getRemoteAddress();
			if(sa instanceof InetSocketAddress) {
				if(allowedHosts.contains(((InetSocketAddress) sa).getHostName())) {
					return true;
				} else {
					logger.error("Connection came from somewhere other than the list of allowed hosts ("+sa+"). Refusing the connection.");
					return false;
				}
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