package com.findwise.hydra.net;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.UUID;

import org.apache.http.HttpResponse;
import org.apache.http.HttpResponseInterceptor;
import org.apache.http.HttpStatus;
import org.apache.http.impl.DefaultConnectionReuseStrategy;
import org.apache.http.impl.nio.DefaultHttpServerIODispatch;
import org.apache.http.impl.nio.DefaultNHttpServerConnection;
import org.apache.http.impl.nio.DefaultNHttpServerConnectionFactory;
import org.apache.http.impl.nio.reactor.DefaultListeningIOReactor;
import org.apache.http.nio.NHttpConnectionFactory;
import org.apache.http.nio.NHttpServerConnection;
import org.apache.http.nio.protocol.BasicAsyncRequestHandler;
import org.apache.http.nio.protocol.HttpAsyncRequestHandlerRegistry;
import org.apache.http.nio.protocol.HttpAsyncService;
import org.apache.http.nio.reactor.IOEventDispatch;
import org.apache.http.params.CoreConnectionPNames;
import org.apache.http.params.CoreProtocolPNames;
import org.apache.http.params.HttpParams;
import org.apache.http.params.SyncBasicHttpParams;
import org.apache.http.protocol.HttpProcessor;
import org.apache.http.protocol.ImmutableHttpProcessor;
import org.apache.http.protocol.ResponseConnControl;
import org.apache.http.protocol.ResponseContent;
import org.apache.http.protocol.ResponseDate;
import org.apache.http.protocol.ResponseServer;
import org.apache.http.util.EntityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.findwise.hydra.CoreConfiguration;
import com.findwise.tools.HttpConnection;
import com.google.inject.Inject;
import com.google.inject.Injector;

/**
 * Sets up a REST service on the specified port, or 12001 by default.
 * 
 * @author joel.westberg
 */
public class RESTServer extends Thread {
	private int port;
	private static Logger logger = LoggerFactory.getLogger(RESTServer.class);

	private Exception error;
	
	private DefaultListeningIOReactor ioReactor;

	private static final int BUFFER_SIZE = 8 * 1024; // Consider increasing this
														// for performance?

	private boolean shutdownCalled = false;
	private boolean executing = false;
	
	private HttpRESTHandler<?> requestHandler;
	
	private String id;

	@SuppressWarnings("rawtypes")
	public RESTServer(int port, HttpRESTHandler requestHandler) {
		this.requestHandler = requestHandler;
		id = UUID.randomUUID().toString();
		requestHandler.setRestId(id);
		this.port = port;
		executing = false;
		setDaemon(true);
	}
	
	@SuppressWarnings("rawtypes")
	@Inject
	public RESTServer(CoreConfiguration conf, HttpRESTHandler requestHandler) {
		this(conf.getRestPort(), requestHandler);
	}
	
	public boolean isExecuting() {
		return executing;
	}
	
	public boolean hasError() {
		return error!=null;
	}
	
	public Exception getError() {
		return error;
	}
	
	public int getPort() {
		return port;
	}
	
	/**
	 * Starts the server, confirming that the the startup process went correctly before returning. 
	 * 
	 * @return false if server could not be started (on that port), true otherwise.
	 */
	public boolean blockingStart() {
		start();
		try {
			return isWorking(System.currentTimeMillis(), 2000);
		}
		catch (InterruptedException e) {
			error = e;
			Thread.currentThread().interrupt();
			return false;
		}
	}
	
	public boolean isWorking(long startTime, long timeout) throws InterruptedException {
		if(hasError()) {
			return false;
		}
		if(System.currentTimeMillis()-timeout > startTime) {
			return false;
		}
		if (isExecuting()) {
			HttpConnection conn = new HttpConnection("localhost", port);
			try {
				HttpResponse response = conn.get("/");
				String result = EntityUtils.toString(response.getEntity());
				if(response.getStatusLine().getStatusCode() == HttpStatus.SC_OK) {
					return result.equals(id);
				}
			}
			catch (IOException e) {
				logger.error("An IOException occurred during ping operation", e);
			}
		}
		Thread.sleep(50);
		return isWorking(startTime, timeout);
	}
	
	@Override
	public void run() {
		try {
			HttpParams params = new SyncBasicHttpParams();
			params.setIntParameter(CoreConnectionPNames.SOCKET_BUFFER_SIZE, BUFFER_SIZE)
					.setBooleanParameter(CoreConnectionPNames.STALE_CONNECTION_CHECK, false)
					.setBooleanParameter(CoreConnectionPNames.TCP_NODELAY, true)
					.setParameter(CoreProtocolPNames.ORIGIN_SERVER, "Hydra Core (Apache HttpComponents 4.2.1/1.1)");

			HttpProcessor httpproc = new ImmutableHttpProcessor(new HttpResponseInterceptor[] { 
							new ResponseDate(),
							new ResponseServer(), 
							new ResponseContent(),
							new ResponseConnControl() 
						});

			HttpAsyncRequestHandlerRegistry registry = new HttpAsyncRequestHandlerRegistry();
			registry.register("*", new BasicAsyncRequestHandler(requestHandler));
			
			HttpAsyncService handler = new HttpAsyncService(httpproc, new DefaultConnectionReuseStrategy(), registry, params) {
	            @Override
	            public void connected(final NHttpServerConnection conn) {
	            	logger.debug("Connection open: " + conn);
	                super.connected(conn);
	            }

	            @Override
	            public void closed(final NHttpServerConnection conn) {
	            	logger.debug("Connection closed: " + conn);
	                super.closed(conn);
	            }
			};

	        NHttpConnectionFactory<DefaultNHttpServerConnection> connFactory = new DefaultNHttpServerConnectionFactory(params);
			

	        IOEventDispatch ioEventDispatch = new DefaultHttpServerIODispatch(handler, connFactory);

			ioReactor = new DefaultListeningIOReactor();

			ioReactor.listen(new InetSocketAddress(port));
			executing = true;
			ioReactor.execute(ioEventDispatch);
			if(!shutdownCalled) {
				logger.error("Exiting ioReactor exec loop");
			}
			else {
				logger.debug("Exiting ioReactor exec loop");
			}
			executing = false;
		}
		catch (IOException e) {
			logger.error("I/O error in RESTServer: " + e.getMessage());
			error = e;
		}
	}

	public void shutdown() throws IOException {
		logger.info("Caught shutdown command to RESTServer");
		shutdownCalled = true;
		ioReactor.shutdown();
	}
	
	public static RESTServer getNewStartedRESTServer(Injector injector) {
		return getNewStartedRESTServer(injector.getInstance(CoreConfiguration.class).getRestPort(), injector.getInstance(HttpRESTHandler.class));
	}
	
	public static RESTServer getNewStartedRESTServer(int port, HttpRESTHandler<?> restHandler) {
		RESTServer rest = new RESTServer(port, restHandler);
		
		if(!rest.blockingStart()) {
			return getNewStartedRESTServer((int)(Math.random()*64000)+1024, restHandler);
		}
		
		return rest;
	}
}
