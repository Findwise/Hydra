package com.findwise.hydra.net;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.UUID;

import org.apache.http.HttpException;
import org.apache.http.HttpResponse;
import org.apache.http.HttpResponseInterceptor;
import org.apache.http.HttpStatus;
import org.apache.http.impl.DefaultConnectionReuseStrategy;
import org.apache.http.impl.DefaultHttpResponseFactory;
import org.apache.http.impl.nio.DefaultServerIOEventDispatch;
import org.apache.http.impl.nio.reactor.DefaultListeningIOReactor;
import org.apache.http.nio.NHttpConnection;
import org.apache.http.nio.protocol.BufferingHttpServiceHandler;
import org.apache.http.nio.protocol.EventListener;
import org.apache.http.nio.reactor.IOEventDispatch;
import org.apache.http.nio.reactor.ListeningIOReactor;
import org.apache.http.params.CoreConnectionPNames;
import org.apache.http.params.CoreProtocolPNames;
import org.apache.http.params.HttpParams;
import org.apache.http.params.SyncBasicHttpParams;
import org.apache.http.protocol.HttpProcessor;
import org.apache.http.protocol.HttpRequestHandlerRegistry;
import org.apache.http.protocol.ImmutableHttpProcessor;
import org.apache.http.protocol.ResponseConnControl;
import org.apache.http.protocol.ResponseContent;
import org.apache.http.protocol.ResponseDate;
import org.apache.http.protocol.ResponseServer;
import org.apache.http.util.EntityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.findwise.hydra.Configuration;
import com.findwise.hydra.CoreConfiguration;
import com.findwise.tools.HttpConnection;
import com.google.inject.Inject;
import com.google.inject.Injector;
import com.google.inject.name.Named;

/**
 * Sets up a REST service on the specified port, or 12001 by default.
 * 
 * @author joel.westberg
 */
public class RESTServer extends Thread {
	private int port;
	private static Logger logger = LoggerFactory.getLogger(RESTServer.class);

	private Exception error;
	
	private ListeningIOReactor ioReactor;

	private static final int SOCKET_TIMEOUT_MS = 5000;
	private static final int BUFFER_SIZE = 8 * 1024; // Consider increasing this
														// for performance?

	private boolean shutdownCalled = false;
	private boolean executing = false;
	
	private HttpRESTHandler<?> requestHandler;
	
	private String id;

	@Inject
	public RESTServer(@Named(Configuration.REST_PORT_PARAM) int port, HttpRESTHandler requestHandler) {
		this.requestHandler = requestHandler;
		id = UUID.randomUUID().toString();
		requestHandler.setRestId(id);
		this.port = port;
		executing = false;
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
			return isWorking(System.currentTimeMillis(), SOCKET_TIMEOUT_MS*2);
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
				e.printStackTrace();
			}
		}
		Thread.sleep(50);
		return isWorking(startTime, timeout);
	}
	
	@Override
	public void run() {
		try {
			HttpParams params = new SyncBasicHttpParams();
			params.setIntParameter(CoreConnectionPNames.SO_TIMEOUT, SOCKET_TIMEOUT_MS)
					.setIntParameter(CoreConnectionPNames.SOCKET_BUFFER_SIZE, BUFFER_SIZE)
					.setBooleanParameter(CoreConnectionPNames.STALE_CONNECTION_CHECK, false)
					.setBooleanParameter(CoreConnectionPNames.TCP_NODELAY, true)
					.setParameter(CoreProtocolPNames.ORIGIN_SERVER, "Hydra Core RESTServer (HttpComponents/1.1)");

			HttpProcessor httpproc = new ImmutableHttpProcessor(new HttpResponseInterceptor[] { new ResponseDate(),
					new ResponseServer(), new ResponseContent(), new ResponseConnControl() });

			BufferingHttpServiceHandler handler = new BufferingHttpServiceHandler(httpproc,
					new DefaultHttpResponseFactory(), new DefaultConnectionReuseStrategy(), params);

			// Set up request handlers
			HttpRequestHandlerRegistry reqistry = new HttpRequestHandlerRegistry();
			reqistry.register("*", requestHandler);
			handler.setHandlerResolver(reqistry);

			// Provide an event logger
			handler.setEventListener(new EventLogger());

			IOEventDispatch ioEventDispatch = new DefaultServerIOEventDispatch(handler, params);
			ioReactor = new DefaultListeningIOReactor(2, params);

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
	
	public static RESTServer getNewStartedRESTServer(int port, HttpRESTHandler restHandler) {
		RESTServer rest = new RESTServer(port, restHandler);
		
		if(!rest.blockingStart()) {
			return getNewStartedRESTServer((int)(Math.random()*64000)+1024, restHandler);
		}
		
		return rest;
	}

	static class EventLogger implements EventListener {
		@Override
		public void connectionOpen(final NHttpConnection conn) {
			logger.info("Connection open: " + conn);
		}

		@Override
		public void connectionTimeout(final NHttpConnection conn) {
			logger.info("Connection timed out: " + conn);
		}

		@Override
		public void connectionClosed(final NHttpConnection conn) {
			logger.info("Connection closed: " + conn);
		}

		@Override
		public void fatalIOException(final IOException ex, final NHttpConnection conn) {
			logger.error("I/O error: " + ex.getMessage());
		}

		@Override
		public void fatalProtocolException(final HttpException ex, final NHttpConnection conn) {
			logger.error("HTTP error: " + ex.getMessage());
		}

	}
}