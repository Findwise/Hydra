package com.findwise.hydra.input;

import java.io.IOException;
import java.net.InetSocketAddress;

import org.apache.http.HttpException;
import org.apache.http.HttpResponseInterceptor;
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
import org.apache.http.protocol.HttpRequestHandler;
import org.apache.http.protocol.HttpRequestHandlerRegistry;
import org.apache.http.protocol.ImmutableHttpProcessor;
import org.apache.http.protocol.ResponseConnControl;
import org.apache.http.protocol.ResponseContent;
import org.apache.http.protocol.ResponseDate;
import org.apache.http.protocol.ResponseServer;

import org.slf4j.LoggerFactory;
import org.slf4j.Logger;

/**
 * Sets up a HTTP Input server on the specified port.
 * 
 * @author joel.westberg
 */
@Deprecated
public class HttpInputServer {
    private static Logger logger = LoggerFactory.getLogger(HttpInputServer.class);

	private static final int SOCKET_TIMEOUT_MS = 5000;
	private static final int BUFFER_SIZE = 8 * 1024; // Consider increasing this
														// for performance?
	
	public static final int DEFAULT_LISTEN_PORT = 12002;

	private int port;

	private HttpRequestHandler requestHandler;

	public HttpInputServer(int listenPort, HttpRequestHandler stage) {
		this.requestHandler = stage;
		this.port = listenPort;
	}

	public void init() {
		try {
			HttpParams params = new SyncBasicHttpParams();
			params.setIntParameter(CoreConnectionPNames.SO_TIMEOUT,
					SOCKET_TIMEOUT_MS)
					.setIntParameter(CoreConnectionPNames.SOCKET_BUFFER_SIZE,
							BUFFER_SIZE)
					.setBooleanParameter(
							CoreConnectionPNames.STALE_CONNECTION_CHECK, false)
					.setBooleanParameter(CoreConnectionPNames.TCP_NODELAY, true)
					.setParameter(CoreProtocolPNames.ORIGIN_SERVER,
							"HttpComponents/1.1");

			HttpProcessor httpproc = new ImmutableHttpProcessor(
					new HttpResponseInterceptor[] { new ResponseDate(),
							new ResponseServer(), new ResponseContent(),
							new ResponseConnControl() });

			BufferingHttpServiceHandler handler = new BufferingHttpServiceHandler(
					httpproc, new DefaultHttpResponseFactory(),
					new DefaultConnectionReuseStrategy(), params);

			// Set up request handlers
			HttpRequestHandlerRegistry reqistry = new HttpRequestHandlerRegistry();
			reqistry.register("*", requestHandler);
			handler.setHandlerResolver(reqistry);

			// Provide an event logger
			handler.setEventListener(new EventLogger());

			IOEventDispatch ioEventDispatch = new DefaultServerIOEventDispatch(
					handler, params);
			ListeningIOReactor ioReactor = new DefaultListeningIOReactor(2,
					params);

			ioReactor.listen(new InetSocketAddress(port));
			ioReactor.execute(ioEventDispatch);

		} catch (IOException e) {
			if (e.getCause() instanceof java.net.BindException) {
				logger.error("I/O error in RESTServer", e);
				System.exit(1);
			}

			logger.error("I/O error in RESTServer", e);
		}

	}

	static class EventLogger implements EventListener {
		public void connectionOpen(final NHttpConnection conn) {
			logger.debug("Connection open: " + conn);
		}

		public void connectionTimeout(final NHttpConnection conn) {
			logger.debug("Connection timed out: " + conn);
		}

		public void connectionClosed(final NHttpConnection conn) {
			logger.debug("Connection closed: " + conn);
		}

		public void fatalIOException(final IOException ex,
				final NHttpConnection conn) {
			logger.error("I/O error: " + ex.getMessage());
		}

		public void fatalProtocolException(final HttpException ex,
				final NHttpConnection conn) {
			logger.error("HTTP error: " + ex.getMessage());
		}
	}

}
