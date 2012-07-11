package com.findwise.tools;

import java.io.IOException;
import java.io.InputStream;
import java.net.ConnectException;
import java.net.Socket;

import org.apache.http.ConnectionReuseStrategy;
import org.apache.http.HttpEntity;
import org.apache.http.HttpEntityEnclosingRequest;
import org.apache.http.HttpException;
import org.apache.http.HttpHost;
import org.apache.http.HttpRequest;
import org.apache.http.HttpRequestInterceptor;
import org.apache.http.HttpResponse;
import org.apache.http.HttpVersion;
import org.apache.http.entity.InputStreamEntity;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.DefaultConnectionReuseStrategy;
import org.apache.http.impl.DefaultHttpClientConnection;
import org.apache.http.message.BasicHttpEntityEnclosingRequest;
import org.apache.http.message.BasicHttpRequest;
import org.apache.http.params.HttpParams;
import org.apache.http.params.HttpProtocolParams;
import org.apache.http.params.SyncBasicHttpParams;
import org.apache.http.protocol.BasicHttpContext;
import org.apache.http.protocol.ExecutionContext;
import org.apache.http.protocol.HttpContext;
import org.apache.http.protocol.HttpProcessor;
import org.apache.http.protocol.HttpRequestExecutor;
import org.apache.http.protocol.ImmutableHttpProcessor;
import org.apache.http.protocol.RequestConnControl;
import org.apache.http.protocol.RequestContent;
import org.apache.http.protocol.RequestExpectContinue;
import org.apache.http.protocol.RequestTargetHost;
import org.apache.http.protocol.RequestUserAgent;

import com.findwise.hydra.common.InternalLogger;

public class HttpConnection {
	private HttpParams params;
	private HttpProcessor httpproc;
	private HttpRequestExecutor httpexecutor;
	private HttpContext context;
	private HttpHost host;
	private DefaultHttpClientConnection conn;
	private ConnectionReuseStrategy connStrategy;

	private static final int MAX_CONNECTION_RETRIES = 10;
	private static final int CONNECTION_RETRY_WAIT_MS = 1000;
	
	public HttpConnection(String hostName, int port) {
		params = new SyncBasicHttpParams();
		HttpProtocolParams.setVersion(params, HttpVersion.HTTP_1_1);
		HttpProtocolParams.setContentCharset(params, "UTF-8");
		HttpProtocolParams.setUserAgent(params, "HttpComponents/1.1");
		HttpProtocolParams.setUseExpectContinue(params, true);
		
		httpproc = new ImmutableHttpProcessor(
				new HttpRequestInterceptor[] {
						// Required protocol interceptors
						new RequestContent(), new RequestTargetHost(),
						// Recommended protocol interceptors
						new RequestConnControl(), new RequestUserAgent(),
						new RequestExpectContinue() });
		
		httpexecutor = new HttpRequestExecutor();
		context = new BasicHttpContext(null);
		host = new HttpHost(hostName, port);
		conn = new DefaultHttpClientConnection();
		context.setAttribute(ExecutionContext.HTTP_CONNECTION, conn);
		context.setAttribute(ExecutionContext.HTTP_TARGET_HOST, host);
		

        connStrategy = new DefaultConnectionReuseStrategy();
	}
	
	public HttpResponse get(String url) throws IOException {
		BasicHttpRequest request = new BasicHttpRequest("GET", url);
		if(!conn.isOpen()) {
			connect();
		}
		return request(request);
	}
	
	public HttpResponse post(String url, String content) throws IOException {
		String printable = (content.length()>100) ? content.substring(0, 100)+" [snip]..." : content;
		InternalLogger.debug("Posting "+printable+" to "+url);
		
		return post(url, new StringEntity(content, "UTF-8"));
	}
	
	public HttpResponse post(String url, InputStream content) throws IOException {
		return post(url, new InputStreamEntity(content, -1));
	}
	
	private HttpResponse post(String url, HttpEntity entity) throws IOException {
		HttpEntityEnclosingRequest request = new BasicHttpEntityEnclosingRequest("POST", url);
		
		if (!conn.isOpen()) {
			connect();
		}
		else if (conn.isStale()) {
			conn.close();
			connect();
		}
		
		request.setEntity(entity);

		return request(request);
	}
	
	public HttpResponse delete(String url) throws IOException {
		HttpRequest request = new BasicHttpRequest("DELETE", url);
		if(!conn.isOpen() || conn.isStale()) {
			connect();
		}
		return request(request);
	}
 	
	private HttpResponse request(HttpRequest request) throws IOException {
		request.setParams(params);
		try {
			httpexecutor.preProcess(request, httpproc, context);
			HttpResponse response = httpexecutor.execute(request, conn, context);
			response.setParams(params);
			httpexecutor.postProcess(response, httpproc, context);

			release(response);

			return response;
		} catch (HttpException e) {
			throw new IOException(e);
		}
	}
	
	private void release(HttpResponse response) throws IOException {
        if (!connStrategy.keepAlive(response, context)) {
            conn.close();
        } else {
            InternalLogger.debug("Keeping connection alive");
        }
	}
	
	private void connect() throws IOException {
		InternalLogger.debug("Connecting socket to "+host.getHostName()+":"+host.getPort());
		try {
			Socket socket = new Socket(host.getHostName(), host.getPort());
			conn.bind(socket, params);
		}
		catch(ConnectException e) {
			InternalLogger.error("Unable to connect to Hydra Core...");
			//Retry a few times before failing
			for (int i = 0; i < MAX_CONNECTION_RETRIES && !conn.isOpen(); i++) {
				try {
					Thread.sleep(CONNECTION_RETRY_WAIT_MS);
					InternalLogger.debug("... retrying connection");
					Socket socket = new Socket(host.getHostName(), host.getPort());
					conn.bind(socket, params);
					InternalLogger.info("Connection to Hydra Core successfully reestablished");
				}
				catch (Exception e2) {
					InternalLogger.debug("The connection to Hydra Core is still failing.");
				}
			}
			if(!conn.isOpen()) {
				InternalLogger.error("Connection to Hydra Core failed and cannot be reestablished.");
				throw e;
			}
		}
	}
}
