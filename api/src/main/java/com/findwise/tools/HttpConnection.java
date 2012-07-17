package com.findwise.tools;

import java.io.IOException;
import java.io.InputStream;

import org.apache.http.HttpEntity;
import org.apache.http.HttpHost;
import org.apache.http.HttpRequest;
import org.apache.http.HttpResponse;
import org.apache.http.HttpVersion;
import org.apache.http.client.methods.HttpDelete;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.InputStreamEntity;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.impl.conn.PoolingClientConnectionManager;
import org.apache.http.params.HttpParams;
import org.apache.http.params.HttpProtocolParams;
import org.apache.http.params.SyncBasicHttpParams;

import com.findwise.hydra.common.InternalLogger;

public class HttpConnection {
	private HttpParams params;
	private HttpHost host;
	private DefaultHttpClient client;
	
	public HttpConnection(String hostName, int port) {
		host = new HttpHost(hostName, port);
		
        PoolingClientConnectionManager cm = new PoolingClientConnectionManager();
        cm.setMaxTotal(10);
		params = new SyncBasicHttpParams();
		HttpProtocolParams.setVersion(params, HttpVersion.HTTP_1_1);
		HttpProtocolParams.setContentCharset(params, "UTF-8");
		HttpProtocolParams.setUserAgent(params, "HttpComponents/1.1");
		HttpProtocolParams.setUseExpectContinue(params, true);
		
        client = new DefaultHttpClient(cm, params);
	}
	
	public HttpResponse get(String url) throws IOException {
		return request(new HttpGet(url));
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
		HttpPost request = new HttpPost(url);
		
		request.setEntity(entity);

		return request(request);
	}
	
	public HttpResponse delete(String url) throws IOException {
		return request(new HttpDelete(url));
	}

	private HttpResponse request(HttpRequest request) throws IOException {
		return client.execute(host, request);
	}
}
