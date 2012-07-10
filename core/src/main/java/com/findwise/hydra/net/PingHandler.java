package com.findwise.hydra.net;

import java.io.IOException;

import org.apache.http.HttpException;
import org.apache.http.HttpRequest;
import org.apache.http.HttpResponse;
import org.apache.http.protocol.HttpContext;

public class PingHandler implements ResponsibleHandler {
	private String serverId;
	
	private String pingUrl = "";
	
	public PingHandler(String serverId) {
		this.serverId = serverId;
	}
	
	@Override
	public void handle(HttpRequest request, HttpResponse response, HttpContext context)
			throws HttpException, IOException {
		if (RESTTools.isGet(request)) {
			String uri = RESTTools.getStrippedUri(request);
			if (uri.equals(pingUrl)) {
				HttpResponseWriter.printID(response, serverId);
			}
		}
	}

	@Override
	public boolean supports(HttpRequest request) {
		return RESTTools.isGet(request) && pingUrl.equals(RESTTools.getStrippedUri(request));
	}

	@Override
	public String[] getSupportedUrls() {
		return new String[] { pingUrl };
	}

	public void setServerId(String serverId) {
		this.serverId = serverId;
	}
}
