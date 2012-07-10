package com.findwise.hydra.net;

import java.io.IOException;

import org.apache.http.HttpException;
import org.apache.http.HttpRequest;
import org.apache.http.HttpResponse;
import org.apache.http.protocol.HttpContext;
import org.apache.http.protocol.HttpRequestHandler;

public interface ResponsibleHandler extends HttpRequestHandler {

	/**
	 * Implementing classes should guarantee expected behavior if and only if
	 * supports(request) returns true.
	 */
	@Override
	public void handle(HttpRequest request, HttpResponse response,
			HttpContext context) throws HttpException, IOException;

	/**
	 * Indicates that this handler can serve the given request by a later call
	 * to handle(...)
	 */
	public boolean supports(HttpRequest request);

	public String[] getSupportedUrls();
}
