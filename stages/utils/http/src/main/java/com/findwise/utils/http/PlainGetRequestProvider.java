package com.findwise.utils.http;

import org.apache.http.HttpRequest;
import org.apache.http.client.methods.HttpGet;

public class PlainGetRequestProvider implements RequestProvider {
	@Override
	public HttpGet getRequest() {
		return new HttpGet();
	}
}
