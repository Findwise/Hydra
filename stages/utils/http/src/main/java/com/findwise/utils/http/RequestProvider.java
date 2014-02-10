package com.findwise.utils.http;

import org.apache.http.HttpRequest;
import org.apache.http.client.methods.HttpRequestBase;

public interface RequestProvider {
	HttpRequestBase getRequest();
}
