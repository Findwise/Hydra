package com.findwise.hydra.net;

import java.util.Locale;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.http.HttpRequest;

import com.findwise.hydra.local.RemotePipeline;

public final class RESTTools {
	public enum Method { GET, PUT, POST, DELETE, HEAD, TRACE, CONNECT, PATCH, OPTIONS };
	
	public static String getUri(HttpRequest request) {
		return request.getRequestLine().getUri();
	}

	public static String getStrippedUri(HttpRequest request) {
		return getUri(request).substring(1);
	}
	
	public static Method getMethod(HttpRequest request) {
		return Method.valueOf(request.getRequestLine().getMethod().toUpperCase(Locale.ENGLISH));
	}
	
	public static String getParam(HttpRequest request, String param) {
		String uri = getUri(request);
		Pattern p = Pattern.compile("[^\\?]+\\?.*"+param+"=([^&]+)");
		Matcher m = p.matcher(uri);
		if(!m.find()) {
			return null;
		}
		return m.group(1);
	}
	
	public static String getBaseUrl(HttpRequest request) {
		String uri = getStrippedUri(request);
		Pattern p = Pattern.compile("([^\\?]+).*");
		Matcher m = p.matcher(uri);
		if(!m.find()) {
			return null;
		}
		return m.group(1);
	}
	
	public static String getStage(HttpRequest request) {
		return getParam(request, RemotePipeline.STAGE_PARAM);
	}
	
	public static boolean isPost(HttpRequest request) {
		return getMethod(request) == Method.POST;
	}
	
	public static boolean isGet(HttpRequest request) {
		return getMethod(request) == Method.GET;
	}
}
