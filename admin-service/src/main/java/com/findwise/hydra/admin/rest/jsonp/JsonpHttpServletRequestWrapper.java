/*
 * Copyright 2013 ebbesson.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.findwise.hydra.admin.rest.jsonp;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;
import javax.servlet.ServletInputStream;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletRequestWrapper;

/**
 *
 * @author ebbesson
 */
class JsonpHttpServletRequestWrapper extends HttpServletRequestWrapper {
	private final String method;
	private final String bodyContentType;
	private final ByteArrayServletInputStream servletInputStream;
	private BufferedReader reader;

	public JsonpHttpServletRequestWrapper(final HttpServletRequest request, final String method) throws UnsupportedEncodingException {
		super(request);
		this.method = method;
		final String body = request.getParameter(JsonpCallbackFilter.REQUEST_BODY_PARAM_NAME);
		if ((method.equals("POST") || method.equals("PUT")) && body != null) {
			servletInputStream = new ByteArrayServletInputStream(body.getBytes(getCharacterEncoding()));
			bodyContentType = request.getParameter(JsonpCallbackFilter.REQUEST_BODY_CONTENT_TYPE);
		} else {
			servletInputStream = null;
			bodyContentType = null;
		}
	}

	@Override
	public String getMethod() {
		return method;
	}

	@Override
	public int getContentLength() {
		return servletInputStream == null ? super.getContentLength() : servletInputStream.size();
	}

	@Override
	public String getContentType() {
		return bodyContentType == null ? super.getContentType() : bodyContentType;
	}

	@Override
	public ServletInputStream getInputStream() throws IOException {
		return servletInputStream == null ? super.getInputStream() : servletInputStream;
	}

	@Override
	public BufferedReader getReader() throws IOException {
		if (servletInputStream != null && reader == null) {
			reader = new BufferedReader(new InputStreamReader(servletInputStream));
		}
		return reader == null ? super.getReader() : reader;
	}

}
