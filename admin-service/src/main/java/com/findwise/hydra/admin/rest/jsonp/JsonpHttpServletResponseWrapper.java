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

import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.util.Arrays;
import javax.servlet.ServletOutputStream;
import javax.servlet.http.HttpServletResponse;
import javax.servlet.http.HttpServletResponseWrapper;
import org.codehaus.jackson.io.JsonStringEncoder;

/**
 *
 * @author ebbesson
 */
class JsonpHttpServletResponseWrapper extends HttpServletResponseWrapper {
	private final String callback;
	private final ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
	private PrintWriter printWriter;
	private static final JsonStringEncoder JSON_ENCODER = new JsonStringEncoder();

	public JsonpHttpServletResponseWrapper(final HttpServletResponse response, final String callback) {
		super(response);
		this.callback = callback;
	}

	@Override
	public String getContentType() {
		return "application/javascript";
	}

	@Override
	public PrintWriter getWriter() throws IOException {
		if (printWriter == null) {
			printWriter = new PrintWriter(new OutputStreamWriter(byteArrayOutputStream, this.getCharacterEncoding()));
		}
		return printWriter;
	}

	@Override
	public ServletOutputStream getOutputStream() throws IOException {
		return byteArrayOutputStream;
	}

	@Override
	public void setBufferSize(final int size) {
		byteArrayOutputStream.ensureCapacity(size);
	}

	@Override
	public int getBufferSize() {
		return byteArrayOutputStream.size();
	}

	@Override
	public void reset() {
		getResponse().reset();
		byteArrayOutputStream.reset();
	}

	@Override
	public void resetBuffer() {
		reset();
	}

	@Override
	public void flushBuffer() throws IOException {
		if (printWriter != null) {
			printWriter.close();
		}
		byteArrayOutputStream.close();
		String content = new String(byteArrayOutputStream.toByteArray(), "UTF-8");
		byte[] bytes = (callback + "(" + content + ");").getBytes(getCharacterEncoding());
		getResponse().setContentLength(bytes.length);
		getResponse().setContentType(getContentType());
		getResponse().getOutputStream().write(bytes);
		getResponse().flushBuffer();
	}

}
