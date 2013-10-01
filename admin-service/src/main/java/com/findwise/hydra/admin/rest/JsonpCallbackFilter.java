package com.findwise.hydra.admin.rest;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.util.Arrays;
import javax.servlet.Filter;
import javax.servlet.FilterChain;
import javax.servlet.FilterConfig;
import javax.servlet.ServletException;
import javax.servlet.ServletInputStream;
import javax.servlet.ServletOutputStream;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletRequestWrapper;
import javax.servlet.http.HttpServletResponse;
import javax.servlet.http.HttpServletResponseWrapper;
import org.codehaus.jackson.io.JsonStringEncoder;

public class JsonpCallbackFilter implements Filter {

    private static final String CALLBACK_PARAM_NAME = "callback",
                                              REQUEST_METHOD_PARAM_NAME = "_m",
                                              REQUEST_BODY_PARAM_NAME = "_b",
                                              REQUEST_BODY_CONTENT_TYPE = "_ct";
    private static final String[] JSON_MIME_TYPES = {"application/json", "application/x-json", "text/json", "text/x-json"};

    /**
     *
     * @param request
     * @param response
     * @param filterChain
     * @throws IOException
     * @throws ServletException
     */
    @Override
    public void doFilter(ServletRequest request, ServletResponse response, final FilterChain filterChain) throws IOException, ServletException {

        HttpServletRequest httpRequest = (HttpServletRequest) request;

        if (httpRequest.getMethod().equals("GET")) {

            // Change request method
            {
                final String requestMethodOverride = httpRequest.getParameter(REQUEST_METHOD_PARAM_NAME);
                if (requestMethodOverride != null) {
                    request = new JsonpHttpServletRequestWrapper(httpRequest, requestMethodOverride.toUpperCase());
                }
            }

            // Wrap content in callback
            {
                final String callback = httpRequest.getParameter(CALLBACK_PARAM_NAME);
                if (callback != null && !callback.isEmpty()) {

                    response = new JsonpHttpServletResponseWrapper((HttpServletResponse) response, callback);

                }
            }
        }
        filterChain.doFilter(request, response);
        response.flushBuffer();
    }

    public void init(final FilterConfig filterConfig) throws ServletException {
    }

    public void destroy() {
    }


    private static class JsonpHttpServletResponseWrapper extends HttpServletResponseWrapper {

        private static class ByteArrayOutputStream extends ServletOutputStream {

            private byte buf[] = new byte[32];
            private int count;

            private void ensureCapacity(final int minCapacity) {

                if (minCapacity - buf.length > 0) {
                    grow(minCapacity);
                }
            }

            private void grow(final int minCapacity) {

                final int oldCapacity = buf.length;
                int newCapacity = oldCapacity << 1;
                if (newCapacity - minCapacity < 0) {
                    newCapacity = minCapacity;
                }
                if (newCapacity < 0) {

                    if (minCapacity < 0) {
                        throw new OutOfMemoryError();
                    }
                    newCapacity = Integer.MAX_VALUE;
                }
                buf = Arrays.copyOf(buf, newCapacity);
            }

            @Override
            public void write(final int b) {

                ensureCapacity(count + 1);
                buf[count] = (byte) b;
                count += 1;
            }

            public void reset() {

                count = 0;
            }

            public byte toByteArray ()
                [] {

				return Arrays.copyOf(buf, count);
            }

            public int size() {

                return count;
            }
        }
        private final String callback;
        private final JsonpHttpServletResponseWrapper.ByteArrayOutputStream byteArrayOutputStream = new JsonpHttpServletResponseWrapper.ByteArrayOutputStream();
        private PrintWriter printWriter;
        private final static JsonStringEncoder JSON_ENCODER = new JsonStringEncoder();

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

    private static class JsonpHttpServletRequestWrapper extends HttpServletRequestWrapper {

        private static class ByteArrayServletInputStream extends ServletInputStream {

            protected byte buf[];
            private int pos, mark = 0, count;

            public ByteArrayServletInputStream(final byte buf[]) {

                this.buf = buf;
                this.pos = 0;
                this.count = buf.length;
            }

            @Override
            public int read() throws IOException {

                return (pos < count) ? (buf[pos++] & 0xff) : -1;
            }

            @Override
            public long skip(final long n) {

                long k = count - pos;
                if (n < k) {
                    k = n < 0 ? 0 : n;
                }
                pos += k;
                return k;
            }

            @Override
            public int available() {

                return count - pos;
            }

            @Override
            public boolean markSupported() {

                return true;
            }

            @Override
            public void mark(final int readAheadLimit) {

                mark = pos;
            }

            @Override
            public void reset() {

                pos = mark;
            }

            public int size() {

                return buf.length;
            }
        }
        private final String method, bodyContentType;
        private final JsonpHttpServletRequestWrapper.ByteArrayServletInputStream servletInputStream;
        private BufferedReader reader;

        public JsonpHttpServletRequestWrapper(final HttpServletRequest request, final String method) throws UnsupportedEncodingException {

            super(request);

            this.method = method;

            final String body = request.getParameter(REQUEST_BODY_PARAM_NAME);
            if ((method.equals("POST") || method.equals("PUT")) && body != null) {

                servletInputStream = new JsonpHttpServletRequestWrapper.ByteArrayServletInputStream(body.getBytes(getCharacterEncoding()));
                bodyContentType = request.getParameter(REQUEST_BODY_CONTENT_TYPE);
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

}