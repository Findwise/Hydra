package com.findwise.hydra.admin.rest.jsonp;

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

    static final String CALLBACK_PARAM_NAME = "callback",
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

}