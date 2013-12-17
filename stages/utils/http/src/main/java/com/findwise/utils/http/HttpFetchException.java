package com.findwise.utils.http;

public class HttpFetchException extends RuntimeException {
    public HttpFetchException() {
        super();
    }

    public HttpFetchException(String s) {
        super(s);
    }

    public HttpFetchException(String s, Throwable throwable) {
        super(s, throwable);
    }

    public HttpFetchException(Throwable throwable) {
        super(throwable);
    }
}