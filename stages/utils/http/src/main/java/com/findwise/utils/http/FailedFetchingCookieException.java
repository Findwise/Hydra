package com.findwise.utils.http;

public class FailedFetchingCookieException extends HttpFetchException {
    public FailedFetchingCookieException() {
        super();
    }

    public FailedFetchingCookieException(String s) {
        super(s);
    }

    public FailedFetchingCookieException(String s, Throwable throwable) {
        super(s, throwable);
    }

    public FailedFetchingCookieException(Throwable throwable) {
        super(throwable);
    }
}