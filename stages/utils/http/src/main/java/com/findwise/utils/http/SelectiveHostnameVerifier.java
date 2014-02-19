package com.findwise.utils.http;

import java.io.IOException;
import java.security.cert.X509Certificate;
import java.util.List;

import javax.net.ssl.SSLException;
import javax.net.ssl.SSLSession;
import javax.net.ssl.SSLSocket;

import org.apache.http.conn.ssl.X509HostnameVerifier;

public class SelectiveHostnameVerifier implements X509HostnameVerifier {
    private List<String> allowedHosts;
    private X509HostnameVerifier verifier;

    public SelectiveHostnameVerifier(X509HostnameVerifier verifier,
            List<String> allowedHosts) {
        this.verifier = verifier;
        this.allowedHosts = allowedHosts;
    }

    @Override
    public boolean verify(String hostname, SSLSession session) {
        if (allowedHosts.contains(hostname)) {
            return true;
        } else {
            return verifier.verify(hostname, session);
        }
    }

    @Override
    public void verify(String host, SSLSocket ssl) throws IOException {
        if (allowedHosts.contains(host)) {
            return;
        } else {
            verifier.verify(host, ssl);
        }
    }

    @Override
    public void verify(String host, X509Certificate cert) throws SSLException {
        if (allowedHosts.contains(host)) {
            return;
        } else {
            verifier.verify(host, cert);
        }
    }

    @Override
    public void verify(String host, String[] cns, String[] subjectAlts)
            throws SSLException {
        if (allowedHosts.contains(host)) {
            return;
        } else {
            verifier.verify(host, cns, subjectAlts);
        }
    }
}
