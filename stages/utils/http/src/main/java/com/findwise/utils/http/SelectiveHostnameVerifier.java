package com.findwise.utils.http;

import java.util.List;

import javax.net.ssl.SSLException;

import org.apache.http.conn.ssl.AbstractVerifier;
import org.apache.http.conn.ssl.X509HostnameVerifier;

public class SelectiveHostnameVerifier extends AbstractVerifier {
    private List<String> allowedHosts;
    private X509HostnameVerifier verifier;

    public SelectiveHostnameVerifier(X509HostnameVerifier verifier,
            List<String> allowedHosts) {
        this.verifier = verifier;
        this.allowedHosts = allowedHosts;
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
