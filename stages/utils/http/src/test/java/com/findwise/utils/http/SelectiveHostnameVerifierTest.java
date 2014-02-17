package com.findwise.utils.http;

import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.security.cert.X509Certificate;
import java.util.Arrays;
import java.util.List;

import javax.net.ssl.SSLException;
import javax.net.ssl.SSLSession;
import javax.net.ssl.SSLSocket;

import org.apache.http.conn.ssl.X509HostnameVerifier;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class SelectiveHostnameVerifierTest {

    @Mock
    private X509HostnameVerifier wrappedVerifier;
    private SelectiveHostnameVerifier verifier;

    @Before
    public void setUp() {
        List<String> hostnames = Arrays.asList("one", "two");
        verifier = new SelectiveHostnameVerifier(wrappedVerifier, hostnames);
    }

    @Test
    public void testVerify_allowed_host_session() throws IOException {
        SSLSession session = mock(SSLSession.class);
        when(wrappedVerifier.verify("one", session)).thenReturn(false);
        assertTrue(verifier.verify("one", session));
        verifyNoMoreInteractions(wrappedVerifier);
    }

    @Test
    public void testVerify_calls_wrapped_verifier_session() throws IOException {
        String host = "three";
        SSLSession session = mock(SSLSession.class);
        when(wrappedVerifier.verify(host, session)).thenReturn(true);
        assertTrue(verifier.verify(host, session));
        verify(wrappedVerifier).verify(host, session);
    }

    @Test
    public void testVerify_allowed_host_socket() throws IOException {
        verifier.verify("one", mock(SSLSocket.class));
        verifyNoMoreInteractions(wrappedVerifier);
    }

    @Test
    public void testVerify_calls_wrapped_verifier_socket() throws IOException {
        String host = "three";
        SSLSocket socket = mock(SSLSocket.class);
        verifier.verify(host, socket);
        verify(wrappedVerifier).verify(host, socket);
    }

    @Test(expected = SSLException.class)
    public void testVerify_wrapped_verifier_throws_for_disallowed_socket()
            throws IOException {
        doThrow(new SSLException("failed")).when(wrappedVerifier).verify(
                anyString(), any(SSLSocket.class));
        String host = "three";
        SSLSocket socket = mock(SSLSocket.class);
        verifier.verify(host, socket);
    }

    @Test
    public void testVerify_allowed_host_certificate() throws SSLException {
        verifier.verify("one", mock(X509Certificate.class));
        verifyNoMoreInteractions(wrappedVerifier);
    }

    @Test
    public void testVerify_calls_wrapped_verifier_certificate() throws SSLException {
        String host = "three";
        X509Certificate certificate = mock(X509Certificate.class);
        verifier.verify(host, certificate);
        verify(wrappedVerifier).verify(host, certificate);
    }

    @Test(expected = SSLException.class)
    public void testVerify_wrapped_verifier_throws_for_disallowed_certificate()
            throws SSLException {
        doThrow(new SSLException("failed")).when(wrappedVerifier).verify(
                anyString(), any(X509Certificate.class));
        String host = "three";
        X509Certificate certificate = mock(X509Certificate.class);
        verifier.verify(host, certificate);
    }

    @Test
    public void testVerify_allowed_host_cns() throws SSLException {
        verifier.verify("two", new String[] { "cns" },
                new String[] { "subjectalt1" });
        verifyNoMoreInteractions(wrappedVerifier);
    }

    @Test
    public void testVerify_calls_wrapped_verifier_cns() throws SSLException {
        String host = "three";
        String[] cns = new String[] { "cns" };
        String[] subjectAlts = new String[] { "subjectalt1" };
        verifier.verify(host, cns, subjectAlts);
        verify(wrappedVerifier).verify(host, cns, subjectAlts);
    }

    @Test(expected = SSLException.class)
    public void testVerify_wrapped_verifier_throws_for_disallowed_cns()
            throws SSLException {
        doThrow(new SSLException("failed")).when(wrappedVerifier).verify(
                anyString(), any(String[].class), any(String[].class));
        String host = "three";
        String[] cns = new String[] { "cns" };
        String[] subjectAlts = new String[] { "subjectalt1" };
        verifier.verify(host, cns, subjectAlts);
    }
}
