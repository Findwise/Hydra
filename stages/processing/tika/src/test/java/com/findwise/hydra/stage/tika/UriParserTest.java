package com.findwise.hydra.stage.tika;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.net.URI;
import java.net.URL;
import java.util.Arrays;
import java.util.List;

import org.junit.Before;
import org.junit.Test;

public class UriParserTest {

    private UriParser parser;

    @Before
    public void setUp() {
        parser = new UriParser();
    }

    @Test
    public void testURLEncoding() throws Exception {
        String path = "/some spaces in url/and query";
        URI uri = parser.uriFromString("https://user:password@google.com:8080" +
                path + "?q=some space&some%20other%20space#anchor");

        assertEquals(8080, uri.getPort());
        assertEquals("google.com", uri.getHost());
        assertEquals("https", uri.getScheme());
        assertEquals(path, uri.getPath());
        assertEquals(path.replace(" ", "%20"), uri.getRawPath());
    }

    @Test
    public void testGetUrlFromString() throws Exception {
        List<URL> urls = parser.getUrlsFromObject("http://google.com");

        assertEquals(1, urls.size());
        for (URL url : urls) {
            assertEquals("http://google.com", url.toString());
        }
    }

    @Test
    public void testGetUrlsFromList() throws Exception {
        List<String> exp = Arrays.asList("http://google.com", "http://dn.se");
        List<URL> urls = parser.getUrlsFromObject(exp);

        assertEquals(exp.size(), urls.size());
        for (URL url : urls) {
            assertTrue(exp.contains(url.toString()));
        }
    }

    @Test(expected = IllegalArgumentException.class)
    public void testGetUrlFromIncorrectString() throws Exception {
        parser.getUrlsFromObject("a");
    }

    @Test
    public void testUrlWithAtCharacterInThePath() throws Exception {
        String path = "http://hostname:9080/groups/scs/@scswithdrawn/documents/scs/mdaw/mzm5/~edisp/scs_0000377_99.pdf";
        URI uri = parser.uriFromString(path);
        assertEquals("http", uri.getScheme());
        assertEquals("hostname:9080", uri.getAuthority());
        assertNull(uri.getUserInfo());
        assertEquals("hostname", uri.getHost());
        assertEquals(9080, uri.getPort());
        assertEquals("/groups/scs/@scswithdrawn/documents/scs/mdaw/mzm5/~edisp/scs_0000377_99.pdf", uri.getPath());
        assertNull(uri.getQuery());
        assertNull(uri.getFragment());
    }
}
