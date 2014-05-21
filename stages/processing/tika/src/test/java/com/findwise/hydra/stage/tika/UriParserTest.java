package com.findwise.hydra.stage.tika;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.Arrays;
import java.util.List;

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
                path + "?q=some space&some other space#anchor");

        Assert.assertEquals(8080, uri.getPort());
        Assert.assertEquals("google.com", uri.getHost());
        Assert.assertEquals("https", uri.getScheme());
        Assert.assertEquals(path, uri.getPath());
        Assert.assertEquals(path.replace(" ", "%20"), uri.getRawPath());
    }

    @Test
    public void testGetUrlFromString() throws Exception {
        List<URL> urls = parser.getUrlsFromObject("http://google.com");

        Assert.assertEquals(1, urls.size());
        for (URL url : urls) {
            Assert.assertEquals("http://google.com", url.toString());
        }
    }

    @Test
    public void testGetUrlsFromList() throws Exception {
        List<String> exp = Arrays.asList("http://google.com", "http://dn.se");
        List<URL> urls = parser.getUrlsFromObject(exp);

        Assert.assertEquals(exp.size(), urls.size());
        for (URL url : urls) {
            Assert.assertTrue(exp.contains(url.toString()));
        }
    }

    @Test(expected = URISyntaxException.class)
    public void testGetUrlFromIncorrectString() throws Exception {
        parser.getUrlsFromObject("a");
    }
}
