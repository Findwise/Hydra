package com.findwise.hydra.stage.tika;

import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class UriParser {
    public URI uriFromString(String s) throws URISyntaxException {
        return new URI(s.replace(" ", "%20"));
    }

    public List<URL> getUrlsFromObject(Object urlsObject) throws MalformedURLException, URISyntaxException {
        if (urlsObject instanceof String) {
            return Arrays.asList(uriFromString((String) urlsObject).toURL());
        }
        if (urlsObject instanceof Iterable<?>) {
            List<URL> urls = new ArrayList<URL>();
            for (Object urlObj : (Iterable<?>) urlsObject) {
                urls.addAll(getUrlsFromObject(urlObj));
            }
            return urls;
        }
        return new ArrayList<URL>();
    }
}
