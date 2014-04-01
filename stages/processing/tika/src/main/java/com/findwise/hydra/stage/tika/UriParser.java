package com.findwise.hydra.stage.tika;

import org.apache.http.client.utils.URLEncodedUtils;

import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class UriParser {
    public URI uriFromString(String s) throws URISyntaxException {
        String scheme = null;
        int port = -1;
        String userinfo = null;
        String host;
        String path = "";
        String query = null;
        String fragment = null;
        URLEncodedUtils.parse(s, Charset.defaultCharset());
        Matcher m = Pattern.compile("([^:]+)://.*").matcher(s);
        if(m.matches()) {
            scheme = m.group(1);
        }

        m = Pattern.compile("[^:]+://([^@]+)@.*").matcher(s);
        if(m.matches()) {
            userinfo = m.group(1);
            m = Pattern.compile("[^:]+://"+userinfo+"@([^:/]+).*").matcher(s);
        } else {
            m = Pattern.compile("[^:]+://([^:/]+).*").matcher(s);
        }

        if(m.matches()) {
            host = m.group(1);
        } else {
            throw new URISyntaxException(s, "No host specified");
        }


        m = Pattern.compile("[^:]+://.*"+host+":([0-9]+).*").matcher(s);
        if(m.matches()) {
            port = Integer.parseInt(m.group(1));
        }

        m = Pattern.compile("[^:]+://[^/]+(/[^?#]*).*").matcher(s);
        if(m.matches()) {
            path = m.group(1);
        }

        m = Pattern.compile("[^:]+://[^?]+\\?([^#]*).*").matcher(s);
        if(m.matches()) {
            query = m.group(1);
        }

        m = Pattern.compile("[^:]+://[^#]+#(.*)").matcher(s);
        if(m.matches()) {
            fragment = m.group(1);
        }

        return new URI(scheme, userinfo, host, port, path, query, fragment);
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
