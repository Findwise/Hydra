package com.findwise.utils.http;

import java.net.URI;
import java.net.URISyntaxException;

public interface UriProvider {
    URI getUriFromIdentifier(String identifier, int attempts) throws
            URISyntaxException;
}