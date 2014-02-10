package com.findwise.utils.http;

import java.net.URI;
import java.net.URISyntaxException;

/**
 * A {@link UriProvider} which returns the identifier as a URI regardless of the number
 * of attempts.
 */
public class PlainUriProvider implements UriProvider {
	@Override
	public URI getUriFromIdentifier(String identifier, int attempts) throws
			URISyntaxException {
		return new URI(identifier);
	}
}
