package com.findwise.hydra.stage;

import com.findwise.hydra.local.LocalDocument;
import org.apache.commons.io.IOUtils;
import org.apache.http.HttpEntity;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpRequestBase;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.Charset;
import java.nio.charset.UnsupportedCharsetException;

/**
 * Simple implementation of a fetching stage.
 */
@Stage(description = "Fetches content over HTTP, using a URL from a field in the document. Outputs content as-is to a specified output field.")
public class SimpleHttpFetchingStage extends AbstractHttpFetchingProcessStage {

	@Parameter(description = "Content-type headers to accept")
	private String acceptedContent = "*/*";

	@Parameter(required = true, description = "Destination field for fetched content")
	private String outputField;

	@Override
	public URI getUriFromIdentifier(String identifier, int attempts) throws URISyntaxException {
		return new URI(identifier);
	}

	@Override
	public void processResponseEntity(HttpEntity responseEntity, LocalDocument doc) throws ProcessException {
		try {
			InputStream inputStream = responseEntity.getContent();
			Charset encoding = Charset.forName(responseEntity.getContentEncoding().getValue());
			String content = IOUtils.toString(inputStream, encoding);
			doc.putContentField(outputField, content);
		} catch (IOException e) {
			throw new ProcessException("Failed to read HTTP entity body", e);
		} catch (UnsupportedCharsetException e) {
			throw new ProcessException("Response used unsupported encoding", e);
		}
	}

	public void setOutputField(String outputField) {
		this.outputField = outputField;
	}

	public void setAcceptedContent(String acceptedContent) {
		this.acceptedContent = acceptedContent;
	}

	@Override
	public String getAcceptedContentHeader() {
		return acceptedContent;
	}

	@Override
	public HttpRequestBase getRequest() {
		return new HttpGet();
	}
}
