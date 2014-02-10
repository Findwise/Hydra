package com.findwise.utils.http;

import com.github.tomakehurst.wiremock.junit.WireMockRule;
import org.apache.http.HttpEntity;
import org.apache.http.HttpStatus;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.util.EntityUtils;
import org.junit.Rule;
import org.junit.Test;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Scanner;

import static com.github.tomakehurst.wiremock.client.WireMock.aResponse;
import static com.github.tomakehurst.wiremock.client.WireMock.get;
import static com.github.tomakehurst.wiremock.client.WireMock.stubFor;
import static com.github.tomakehurst.wiremock.client.WireMock.urlEqualTo;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class HttpFetcherTest {

	@Rule
	public WireMockRule wireMockRule = new WireMockRule(37777);

	@Test
	public void testBodyShouldBeFetched() throws Exception {
		HttpFetchConfiguration settings = new HttpFetchConfigurationBuilder().build();
		HttpFetcher httpFetcher = new HttpFetcher(settings);
		stubFor(get(urlEqualTo("/")).willReturn(aResponse().withBody("body")));
		HttpEntity responseEntity = httpFetcher.fetch("http://localhost:37777", "*/*");
		String bodyContent = readStream(responseEntity.getContent());
		EntityUtils.consume(responseEntity);
		assertEquals("body", bodyContent);
	}

	@Test
	public void testRetriesRequests() throws IOException {
		final int retries = 3;
		HttpFetchConfiguration settings = new HttpFetchConfigurationBuilder().setRetries(retries).build();
		HttpFetcher httpFetcher = new HttpFetcher(settings);
		stubFor(get(urlEqualTo("/1")).willReturn(aResponse().withStatus(HttpStatus.SC_INTERNAL_SERVER_ERROR)));
		stubFor(get(urlEqualTo("/2")).willReturn(aResponse().withStatus(HttpStatus.SC_INTERNAL_SERVER_ERROR)));
		stubFor(get(urlEqualTo("/3")).willReturn(aResponse().withStatus(HttpStatus.SC_OK).withBody("body")));
		HttpEntity responseEntity = httpFetcher.fetch("http://localhost:37777", "*/*", new IncrementingUriProvider(), new PlainGetRequestProvider());
		String bodyContent = readStream(responseEntity.getContent());
		EntityUtils.consume(responseEntity);
		assertThat(bodyContent, equalTo("body"));
	}

	@Test
	public void testRetriesRequests_for_bad_request() throws IOException {
		final int retries = 2;
		HttpFetchConfiguration settings = new HttpFetchConfigurationBuilder().setRetries(retries).build();
		HttpFetcher httpFetcher = new HttpFetcher(settings);
		stubFor(get(urlEqualTo("/1")).willReturn(aResponse().withStatus(HttpStatus.SC_BAD_REQUEST)));
		stubFor(get(urlEqualTo("/2")).willReturn(aResponse().withStatus(HttpStatus.SC_OK).withBody("body")));
		HttpEntity responseEntity = httpFetcher.fetch("http://localhost:37777", "*/*", new IncrementingUriProvider(), new PlainGetRequestProvider());
		String bodyContent = readStream(responseEntity.getContent());
		EntityUtils.consume(responseEntity);
		assertThat(bodyContent, equalTo("body"));
	}

	@Test
	public void testUsesProviders() throws IOException {
		RequestProvider requestProvider = mock(RequestProvider.class);
		when(requestProvider.getRequest()).thenReturn(new HttpGet());
		HttpFetchConfiguration settings = new HttpFetchConfigurationBuilder().build();
		HttpFetcher httpFetcher = new HttpFetcher(settings);
		stubFor(get(urlEqualTo("/1")).willReturn(aResponse().withStatus(HttpStatus.SC_OK).withBody("body")));
		HttpEntity responseEntity = httpFetcher.fetch("1", "*/*", new AppendingUriProvider(), requestProvider);
		String bodyContent = readStream(responseEntity.getContent());
		EntityUtils.consume(responseEntity);
		assertThat(bodyContent, equalTo("body"));
		verify(requestProvider).getRequest();
	}

	private String readStream(InputStream contentStream) {
		Scanner scanner = new Scanner(contentStream);
		String content = scanner.useDelimiter("\\A").next();
		scanner.close();
		return content;
	}

	private static class IncrementingUriProvider implements UriProvider {
		@Override
		public URI getUriFromIdentifier(String identifier, int attempts) throws URISyntaxException {
			return new URI(identifier + "/" + attempts);
		}
	}

	private static class AppendingUriProvider implements UriProvider {
		@Override
		public URI getUriFromIdentifier(String identifier, int attempts) throws URISyntaxException {
			return new URI("http://localhost:37777/" + identifier);
		}
	}
}
