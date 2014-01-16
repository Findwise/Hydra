package com.findwise.utils.http;

import static com.github.tomakehurst.wiremock.client.WireMock.aResponse;
import static com.github.tomakehurst.wiremock.client.WireMock.get;
import static com.github.tomakehurst.wiremock.client.WireMock.stubFor;
import static com.github.tomakehurst.wiremock.client.WireMock.urlEqualTo;
import static org.junit.Assert.assertEquals;

import java.io.InputStream;
import java.util.Scanner;

import org.apache.http.HttpEntity;
import org.apache.http.util.EntityUtils;
import org.junit.Rule;
import org.junit.Test;

import com.github.tomakehurst.wiremock.junit.WireMockRule;

public class HttpFetcherTest {

	@Rule
	public WireMockRule wireMockRule = new WireMockRule(37777);

	@Test
	public void testBodyShouldBeFetched() throws Exception {
		HttpFetchConfiguration settings = new HttpFetchConfigurationBuilder().build();
		HttpFetcher httpFetcher = new HttpFetcher(settings);
		stubFor(get(urlEqualTo("/")).willReturn(aResponse().withBody("body")));
		HttpEntity responseEntity = httpFetcher.fetch("http://localhost:37777", "*/*");
		InputStream contentStream = responseEntity.getContent();
		Scanner scanner = new Scanner(contentStream);
		String bodyContent = scanner.useDelimiter("\\A").next();
		scanner.close();
		EntityUtils.consume(responseEntity);
		assertEquals("body", bodyContent);
	}

}
