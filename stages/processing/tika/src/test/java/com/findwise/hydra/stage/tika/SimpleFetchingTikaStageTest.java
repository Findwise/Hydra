package com.findwise.hydra.stage.tika;

import static com.github.tomakehurst.wiremock.client.WireMock.aResponse;
import static com.github.tomakehurst.wiremock.client.WireMock.get;
import static com.github.tomakehurst.wiremock.client.WireMock.stubFor;
import static com.github.tomakehurst.wiremock.client.WireMock.urlEqualTo;
import static com.github.tomakehurst.wiremock.core.WireMockConfiguration.wireMockConfig;

import java.io.InputStream;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import org.apache.tika.metadata.Metadata;
import org.apache.tika.parser.AutoDetectParser;
import org.apache.tika.parser.ParseContext;
import org.apache.tika.parser.Parser;
import org.apache.tika.sax.BodyContentHandler;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.mockito.Mockito;

import com.findwise.hydra.local.LocalDocument;
import com.findwise.hydra.stage.ProcessException;
import com.findwise.utils.http.HttpFetchException;
import com.github.tomakehurst.wiremock.junit.WireMockRule;
import com.google.common.io.ByteStreams;

public class SimpleFetchingTikaStageTest {

	private SimpleFetchingTikaStage stage;
	private LocalDocument doc;

	private final String pattern = "attachment_(.*)";

	private static final String mockHost = "localhost";
	private static final int mockPort = 37777;
	private static final String mockUrl = "http://" + mockHost + ":" + mockPort;

	@ClassRule
	public static WireMockRule wireMockRule = new WireMockRule(wireMockConfig().port(mockPort));

	@BeforeClass
	public static void initClass() throws Exception {
		// Make all requests to the mock host and port return src/test/resources/test.html
		byte[] bytes = ByteStreams.toByteArray(
			SimpleFetchingTikaStageTest.class.getClassLoader().getResourceAsStream("test.html")
		);
		stubFor(get(urlEqualTo("/")).willReturn(aResponse().withBody(bytes)));
	}

	@Before
	public void init() throws Exception {
		stage = new SimpleFetchingTikaStage();
		Map<String, Object> params = new HashMap<String, Object>();
		params.put("urlFieldPattern", pattern);
		stage.setParameters(params);
		stage.init();

		doc = new LocalDocument();
	}

	@Test(expected = RuntimeException.class)
	public void testProcess() throws Exception {

		doc.putContentField("attachment_a", mockUrl);

		Parser parser = Mockito.mock(AutoDetectParser.class);
		stage.setParser(parser);

		Mockito.doThrow(new RuntimeException())
				.when(parser)
				.parse(Mockito.any(InputStream.class),
						Mockito.any(BodyContentHandler.class),
						Mockito.any(Metadata.class),
						Mockito.any(ParseContext.class));
		stage.process(doc);
	}
	
	@Test
	public void testListAttachments() throws Exception {
		doc.putContentField("attachment_links", Arrays.asList(mockUrl, mockUrl, mockUrl));
		
		stage.process(doc);
		
		Assert.assertTrue(doc.hasContentField("links_content"));
		Assert.assertTrue(doc.hasContentField("links2_content"));
		Assert.assertTrue(doc.hasContentField("links3_content"));
	}
	
	@Test
	public void testURIEscaping() throws Exception {
		doc.putContentField("attachment_a", mockUrl + "/ arbitrary path with spaces/");
		
		try {
			stage.process(doc);
			Assert.fail("Did not throw exception, path was incorrect");
		} catch(ProcessException e) {
			Assert.assertEquals(HttpFetchException.class, e.getCause().getClass());
		}
	}
}
