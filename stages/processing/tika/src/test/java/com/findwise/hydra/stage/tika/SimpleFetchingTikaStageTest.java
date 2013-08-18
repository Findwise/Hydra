package com.findwise.hydra.stage.tika;

import java.io.FileNotFoundException;
import java.io.InputStream;
import java.util.Arrays;

import org.junit.Assert;
import com.github.tomakehurst.wiremock.junit.WireMockRule;
import static com.github.tomakehurst.wiremock.client.WireMock.*;

import com.google.common.io.ByteStreams;
import org.junit.*;

import org.apache.tika.metadata.Metadata;
import org.apache.tika.parser.AutoDetectParser;
import org.apache.tika.parser.ParseContext;
import org.apache.tika.parser.Parser;
import org.apache.tika.sax.BodyContentHandler;
import org.mockito.Mockito;

import com.findwise.hydra.local.LocalDocument;
import com.findwise.hydra.stage.ProcessException;

public class SimpleFetchingTikaStageTest {

	private SimpleFetchingTikaStage stage;
	private LocalDocument doc;

	private final String pattern = "attachment_(.*)";

	@ClassRule
	public static WireMockRule wireMockRule = new WireMockRule();

	@BeforeClass
	public static void initClass() throws Exception {
		// Make all requests to localhost:8080 return src/test/resources/test.html
		byte[] bytes = ByteStreams.toByteArray(
			SimpleFetchingTikaStageTest.class.getClassLoader().getResourceAsStream("test.html")
		);
		stubFor(get(urlEqualTo("/")).willReturn(aResponse().withBody(bytes)));
	}

	@Before
	public void init() {
		stage = new SimpleFetchingTikaStage();
		stage.setUrlFieldPattern(pattern);

		doc = new LocalDocument();
	}

	@Test(expected = RuntimeException.class)
	public void testProcess() throws Exception {

		doc.putContentField("attachment_a", "http://localhost:8080");

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
		doc.putContentField("attachment_links", Arrays.asList("http://localhost:8080", "http://localhost:8080", "http://localhost:8080"));
		
		stage.process(doc);
		
		Assert.assertTrue(doc.hasContentField("links_content"));
		Assert.assertTrue(doc.hasContentField("links2_content"));
		Assert.assertTrue(doc.hasContentField("links3_content"));
	}
	
	@Test
	public void testURIEscaping() throws Exception {
		doc.putContentField("attachment_a", "http://localhost:8080/ arbitrary path with spaces/");
		
		try {
			stage.process(doc);
			Assert.fail("Did not throw exception, path was incorrect");
		} catch(ProcessException e) {
			Assert.assertEquals(FileNotFoundException.class, e.getCause().getClass());
		}
	}
}
