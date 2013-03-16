package com.findwise.hydra.stage.tika;

import java.io.FileNotFoundException;
import java.io.InputStream;
import java.util.Arrays;

import junit.framework.Assert;

import org.apache.tika.metadata.Metadata;
import org.apache.tika.parser.AutoDetectParser;
import org.apache.tika.parser.ParseContext;
import org.apache.tika.parser.Parser;
import org.apache.tika.sax.BodyContentHandler;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import com.findwise.hydra.local.LocalDocument;
import com.findwise.hydra.stage.ProcessException;

public class SimpleFetchingTikaStageTest {

	private SimpleFetchingTikaStage stage;
	private LocalDocument doc;

	private String pattern = "attachment_(.*)";

	@Before
	public void init() {
		stage = new SimpleFetchingTikaStage();
		stage.setUrlFieldPattern(pattern);

		doc = new LocalDocument();
	}

	@Test(expected = RuntimeException.class)
	public void testProcess() throws Exception {

		doc.putContentField("attachment_a", "http://www.google.com");

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
		doc.putContentField("attachment_links", Arrays.asList(new String[] {"http://www.google.com", "http://www.google.com", "http://www.google.com"}));
		
		stage.process(doc);
		
		Assert.assertTrue(doc.hasContentField("links_content"));
		Assert.assertTrue(doc.hasContentField("links2_content"));
		Assert.assertTrue(doc.hasContentField("links3_content"));
	}
	
	@Test
	public void testURIEscaping() throws Exception {
		doc.putContentField("attachment_a", "http://google.com/ arbitrary path with spaces/");
		
		try {
			stage.process(doc);
			Assert.fail("Did not throw exception, path was incorrect");
		} catch(ProcessException e) {
			Assert.assertEquals(FileNotFoundException.class, e.getCause().getClass());
		}
	}
}