package com.findwise.hydra.stage.tika.utils;


import static org.junit.Assert.fail;

import java.io.StringWriter;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import junit.framework.Assert;

import org.apache.tika.metadata.Metadata;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.findwise.hydra.local.LocalDocument;

public class TikaUtilsTest {
	private LocalDocument doc;
	
	private String pattern = "attachment_(.*)";


	@Before
	public void setUp() throws Exception {
		doc = new LocalDocument();
	}

	@After
	public void tearDown() throws Exception {
	}
	@Test
	public void testGetUrls() {
		doc.putContentField("title", "title");
		doc.putContentField("attachment_a", "a");
		doc.putContentField("attachment_b", "b");
		doc.putContentField("attachment_c", "c");

		Map<String, Object> ret = TikaUtils.getFieldMatchingPattern(doc,
				pattern);

		Assert.assertEquals(3, ret.size());
		Assert.assertEquals("a", ret.get("a"));
		Assert.assertEquals("b", ret.get("b"));
		Assert.assertEquals("c", ret.get("c"));
	}

	@Test
	public void testGetUrlsNoGroup() {
		doc.putContentField("title", "title");
		doc.putContentField("attachment_a", "a");
		doc.putContentField("attachment_b", "b");
		doc.putContentField("attachment_c", "c");

		Map<String, Object> ret = TikaUtils.getFieldMatchingPattern(doc,
				"attachment_.*");

		Assert.assertEquals(3, ret.size());
		Assert.assertEquals("a", ret.get("attachment_a"));
		Assert.assertEquals("b", ret.get("attachment_b"));
		Assert.assertEquals("c", ret.get("attachment_c"));
	}

	@Test
	public void testGetUrlFromString() throws Exception {
		List<URL> urls = TikaUtils.getUrlsFromObject("http://google.com");

		Assert.assertEquals(1, urls.size());
		for (URL url : urls)
			Assert.assertEquals("http://google.com", url.toString());
	}

	@Test
	public void testGetUrlsFromList() throws Exception {
		List<String> exp = Arrays.asList("http://google.com", "http://dn.se");
		List<URL> urls = TikaUtils.getUrlsFromObject(exp);

		Assert.assertEquals(exp.size(), urls.size());
		for (URL url : urls) {
			Assert.assertTrue(exp.contains(url.toString()));
		}
	}

	@Test(expected = MalformedURLException.class)
	public void testGetUrlFromIncorrectString() throws Exception {
		TikaUtils.getUrlsFromObject("a");
	}

	@Test
	public void testAddTextToDocument() {
		StringWriter textData = new StringWriter();
		String text = "My text";
		textData.append(text);

		String fieldPrefix = "a_";

		TikaUtils.addTextToDocument(doc, fieldPrefix, textData);

		String field = (String) doc.getContentField(fieldPrefix + "content");
		if (!text.equals(field)) {
			fail("Expected: " + text + " got: " + field);
		}
	}

	@Test
	public void testAddMetadataToDocument() {
		Metadata meta = new Metadata();
		meta.set("author", "Simon");
		meta.set("pages", "5");

		String fieldPrefix = "a_";

		TikaUtils.addMetadataToDocument(doc, fieldPrefix, meta);

		String author = (String) doc.getContentField(fieldPrefix + "author");
		String pages = (String) doc.getContentField(fieldPrefix + "pages");

		if (!"Simon".equals(author)) {
			fail("Expected Simon Got: " + author + " in field " + fieldPrefix
					+ "author");
		}

		if (!"5".equals(pages)) {
			fail("Expected 5 Got: " + pages + " in field " + fieldPrefix
					+ "pages");
		}

	}

	@Test
	public void testAddMultiValueMetadataToDocument() {
		Metadata meta = new Metadata();
		meta.add("author", "Anton");
		meta.add("author", "Simon");

		String fieldPrefix = "a_";

		TikaUtils.addMetadataToDocument(doc, fieldPrefix, meta);

		@SuppressWarnings("unchecked")
		List<String> author = (List<String>) doc.getContentField(fieldPrefix
				+ "author");

		if (!author.contains("Anton")) {
			fail("Missing Anton in authors");
		}

		if (!author.contains("Simon")) {
			fail("Missing Simon in authors");
		}

	}
}
