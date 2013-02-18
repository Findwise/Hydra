package com.findwise.hydra.stage;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import junit.framework.Assert;

import org.junit.Before;
import org.junit.Test;

import com.findwise.hydra.local.LocalDocument;

public class RegexStageTest {

	private RegexStage regexStage;
	private LocalDocument doc;

	@Before
	public void setUp() {
		regexStage = new RegexStage();
		doc = new LocalDocument();
	}

	@Test
	public void testExtractContent() throws RequiredArgumentMissingException,
			ProcessException, IllegalArgumentException, IllegalAccessException {
		List<Map<String, String>> configs = new ArrayList<Map<String, String>>();
		Map<String, String> config1 = new HashMap<String, String>();
		config1.put("inField", "rawcontent");
		config1.put("outField", "out");
		config1.put("regex", "\\Q<![CDATA[<!DOCTYPE html>\\E(.*)\\Q]]>\\E");
		config1.put("substitute", "$1");
		configs.add(config1);
		regexStage.setRegexConfigsList(configs);

		// should match
		doc.putContentField(
				"rawcontent",
				"<![CDATA[<!DOCTYPE html><html><head></head><body><h1 class=\"BIG\">h1 #1</h1><h1>h1 #2</h1><h2>h2 #1</h2><h2>h2 #2</h2></body></html>]]>");
		regexStage.process(doc);
		assertEquals(
				"<html><head></head><body><h1 class=\"BIG\">h1 #1</h1><h1>h1 #2</h1><h2>h2 #1</h2><h2>h2 #2</h2></body></html>",
				doc.getContentField("out").toString());

		// should not match
		LocalDocument doc2 = new LocalDocument();
		doc2.putContentField(
				"rawcontent",
				"<html><head></head><body><h1 class=\"BIG\">h1 #1</h1><h1>h1 #2</h1><h2>h2 #1</h2><h2>h2 #2</h2></body></html>");
		regexStage.process(doc2);
		assertTrue(!doc2.hasContentField("out"));
	}


	@Test
	public void testReplaceString() throws RequiredArgumentMissingException,
			ProcessException, IllegalArgumentException, IllegalAccessException {
		List<Map<String, String>> configs = new ArrayList<Map<String, String>>();
		Map<String, String> config1 = new HashMap<String, String>();
		config1.put("inField", "rawcontent");
		config1.put("outField", "out");
		config1.put("regex", "(^[A-Za-z0-9]+.[A-Za-z0-9]+){1}.*");
		config1.put("substitute", "$1.ru");
		configs.add(config1);
		regexStage.setRegexConfigsList(configs);

		doc.putContentField("rawcontent", "www.giantbomb.com");
		regexStage.process(doc);
		assertEquals("www.giantbomb.ru", doc.getContentField("out").toString());
	}

        @Test
	public void testReplace3Strings() throws RequiredArgumentMissingException,
			ProcessException, IllegalArgumentException, IllegalAccessException {
		List<Map<String, String>> configs = new ArrayList<Map<String, String>>();
		Map<String, String> config1 = new HashMap<String, String>();
		config1.put("inField", "rawcontent");
		config1.put("outField", "out");
		config1.put("regex", "(^[A-Za-z0-9]+)(.)([A-Za-z0-9]+){1}.*");
		config1.put("substitute", "$1$2$3.ru");
		configs.add(config1);
		regexStage.setRegexConfigsList(configs);

		doc.putContentField("rawcontent", "www.giantbomb.com");
		regexStage.process(doc);
		assertEquals("www.giantbomb.ru", doc.getContentField("out").toString());
	}
        
	@Test
	public void testReplaceHTMLStringWithIncludingCharacters() throws RequiredArgumentMissingException,
			ProcessException, IllegalArgumentException, IllegalAccessException {
		List<Map<String, String>> configs = new ArrayList<Map<String, String>>();
		Map<String, String> config1 = new HashMap<String, String>();
		config1.put("inField", "rawcontent");
		config1.put("outField", "out");
                config1.put("regex", "\ufffd*(.*)");
		config1.put("substitute", "$1");
		configs.add(config1);
		regexStage.setRegexConfigsList(configs);

		doc.putContentField("rawcontent", "���Test");
		regexStage.process(doc);
		assertEquals("Test", doc.getContentField("out").toString());
	}

        @Test
	public void testReplaceHTMLStringBracketsAndSuch() throws RequiredArgumentMissingException,
			ProcessException, IllegalArgumentException, IllegalAccessException {
		List<Map<String, String>> configs = new ArrayList<Map<String, String>>();
		Map<String, String> config1 = new HashMap<String, String>();
		config1.put("inField", "rawcontent");
		config1.put("outField", "out");
                config1.put("regex", "<[^>]+>([^<]+)");
		config1.put("substitute", "$1");
		configs.add(config1);
		regexStage.setRegexConfigsList(configs);

		doc.putContentField("rawcontent", "<html><div a=\"b\">this is the only text that should remain</div> after html is <b>removed</b><html>");
		regexStage.process(doc);
		assertEquals("this is the only text that should remain after html is removed", doc.getContentField("out").toString());
	}
	@Test
	public void testMultipleConfigs() throws RequiredArgumentMissingException,
			ProcessException, IllegalArgumentException, IllegalAccessException {
		List<Map<String, String>> configs = new ArrayList<Map<String, String>>();
		Map<String, String> config1 = new HashMap<String, String>();
		config1.put("inField", "rawcontent");
		config1.put("outField", "out");
		config1.put("regex", "(^[A-Za-z0-9]*.[A-Za-z0-9]*).*");
		config1.put("substitute", "$1.ru");

		Map<String, String> config2 = new HashMap<String, String>();
		config2.put("inField", "rawcontent");
		config2.put("outField", "out2");
		config2.put("regex", "^[A-Za-z0-9]*.([A-Za-z0-9]*).*");
		config2.put("substitute", "$1");

		configs.add(config1);
		configs.add(config2);
		regexStage.setRegexConfigsList(configs);
		regexStage.init();

		doc.putContentField("rawcontent", "www.giantbomb.com");
		regexStage.process(doc);
		assertEquals("www.giantbomb.ru", doc.getContentField("out").toString());
		assertEquals("giantbomb", doc.getContentField("out2").toString());
	}

	@Test
	public void testValidateFieldValue()
			throws RequiredArgumentMissingException, ProcessException,
			IllegalArgumentException, IllegalAccessException {
		List<Map<String, String>> configs = new ArrayList<Map<String, String>>();
		Map<String, String> config1 = new HashMap<String, String>();
		config1.put("inField", "rawcontent");
		config1.put("outField", "out");
		config1.put("regex", "^[A-Za-z].*(giantbomb*).*");
		config1.put("substitute", "Gaming website");

		configs.add(config1);
		regexStage.setRegexConfigsList(configs);
		regexStage.init();

		doc.putContentField("rawcontent", "www.giantbomb.com");
		regexStage.process(doc);

		assertEquals("Gaming website", doc.getContentField("out").toString());
	}

	@Test
	public void testContentWithLineBreak()
			throws RequiredArgumentMissingException, ProcessException,
			IllegalArgumentException, IllegalAccessException {
		List<Map<String, String>> configs = new ArrayList<Map<String, String>>();
		Map<String, String> config1 = new HashMap<String, String>();
		config1.put("inField", "rawcontent");
		config1.put("outField", "out");
		config1.put("regex", "\\Q<![CDATA[<!DOCTYPE html>\\E(.*)\\Q]]>\\E");
		config1.put("substitute", "$1");
		configs.add(config1);
		regexStage.setRegexConfigsList(configs);
		regexStage.init();

		// should match
		doc.putContentField("rawcontent",
				"<![CDATA[<!DOCTYPE html><html><head></head><body>\n</body></html>]]>");
		regexStage.process(doc);
		assertEquals("<html><head></head><body>\n</body></html>",
				doc.getContentField("out"));
	}

	
	@Test
	public void testOwerWriteFieldWithNoMatch()
			throws RequiredArgumentMissingException, ProcessException,
			IllegalArgumentException, IllegalAccessException {
		List<Map<String, String>> configs = new ArrayList<Map<String, String>>();
		Map<String, String> config1 = new HashMap<String, String>();
		config1.put("inField", "rawcontent");
		config1.put("outField", "url");
		config1.put("regex", "not real regex since we dont want match");
		config1.put("substitute", "$1");
		configs.add(config1);

		regexStage.setRegexConfigsList(configs);

		regexStage.init();

		doc.putContentField("rawcontent",
				"<![CDATA[<!DOCTYPE html><html><head></head><body>\n</body></html>]]>");
		doc.putContentField("url", "http://www.giantbomb.com");
		regexStage.process(doc);
		assertEquals("http://www.giantbomb.com", doc.getContentField("url"));
	}

	

	@Test
	public void testThatOutfieldNotWrittenWhenNoMatch()
			throws RequiredArgumentMissingException, ProcessException,
			IllegalArgumentException, IllegalAccessException {
		List<Map<String, String>> configs = new ArrayList<Map<String, String>>();
		Map<String, String> config1 = new HashMap<String, String>();
		config1.put("inField", "rawcontent");
		config1.put("outField", "out");
		config1.put("regex", "<div id=\"mainContant\">(.*)<div id=\"footer\">");
		config1.put("substitute", "$1");
		configs.add(config1);
		regexStage.setRegexConfigsList(configs);
		regexStage.init();

		// should match
		doc.putContentField(
				"rawcontent",
				"<html><head><link href=\"http://www.findwise.com\" rel=\"canonical\"></head><body><div id=\"mainContent\">test<div id=\"footer\"></body></html>");
		regexStage.process(doc);
		Assert.assertNull(doc.getContentField("out"));
	}

	
}
