package com.findwise.hydra.stage;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.findwise.hydra.local.LocalDocument;

public class RegexStageTest {
	private RegexStage regexStage;
	private LocalDocument doc;
	private List<Map<String, String>> configs;
	private Map<String, String> config1;

	@Before
	public void setUp() {
		regexStage = new RegexStage();
		doc = new LocalDocument();
		configs = new ArrayList<Map<String, String>>();
		config1 = new HashMap<String, String>();
		config1.put("inField", "rawcontent");
		config1.put("outField", "out");
	}

	@Test
	public void testExtractContent() throws Exception {
		config1.put("regex", "\\Q<![CDATA[<!DOCTYPE html>\\E(.*)\\Q]]>\\E");
		config1.put("substitute", "$1");
		configs.add(config1);
		setParameters();

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

	private void setParameters() throws IllegalAccessException, IllegalArgumentException, RequiredArgumentMissingException {
		Map<String, Object> parameters = new HashMap<String, Object>();
		parameters.put("regexConfigs", configs);
		regexStage.setParameters(parameters);
	}

	@Test
	public void testReplaceString() throws Exception {
		config1.put("regex", "(^[A-Za-z0-9]+.[A-Za-z0-9]+){1}.*");
		config1.put("substitute", "$1.ru");
		configs.add(config1);
		setParameters();

		doc.putContentField("rawcontent", "www.giantbomb.com");
		regexStage.process(doc);
		assertEquals("www.giantbomb.ru", doc.getContentField("out").toString());
	}

	@Test
	public void testReplace3Strings() throws Exception {
		config1.put("regex", "(^[A-Za-z0-9]+)(.)([A-Za-z0-9]+){1}.*");
		config1.put("substitute", "$1$2$3.ru");
		configs.add(config1);
		setParameters();

		doc.putContentField("rawcontent", "www.giantbomb.com");
		regexStage.process(doc);
		assertEquals("www.giantbomb.ru", doc.getContentField("out").toString());
	}

	@Test
	public void testReplaceHTMLStringWithIncludingCharacters() throws Exception {
		config1.put("regex", "\ufffd*(.*)");
		config1.put("substitute", "$1");
		configs.add(config1);
		setParameters();

		doc.putContentField("rawcontent", "���Test");
		regexStage.process(doc);
		assertEquals("Test", doc.getContentField("out").toString());
	}

	@Test
	public void testReplaceHTMLStringBracketsAndSuch() throws Exception {
		config1.put("regex", "<[^>]+>([^<]+)");
		config1.put("substitute", "$1");
		configs.add(config1);
		setParameters();
		doc.putContentField(
				"rawcontent",
				"<html><div a=\"b\">this is the only text that should remain</div> after html is <b>removed</b><html>");
		regexStage.process(doc);
		assertEquals(
				"this is the only text that should remain after html is removed",
				doc.getContentField("out").toString());
	}

	@Test
	public void testMultipleConfigs() throws Exception {
		config1.put("regex", "(^[A-Za-z0-9]*.[A-Za-z0-9]*).*");
		config1.put("substitute", "$1.ru");

		Map<String, String> config2 = new HashMap<String, String>();
		config2.put("inField", "rawcontent");
		config2.put("outField", "out2");
		config2.put("regex", "^[A-Za-z0-9]*.([A-Za-z0-9]*).*");
		config2.put("substitute", "$1");

		configs.add(config1);
		configs.add(config2);
		setParameters();

		doc.putContentField("rawcontent", "www.giantbomb.com");
		regexStage.process(doc);
		assertEquals("www.giantbomb.ru", doc.getContentField("out").toString());
		assertEquals("giantbomb", doc.getContentField("out2").toString());
	}

	@Test
	public void testValidateFieldValue() throws Exception {
		config1.put("regex", "^[A-Za-z].*(giantbomb*).*");
		config1.put("substitute", "Gaming website");

		configs.add(config1);
		setParameters();

		doc.putContentField("rawcontent", "www.giantbomb.com");
		regexStage.process(doc);

		assertEquals("Gaming website", doc.getContentField("out").toString());
	}

	@Test
	public void testContentWithLineBreak() throws Exception {
		config1.put("regex", "\\Q<![CDATA[<!DOCTYPE html>\\E(.*)\\Q]]>\\E");
		config1.put("substitute", "$1");
		configs.add(config1);
		setParameters();

		// should match
		doc.putContentField("rawcontent",
				"<![CDATA[<!DOCTYPE html><html><head></head><body>\n</body></html>]]>");
		regexStage.process(doc);
		assertEquals("<html><head></head><body>\n</body></html>",
				doc.getContentField("out"));
	}

	@Test
	public void testOwerWriteFieldWithNoMatch() throws Exception {
		config1.put("regex", "not real regex since we dont want match");
		config1.put("substitute", "$1");
		configs.add(config1);

		setParameters();

		doc.putContentField("rawcontent",
				"<![CDATA[<!DOCTYPE html><html><head></head><body>\n</body></html>]]>");
		doc.putContentField("out", "http://www.giantbomb.com");
		regexStage.process(doc);
		assertEquals("http://www.giantbomb.com", doc.getContentField("out"));
	}

	@Test
	public void testThatOutfieldNotWrittenWhenNoMatch() throws Exception {
		config1.put("regex", "<div id=\"mainContant\">(.*)<div id=\"footer\">");
		config1.put("substitute", "$1");
		configs.add(config1);
		setParameters();

		// should match
		doc.putContentField(
				"rawcontent",
				"<html><head><link href=\"http://www.findwise.com\" rel=\"canonical\"></head><body><div id=\"mainContent\">test<div id=\"footer\"></body></html>");
		regexStage.process(doc);
		Assert.assertNull(doc.getContentField("out"));
	}

	@Test
	public void testMultipleMatchesWithoutConcatenationOfMatches() throws Exception {
		config1.put("regex", "23(.*?)87");
		config1.put("substitute", "$1");
		configs.add(config1);
		Map<String, Object> parameters = new HashMap<String, Object>();
		parameters.put("regexConfigs", configs);
		parameters.put("concatenateMatches", false);
		regexStage.setParameters(parameters);
		doc.putContentField("rawcontent",
				"23match187jan23match28723match387nomatch");
		regexStage.process(doc);
		List<String> expected = new ArrayList<String>();
		expected.add("match1");
		expected.add("match2");
		expected.add("match3");
		Assert.assertEquals(expected, doc.getContentField("out"));
	}

	@Test
	public void testGroupInRegexAndSubstitute() throws Exception {
		config1.put("regex", "(\\$1)");
		config1.put("substitute", "$1$1");
		configs.add(config1);
		setParameters();
		doc.putContentField("rawcontent", "$1$1$1");
		regexStage.process(doc);
		Assert.assertEquals("$1$1$1$1$1$1", doc.getContentField("out"));
	}

	@Test
	public void testReverseMatchedGroups() throws Exception {
		config1.put("regex", "(\\$1)(\\$2)");
		config1.put("substitute", "$2$1");
		configs.add(config1);
		setParameters();
		doc.putContentField("rawcontent", "$1$2$1$2");
		regexStage.process(doc);
		Assert.assertEquals("$2$1$2$1", doc.getContentField("out"));
	}

	@Test
	public void testShouldLoopThroughSecondConfigWhenFirstConfigIsMissingInput() throws Exception {
		config1.put("regex", "(.*)");
		config1.put("substitute", "$1");
		configs.add(config1);
		HashMap<String, String> config2 = new HashMap<String, String>();
		config2.put("inField", "rawcontent2");
		config2.put("outField", "out2");
		config2.put("regex", "(.*)");
		config2.put("substitute", "$1");
		configs.add(config2);
		setParameters();
		doc.putContentField("rawcontent2", "content2");
		regexStage.process(doc);
		Assert.assertNull(doc.getContentField("out"));
		Assert.assertEquals("content2", doc.getContentField("out2"));
	}
}
