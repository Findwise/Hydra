package com.findwise.hydra.stage.webstages;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertNull;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.Before;
import org.junit.Test;

import com.findwise.hydra.local.LocalDocument;
import com.findwise.hydra.stage.ProcessException;
import com.findwise.hydra.stage.RequiredArgumentMissingException;
import junit.framework.Assert;

public class JsoupSelectorTest {

	private JsoupSelector jsoup;
	private LocalDocument doc;
        
 	@Before
	public void setUp() {	
		jsoup = new JsoupSelector();
		jsoup.setHtmlField("rawcontent");

		doc = new LocalDocument();
		doc.putContentField("rawcontent", "<html><head><link href=\"http://www.findwise.com\" rel=\"canonical\"></head><body><h1 class=\"BIG\">h1 #1</h1><h1>h1 #2</h1><h2>h2 #1</h2><h2>h2 #2</h2></body></html>");
		
		Map<String, String> config1 = new HashMap<String, String>();
		config1.put("selector", "h1");
		config1.put("fieldname", "h1");
		config1.put("singlevalue", "true");
		Map<String, String> config2 = new HashMap<String, String>();
		config2.put("selector", "h2");
		config2.put("fieldname", "h2");
		config2.put("singlevalue", "false");
		Map<String, String> config3 = new HashMap<String, String>();
		config3.put("selector", "*");
		config3.put("fieldname", "extracted_text");
		config3.put("singlevalue", "true");
		Map<String, String> config4 = new HashMap<String, String>();
		config4.put("selector", "content");
		config4.put("fieldname", "extracted_xml_content");
		config4.put("singlevalue", "true");
		Map<String, String> config5 = new HashMap<String, String>();
		config5.put("selector", "email");
		config5.put("fieldname", "email");
		config5.put("singlevalue", "false");
		Map<String, String> config6 = new HashMap<String, String>();
		config6.put("selector", "name");
		config6.put("fieldname", "name");
		config6.put("singlevalue", "false");
		Map<String, String> config7 = new HashMap<String, String>();
		config7.put("selector", "p");
		config7.put("fieldname", "p");
		config7.put("singlevalue", "false");
		Map<String, String> config8 = new HashMap<String, String>();
		config8.put("selector", "p:has(a)");
		config8.put("fieldname", "pseudo");
		config8.put("singlevalue", "true");
		Map<String, String> config9 = new HashMap<String, String>();
		config9.put("selector", "body");
		config9.put("fieldname", "html");
		config9.put("singlevalue", "true");
		List<Map<String, String>> configs = new ArrayList<Map<String, String>>();
		configs.add(config1);
		configs.add(config2);
		configs.add(config3);
        configs.add(config4);
        configs.add(config5);
        configs.add(config6);
        configs.add(config7);
        configs.add(config8);
        configs.add(config9);
		jsoup.setjSoupConfigs(configs);
	}

	@Test
	public void testGetSingleH1Tag() throws ProcessException, RequiredArgumentMissingException, IllegalArgumentException, IllegalAccessException {
		
		jsoup.process(doc);
		
		assertTrue("Expected h1 #1 got " + doc.getContentField("h1").toString(),
					doc.getContentField("h1").toString().equalsIgnoreCase("h1 #1"));
	}
	
	@Test
	public void testGetMultipleH2Tag() throws ProcessException, RequiredArgumentMissingException, IllegalArgumentException, IllegalAccessException {

		jsoup.process(doc);
		
		assertTrue(doc.getContentField("h2").toString().equalsIgnoreCase("[h2 #1, h2 #2]"));
	}
	
	@Test
	public void testExtractAllText() throws ProcessException, RequiredArgumentMissingException, IllegalArgumentException, IllegalAccessException {

		jsoup.process(doc);
						
		assertTrue(doc.getContentField("extracted_text").toString().equalsIgnoreCase("h1 #1 h1 #2 h2 #1 h2 #2"));
	}
	
	@Test
	public void testExtractAllTextBrokenHTML() throws ProcessException, RequiredArgumentMissingException, IllegalArgumentException, IllegalAccessException {
		
		doc.putContentField("rawcontent", "<p>min fina <b>text</b> komemr hÃ¤r. Den har en <a href=\"http://hej.se <http://hej.se/> \">lÃ¤nk</a> i ocksÃ¥</p>");

		jsoup.process(doc);
						
		assertEquals("min fina text komemr hÃ¤r. Den har en lÃ¤nk i ocksÃ¥",doc.getContentField("extracted_text").toString());
	}
	
	@Test
	public void testEmptyField() throws ProcessException, RequiredArgumentMissingException, IllegalArgumentException, IllegalAccessException {
		
		doc.putContentField("rawcontent", "");

		jsoup.process(doc);
						
		assertNull(doc.getContentField("extracted_text"));
	}
	
	@Test
	public void testUnavaiablefield() throws ProcessException, RequiredArgumentMissingException, IllegalArgumentException, IllegalAccessException {
		
		jsoup.process(new LocalDocument());
                Assert.assertNull(doc.getContentField("extracted_text"));
                Assert.assertNull(doc.getContentField("h1"));
                Assert.assertNull(doc.getContentField("h2"));
	}
	
	@Test
	public void testGetAllPTagsFromList() throws ProcessException {
		
		String content = "<html><head></head><body><p>one</p><p>two</p></body></html>";
		
		List<String> list = new ArrayList<String>();
		list.add(content);
		list.add(content);
		
		doc.putContentField("list", list);
		String oldHtmlField = jsoup.getHtmlField();
		jsoup.setHtmlField("list");
		jsoup.process(doc);
		jsoup.setHtmlField(oldHtmlField);
		String correct = "[one, two, one, two]";
		String result = doc.getContentField("p").toString();
		assertTrue("Expected " + correct + " got " + result,
					result.equalsIgnoreCase(correct));
	}
	
	@Test
	public void testPseudoSelector() throws ProcessException {

		String content = "<html><head></head><body><p>one</p><p><a href=''>two</a></p></body></html>";

		doc.putContentField("othercontent", content);
		String oldHtmlField = jsoup.getHtmlField();
		jsoup.setHtmlField("othercontent");
		jsoup.process(doc);
		jsoup.setHtmlField(oldHtmlField);
		String correct = "two";
		String result = doc.getContentField("pseudo").toString();
		assertTrue("Expected " + correct + " got " + result,
					result.equalsIgnoreCase(correct));
		
	}
	
	@Test
	public void testGetHTML() throws ProcessException {
		String content = "<html><head></head><body><p>one</p><p>two</p></body></html>";

		doc.putContentField("othercontent", content);
		jsoup.setReturnHTML(true);
		String oldHtmlField = jsoup.getHtmlField();
		jsoup.setHtmlField("othercontent");
		jsoup.process(doc);
		jsoup.setHtmlField(oldHtmlField);
		jsoup.setReturnHTML(false);
		String correct = "<body><p>one</p><p>two</p></body>";
		String result = doc.getContentField("html").toString();
		assertTrue("Expected " + correct + " got " + result,
					result.equalsIgnoreCase(correct));
	}
		
}

