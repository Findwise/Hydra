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

public class JsoupAttrSelectorTest {

	private JsoupAttrSelector jsoup;
	private LocalDocument doc;
        
 	@Before
	public void setUp() {	
		jsoup = new JsoupAttrSelector();
		jsoup.setHtmlField("rawcontent");

		doc = new LocalDocument();
		doc.putContentField("rawcontent", "<html><head><link href=\"http://www.findwise.com\" rel=\"canonical\"></head><body><h1 class=\"BIG\">h1 #1</h1><h1>h1 #2</h1><h2>h2 #1</h2><h2>h2 #2</h2></body></html>");
		
		Map<String, String> config1 = new HashMap<String, String>();
		config1.put("selector", "h1");
		config1.put("attribute", "class");
		config1.put("fieldname", "h1-class");
		config1.put("singlevalue", "true");
		Map<String, String> config2 = new HashMap<String, String>();
		config2.put("selector", "link[rel*=canonical]");
		config2.put("attribute", "href");
		config2.put("fieldname", "canonical-link");
		config2.put("singlevalue", "true");
		Map<String, String> config3 = new HashMap<String, String>();
		config3.put("selector", "*");
		config3.put("attribute", "href");
		config3.put("fieldname", "alllinks");
		config3.put("singlevalue", "false");
		Map<String, String> config4 = new HashMap<String, String>();
		config4.put("selector", "a:has(span)");
		config4.put("attribute", "href");
		config4.put("fieldname", "spanlink");
		config3.put("singlevalue", "false");
		Map<String, String> config5 = new HashMap<String, String>();
		config5.put("selector", "p");
		config5.put("attribute", "id");
		config5.put("fieldname", "pid");
		config5.put("singlevalue", "true");
		List<Map<String, String>> configs = new ArrayList<Map<String, String>>();
		configs.add(config1);
		configs.add(config2);
		configs.add(config3);
        configs.add(config4);
        configs.add(config5);
		jsoup.setjSoupConfigs(configs);
	}

	@Test
	public void testGetSingleH1TagClass() throws ProcessException, RequiredArgumentMissingException, IllegalArgumentException, IllegalAccessException {
		
		jsoup.process(doc);
		
		assertTrue("Expected BIG got " + doc.getContentField("h1-class").toString(),
					doc.getContentField("h1-class").toString().equalsIgnoreCase("BIG"));
	}
	
	@Test
	public void testExtractCanonicalLink() throws ProcessException, RequiredArgumentMissingException, IllegalArgumentException, IllegalAccessException {

		jsoup.process(doc);
		
		assertTrue("Expected http://www.findwise.com got " + doc.getContentField("canonical-link").toString(),
				doc.getContentField("canonical-link").toString().equalsIgnoreCase("http://www.findwise.com"));
	}
	
	@Test
	public void testExtractAllHyperLinksOne() throws ProcessException, RequiredArgumentMissingException, IllegalArgumentException, IllegalAccessException {

		jsoup.process(doc);
						
		assertTrue("Expected [http://www.findwise.com] got " + doc.getContentField("alllinks").toString(),
				doc.getContentField("alllinks").toString().equalsIgnoreCase("[http://www.findwise.com]"));
	}
	
	@Test
	public void testEmptyField() throws ProcessException, RequiredArgumentMissingException, IllegalArgumentException, IllegalAccessException {
		
		doc.putContentField("rawcontent", "");

		jsoup.process(doc);
						
		assertEquals("Expected empty result","[]",doc.getContentField("alllinks").toString());
	}
	

	@Test
	public void testExtractAllHyperLinksSeveral() throws ProcessException, RequiredArgumentMissingException, IllegalArgumentException, IllegalAccessException {

		String content = "<html><head></head><body><p><a href='www.test.com'><span>one<span></a></p><p><a href=\"http://www.findwise.com\"><span>two<span></a></p></body></html>";

		doc.putContentField("othercontent", content);
		String oldHtmlField = jsoup.getHtmlField();
		jsoup.setHtmlField("othercontent");
		jsoup.process(doc);
		jsoup.setHtmlField(oldHtmlField);
		String correct = "[www.test.com, http://www.findwise.com]";
		String result = doc.getContentField("alllinks").toString();
		assertTrue("Expected " + correct + " got " + result,
					result.equalsIgnoreCase(correct));
	}
	
	
	@Test
	public void testUnavailablefield() throws ProcessException, RequiredArgumentMissingException, IllegalArgumentException, IllegalAccessException {
		
		jsoup.process(new LocalDocument());
                assertNull(doc.getContentField("h1-class"));
                assertNull(doc.getContentField("canonical-link"));
                assertNull(doc.getContentField("alllinks"));
	}
	
	@Test
	public void testPseudoSelector() throws ProcessException {

		String content = "<html><head></head><body><p>one</p><p><a href='www.test.com'><span>two<span></a></p></body></html>";

		doc.putContentField("othercontent", content);
		String oldHtmlField = jsoup.getHtmlField();
		jsoup.setHtmlField("othercontent");
		jsoup.process(doc);
		jsoup.setHtmlField(oldHtmlField);
		String correct = "[www.test.com]";
		String result = doc.getContentField("spanlink").toString();
		assertTrue("Expected " + correct + " got " + result,
					result.equalsIgnoreCase(correct));
		
	}
	
	@Test
	public void testParagraphId() throws ProcessException {
		String content = "<html><head></head><body><p id=\"paragraph\">one</p><p>two</p></body></html>";

		doc.putContentField("othercontent", content);
		String oldHtmlField = jsoup.getHtmlField();
		jsoup.setHtmlField("othercontent");
		jsoup.process(doc);
		jsoup.setHtmlField(oldHtmlField);
		String correct = "paragraph";
		String result = doc.getContentField("pid").toString();
		assertTrue("Expected " + correct + " got " + result,
					result.equalsIgnoreCase(correct));
	}
		
}

