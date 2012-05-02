package com.findwise.hydra.stage.webstages;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

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
		doc.putContentField("rawcontent", "<html><head><link href=\"http://www.skolverket.se/forskola_och_skola/2.601/2.2080/stodmaterial-1.121555\" rel=\"canonical\"></head><body><h1 class=\"BIG\">h1 #1</h1><h1>h1 #2</h1><h2>h2 #1</h2><h2>h2 #2</h2></body></html>");
		
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
    public void extractConentFromXml() throws ProcessException{
        LocalDocument doc2 = new LocalDocument();
        doc2.putContentField("rawcontent", "<entry  xmlns=\"http://www.w3.org/2005/Atom\" xmlns:app=\"http://www.w3.org/2007/app\" xmlns:snx=\"http://www.ibm.com/xmlns/prod/sn\"><content><p> This is a community for anyone interested in or working within the area User Experience which can include usability, concept design, information design,&nbsp;graphic design,&nbsp;interaction design, movement design, etc. The community is new and will be as good as we make it, so do contribute!</p> <p> In the UX-portal:</p> <ul> <li> We <strong>blog </strong>about our field</li> <li> Ask us&nbsp;a question in the <strong>Forum</strong></li> <li> Design strategies, instructions&nbsp;and FAQ:s in the <strong>Wiki</strong></li> <li> A&nbsp;collection of UX <strong>Files&nbsp;</strong>and <strong>Bookmarks </strong>(links) concearning UX</li> </ul> <p> In the UX sub-communities:</p> <ul> <li> UX research and competitive intelligence - omv&auml;rldsbevakning</li> <li> Reuse UX&nbsp;- material vi kan &aring;teranv&auml;nda</li> <li> Designforum UX - m&ouml;tesagenda och protokoll f&ouml;r forumet d&auml;r vi lyfter&nbsp;UX-fr&aring;gor och omv&auml;rldsbevakar</li> <li> SEB:s User Experience Day&nbsp;- v&aring;rt &aring;rliga event p&aring; Sergels torg och i Rissne</li> </ul></content></entry>");
        jsoup.process(doc2);
        assertEquals(doc2.getContentField("extracted_xml_content").toString(),"This is a community for anyone interested in or working within the area User Experience which can include usability, concept design, information design, graphic design, interaction design, movement design, etc. The community is new and will be as good as we make it, so do contribute! In the UX-portal: We blog about our field Ask us a question in the Forum Design strategies, instructions and FAQ:s in the Wiki A collection of UX Files and Bookmarks (links) concearning UX In the UX sub-communities: UX research and competitive intelligence - omvärldsbevakning Reuse UX - material vi kan återanvända Designforum UX - mötesagenda och protokoll för forumet där vi lyfter UX-frågor och omvärldsbevakar SEB:s User Experience Day - vårt årliga event på Sergels torg och i Rissne");
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
						
		assertEquals("",doc.getContentField("extracted_text").toString());
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
	
	@Test
	public void testExtractConnectionsAPIMembersNames() throws ProcessException {
		
		doc.putContentField("rawcontent", "<?xml version=\"1.0\" encoding=\"UTF-8\"?> <feed xmlns=\"http://www.w3.org/2005/Atom\" xmlns:app=\"http://www.w3.org/2007/app\" xmlns:opensearch=\"http://a9.com/-/spec/opensearch/1.1/\" xmlns:snx=\"http://www.ibm.com/xmlns/prod/sn\"> <opensearch:totalResults>6</opensearch:totalResults> <opensearch:startIndex>1</opensearch:startIndex> <opensearch:itemsPerPage>10</opensearch:itemsPerPage> <title type=\"text\">Connections Search Integration - Medlemmar</title> <id>http://sebconnections.sebank.se/communities/service/atom/community/members?role=owner&amp;communityUuid=18afc9d8-2c47-4df7-967b-8486c5715dfe</id> <updated>2012-03-07T11:26:15.346Z</updated> <generator uri=\"http://www.ibm.com/xmlns/prod/sn\" version=\"3.0.1.0\">IBM Connections - Communities</generator> <link href=\"http://sebconnections.sebank.se/communities/service/atom/community/members?role=owner&amp;communityUuid=18afc9d8-2c47-4df7-967b-8486c5715dfe\" rel=\"self\"> </link> <link href=\"http://sebconnections.sebank.se/communities/service/atom/community/members?role=owner&amp;communityUuid=18afc9d8-2c47-4df7-967b-8486c5715dfe&amp;outputType=categories\" rel=\"http://www.ibm.com/xmlns/prod/sn/tag-cloud\" type=\"application/atomcat+xml\"> </link> <link href=\"http://sebconnections.sebank.se/communities/service/atom/community/service?communityUuid=18afc9d8-2c47-4df7-967b-8486c5715dfe\" rel=\"http://www.ibm.com/xmlns/prod/sn/service\" type=\"application/atomsvc+xml\"> </link> <snx:communityLastMod component=\"http://www.ibm.com/xmlns/prod/sn/communities\">1331052461141</snx:communityLastMod> <entry> <id>http://communities.ibm.com:2006/service/atom/community/members?communityUuid=18afc9d8-2c47-4df7-967b-8486c5715dfe&amp;email=anna.lindstrom@seb.se</id> <title type=\"text\">Anna Lindström</title> <summary type=\"text\">Anna Lindström</summary> <link href=\"http://sebconnections.sebank.se/profiles/atom/profile.do?email=anna.lindstrom@seb.se\" type=\"application/atom+xml\"> </link> <link href=\"http://sebconnections.sebank.se/profiles/vcard/profile.do?email=anna.lindstrom@seb.se\" type=\"text/directory\"> </link> <published>2012-02-21T08:03:35.431Z</published> <updated>2012-02-21T08:03:35.431Z</updated> <contributor> <email>anna.lindstrom@seb.se</email> <snx:userid>fb44d540-2a51-102e-885a-8151333fdc6d</snx:userid> <snx:userState>active</snx:userState> <name>Anna Lindström</name> </contributor> <link href=\"http://sebconnections.sebank.se/communities/service/atom/community/members?communityUuid=18afc9d8-2c47-4df7-967b-8486c5715dfe&amp;email=anna.lindstrom@seb.se\" rel=\"self\"> </link> <link href=\"http://sebconnections.sebank.se/communities/service/atom/community/members?communityUuid=18afc9d8-2c47-4df7-967b-8486c5715dfe&amp;email=anna.lindstrom@seb.se\" rel=\"edit\"> </link> <snx:role component=\"http://www.ibm.com/xmlns/prod/sn/communities\">owner</snx:role> <content type=\"xhtml\"> <div xmlns=\"http://www.w3.org/1999/xhtml\"> <span> <a href=\"http://sebconnections.sebank.se/profiles/atom/profile.do?email=anna.lindstrom@seb.se\" class=\"url fn\">Anna Lindstr�m</a> <div> <a href=\"mailto:anna.lindstrom@seb.se\" class=\"email\">anna.lindstrom@seb.se</a> </div> <div class=\"x-guid\">fb44d540-2a51-102e-885a-8151333fdc6d</div> <div class=\"x-community-role\">owner</div> </span> </div> </content> </entry> <entry> <id>http://communities.ibm.com:2006/service/atom/community/members?communityUuid=18afc9d8-2c47-4df7-967b-8486c5715dfe&amp;email=charlotte.dahl@seb.se</id> <title type=\"text\">Charlotte Dahl</title> <summary type=\"text\">Charlotte Dahl</summary> <link href=\"http://sebconnections.sebank.se/profiles/atom/profile.do?email=charlotte.dahl@seb.se\" type=\"application/atom+xml\"> </link> <link href=\"http://sebconnections.sebank.se/profiles/vcard/profile.do?email=charlotte.dahl@seb.se\" type=\"text/directory\"> </link> <published>2012-02-21T08:04:44.838Z</published> <updated>2012-02-21T08:04:44.838Z</updated> <contributor> <email>charlotte.dahl@seb.se</email> <snx:userid>e9791840-3809-1030-99c2-b0a08e8fa2fe</snx:userid> <snx:userState>active</snx:userState> <name>Charlotte Dahl</name> </contributor> <link href=\"http://sebconnections.sebank.se/communities/service/atom/community/members?communityUuid=18afc9d8-2c47-4df7-967b-8486c5715dfe&amp;email=charlotte.dahl@seb.se\" rel=\"self\"> </link> <link href=\"http://sebconnections.sebank.se/communities/service/atom/community/members?communityUuid=18afc9d8-2c47-4df7-967b-8486c5715dfe&amp;email=charlotte.dahl@seb.se\" rel=\"edit\"> </link> <snx:role component=\"http://www.ibm.com/xmlns/prod/sn/communities\">owner</snx:role> <content type=\"xhtml\"> <div xmlns=\"http://www.w3.org/1999/xhtml\"> <span> <a href=\"http://sebconnections.sebank.se/profiles/atom/profile.do?email=charlotte.dahl@seb.se\" class=\"url fn\">Charlotte Dahl</a> <div> <a href=\"mailto:charlotte.dahl@seb.se\" class=\"email\">charlotte.dahl@seb.se</a> </div> <div class=\"x-guid\">e9791840-3809-1030-99c2-b0a08e8fa2fe</div> <div class=\"x-community-role\">owner</div> </span> </div> </content> </entry> <entry> <id>http://communities.ibm.com:2006/service/atom/community/members?communityUuid=18afc9d8-2c47-4df7-967b-8486c5715dfe&amp;email=emma.hallin@seb.se</id> <title type=\"text\">Emma Hallin</title> <summary type=\"text\">Emma Hallin</summary> <link href=\"http://sebconnections.sebank.se/profiles/atom/profile.do?email=emma.hallin@seb.se\" type=\"application/atom+xml\"> </link> <link href=\"http://sebconnections.sebank.se/profiles/vcard/profile.do?email=emma.hallin@seb.se\" type=\"text/directory\"> </link> <published>2012-02-21T08:03:35.924Z</published> <updated>2012-02-21T08:03:35.924Z</updated> <contributor> <email>emma.hallin@seb.se</email> <snx:userid>305127c0-2a52-102e-9460-8151333fdc6d</snx:userid> <snx:userState>active</snx:userState> <name>Emma Hallin</name> </contributor> <link href=\"http://sebconnections.sebank.se/communities/service/atom/community/members?communityUuid=18afc9d8-2c47-4df7-967b-8486c5715dfe&amp;email=emma.hallin@seb.se\" rel=\"self\"> </link> <link href=\"http://sebconnections.sebank.se/communities/service/atom/community/members?communityUuid=18afc9d8-2c47-4df7-967b-8486c5715dfe&amp;email=emma.hallin@seb.se\" rel=\"edit\"> </link> <snx:role component=\"http://www.ibm.com/xmlns/prod/sn/communities\">owner</snx:role> <content type=\"xhtml\"> <div xmlns=\"http://www.w3.org/1999/xhtml\"> <span> <a href=\"http://sebconnections.sebank.se/profiles/atom/profile.do?email=emma.hallin@seb.se\" class=\"url fn\">Emma Hallin</a> <div> <a href=\"mailto:emma.hallin@seb.se\" class=\"email\">emma.hallin@seb.se</a> </div> <div class=\"x-guid\">305127c0-2a52-102e-9460-8151333fdc6d</div> <div class=\"x-community-role\">owner</div> </span> </div> </content> </entry> <entry> <id>http://communities.ibm.com:2006/service/atom/community/members?communityUuid=18afc9d8-2c47-4df7-967b-8486c5715dfe&amp;email=james.royal-lawson@seb.se</id> <title type=\"text\">James Royal-Lawson</title> <summary type=\"text\">James Royal-Lawson</summary> <link href=\"http://sebconnections.sebank.se/profiles/atom/profile.do?email=james.royal-lawson@seb.se\" type=\"application/atom+xml\"> </link> <link href=\"http://sebconnections.sebank.se/profiles/vcard/profile.do?email=james.royal-lawson@seb.se\" type=\"text/directory\"> </link> <published>2012-02-21T08:03:35.931Z</published> <updated>2012-02-21T08:03:35.931Z</updated> <contributor> <email>james.royal-lawson@seb.se</email> <snx:userid>229ba240-2a52-102e-87e0-8151333fdc6d</snx:userid> <snx:userState>active</snx:userState> <name>James Royal-Lawson</name> </contributor> <link href=\"http://sebconnections.sebank.se/communities/service/atom/community/members?communityUuid=18afc9d8-2c47-4df7-967b-8486c5715dfe&amp;email=james.royal-lawson@seb.se\" rel=\"self\"> </link> <link href=\"http://sebconnections.sebank.se/communities/service/atom/community/members?communityUuid=18afc9d8-2c47-4df7-967b-8486c5715dfe&amp;email=james.royal-lawson@seb.se\" rel=\"edit\"> </link> <snx:role component=\"http://www.ibm.com/xmlns/prod/sn/communities\">owner</snx:role> <content type=\"xhtml\"> <div xmlns=\"http://www.w3.org/1999/xhtml\"> <span> <a href=\"http://sebconnections.sebank.se/profiles/atom/profile.do?email=james.royal-lawson@seb.se\" class=\"url fn\">James Royal-Lawson</a> <div> <a href=\"mailto:james.royal-lawson@seb.se\" class=\"email\">james.royal-lawson@seb.se</a> </div> <div class=\"x-guid\">229ba240-2a52-102e-87e0-8151333fdc6d</div> <div class=\"x-community-role\">owner</div> </span> </div> </content> </entry> <entry> <id>http://communities.ibm.com:2006/service/atom/community/members?communityUuid=18afc9d8-2c47-4df7-967b-8486c5715dfe&amp;email=joakim.hallin@seb.se</id> <title type=\"text\">Joakim Hallin</title> <summary type=\"text\">Joakim Hallin</summary> <link href=\"http://sebconnections.sebank.se/profiles/atom/profile.do?email=joakim.hallin@seb.se\" type=\"application/atom+xml\"> </link> <link href=\"http://sebconnections.sebank.se/profiles/vcard/profile.do?email=joakim.hallin@seb.se\" type=\"text/directory\"> </link> <published>2012-02-21T08:03:35.927Z</published> <updated>2012-02-21T08:03:35.927Z</updated> <contributor> <email>joakim.hallin@seb.se</email> <snx:userid>144c86c0-2a50-102e-991e-8151333fdc6d</snx:userid> <snx:userState>active</snx:userState> <name>Joakim Hallin</name> </contributor> <link href=\"http://sebconnections.sebank.se/communities/service/atom/community/members?communityUuid=18afc9d8-2c47-4df7-967b-8486c5715dfe&amp;email=joakim.hallin@seb.se\" rel=\"self\"> </link> <link href=\"http://sebconnections.sebank.se/communities/service/atom/community/members?communityUuid=18afc9d8-2c47-4df7-967b-8486c5715dfe&amp;email=joakim.hallin@seb.se\" rel=\"edit\"> </link> <snx:role component=\"http://www.ibm.com/xmlns/prod/sn/communities\">owner</snx:role> <content type=\"xhtml\"> <div xmlns=\"http://www.w3.org/1999/xhtml\"> <span> <a href=\"http://sebconnections.sebank.se/profiles/atom/profile.do?email=joakim.hallin@seb.se\" class=\"url fn\">Joakim Hallin</a> <div> <a href=\"mailto:joakim.hallin@seb.se\" class=\"email\">joakim.hallin@seb.se</a> </div> <div class=\"x-guid\">144c86c0-2a50-102e-991e-8151333fdc6d</div> <div class=\"x-community-role\">owner</div> </span> </div> </content> </entry> <entry> <id>http://communities.ibm.com:2006/service/atom/community/members?communityUuid=18afc9d8-2c47-4df7-967b-8486c5715dfe&amp;email=monica.sundstrom@seb.se</id> <title type=\"text\">Monica Sundström</title> <summary type=\"text\">Monica Sundström</summary> <link href=\"http://sebconnections.sebank.se/profiles/atom/profile.do?email=monica.sundstrom@seb.se\" type=\"application/atom+xml\"> </link> <link href=\"http://sebconnections.sebank.se/profiles/vcard/profile.do?email=monica.sundstrom@seb.se\" type=\"text/directory\"> </link> <published>2012-02-21T08:03:35.921Z</published> <updated>2012-02-21T08:03:35.921Z</updated> <contributor> <email>monica.sundstrom@seb.se</email> <snx:userid>f7b04ec0-2a4f-102e-82d8-8151333fdc6d</snx:userid> <snx:userState>active</snx:userState> <name>Monica Sundström</name> </contributor> <link href=\"http://sebconnections.sebank.se/communities/service/atom/community/members?communityUuid=18afc9d8-2c47-4df7-967b-8486c5715dfe&amp;email=monica.sundstrom@seb.se\" rel=\"self\"> </link> <link href=\"http://sebconnections.sebank.se/communities/service/atom/community/members?communityUuid=18afc9d8-2c47-4df7-967b-8486c5715dfe&amp;email=monica.sundstrom@seb.se\" rel=\"edit\"> </link> <snx:role component=\"http://www.ibm.com/xmlns/prod/sn/communities\">owner</snx:role> <content type=\"xhtml\"> <div xmlns=\"http://www.w3.org/1999/xhtml\"> <span> <a href=\"http://sebconnections.sebank.se/profiles/atom/profile.do?email=monica.sundstrom@seb.se\" class=\"url fn\">Monica Sundstr�m</a> <div> <a href=\"mailto:monica.sundstrom@seb.se\" class=\"email\">monica.sundstrom@seb.se</a> </div> <div class=\"x-guid\">f7b04ec0-2a4f-102e-82d8-8151333fdc6d</div> <div class=\"x-community-role\">owner</div> </span> </div> </content> </entry> </feed>");
		jsoup.process(doc);
		
		String names = "[Anna Lindström, Charlotte Dahl, Emma Hallin, James Royal-Lawson, Joakim Hallin, Monica Sundström]";
		
		assertEquals(names, doc.getContentField("name").toString());
	}
	
	@Test
	public void testExtractConnectionsAPIMembersEmails() throws ProcessException {
		doc.putContentField("rawcontent", "<?xml version=\"1.0\" encoding=\"UTF-8\"?> <feed xmlns=\"http://www.w3.org/2005/Atom\" xmlns:app=\"http://www.w3.org/2007/app\" xmlns:opensearch=\"http://a9.com/-/spec/opensearch/1.1/\" xmlns:snx=\"http://www.ibm.com/xmlns/prod/sn\"> <opensearch:totalResults>6</opensearch:totalResults> <opensearch:startIndex>1</opensearch:startIndex> <opensearch:itemsPerPage>10</opensearch:itemsPerPage> <title type=\"text\">Connections Search Integration - Medlemmar</title> <id>http://sebconnections.sebank.se/communities/service/atom/community/members?role=owner&amp;communityUuid=18afc9d8-2c47-4df7-967b-8486c5715dfe</id> <updated>2012-03-07T11:26:15.346Z</updated> <generator uri=\"http://www.ibm.com/xmlns/prod/sn\" version=\"3.0.1.0\">IBM Connections - Communities</generator> <link href=\"http://sebconnections.sebank.se/communities/service/atom/community/members?role=owner&amp;communityUuid=18afc9d8-2c47-4df7-967b-8486c5715dfe\" rel=\"self\"> </link> <link href=\"http://sebconnections.sebank.se/communities/service/atom/community/members?role=owner&amp;communityUuid=18afc9d8-2c47-4df7-967b-8486c5715dfe&amp;outputType=categories\" rel=\"http://www.ibm.com/xmlns/prod/sn/tag-cloud\" type=\"application/atomcat+xml\"> </link> <link href=\"http://sebconnections.sebank.se/communities/service/atom/community/service?communityUuid=18afc9d8-2c47-4df7-967b-8486c5715dfe\" rel=\"http://www.ibm.com/xmlns/prod/sn/service\" type=\"application/atomsvc+xml\"> </link> <snx:communityLastMod component=\"http://www.ibm.com/xmlns/prod/sn/communities\">1331052461141</snx:communityLastMod> <entry> <id>http://communities.ibm.com:2006/service/atom/community/members?communityUuid=18afc9d8-2c47-4df7-967b-8486c5715dfe&amp;email=anna.lindstrom@seb.se</id> <title type=\"text\">Anna Lindström</title> <summary type=\"text\">Anna Lindström</summary> <link href=\"http://sebconnections.sebank.se/profiles/atom/profile.do?email=anna.lindstrom@seb.se\" type=\"application/atom+xml\"> </link> <link href=\"http://sebconnections.sebank.se/profiles/vcard/profile.do?email=anna.lindstrom@seb.se\" type=\"text/directory\"> </link> <published>2012-02-21T08:03:35.431Z</published> <updated>2012-02-21T08:03:35.431Z</updated> <contributor> <email>anna.lindstrom@seb.se</email> <snx:userid>fb44d540-2a51-102e-885a-8151333fdc6d</snx:userid> <snx:userState>active</snx:userState> <name>Anna Lindström</name> </contributor> <link href=\"http://sebconnections.sebank.se/communities/service/atom/community/members?communityUuid=18afc9d8-2c47-4df7-967b-8486c5715dfe&amp;email=anna.lindstrom@seb.se\" rel=\"self\"> </link> <link href=\"http://sebconnections.sebank.se/communities/service/atom/community/members?communityUuid=18afc9d8-2c47-4df7-967b-8486c5715dfe&amp;email=anna.lindstrom@seb.se\" rel=\"edit\"> </link> <snx:role component=\"http://www.ibm.com/xmlns/prod/sn/communities\">owner</snx:role> <content type=\"xhtml\"> <div xmlns=\"http://www.w3.org/1999/xhtml\"> <span> <a href=\"http://sebconnections.sebank.se/profiles/atom/profile.do?email=anna.lindstrom@seb.se\" class=\"url fn\">Anna Lindstr�m</a> <div> <a href=\"mailto:anna.lindstrom@seb.se\" class=\"email\">anna.lindstrom@seb.se</a> </div> <div class=\"x-guid\">fb44d540-2a51-102e-885a-8151333fdc6d</div> <div class=\"x-community-role\">owner</div> </span> </div> </content> </entry> <entry> <id>http://communities.ibm.com:2006/service/atom/community/members?communityUuid=18afc9d8-2c47-4df7-967b-8486c5715dfe&amp;email=charlotte.dahl@seb.se</id> <title type=\"text\">Charlotte Dahl</title> <summary type=\"text\">Charlotte Dahl</summary> <link href=\"http://sebconnections.sebank.se/profiles/atom/profile.do?email=charlotte.dahl@seb.se\" type=\"application/atom+xml\"> </link> <link href=\"http://sebconnections.sebank.se/profiles/vcard/profile.do?email=charlotte.dahl@seb.se\" type=\"text/directory\"> </link> <published>2012-02-21T08:04:44.838Z</published> <updated>2012-02-21T08:04:44.838Z</updated> <contributor> <email>charlotte.dahl@seb.se</email> <snx:userid>e9791840-3809-1030-99c2-b0a08e8fa2fe</snx:userid> <snx:userState>active</snx:userState> <name>Charlotte Dahl</name> </contributor> <link href=\"http://sebconnections.sebank.se/communities/service/atom/community/members?communityUuid=18afc9d8-2c47-4df7-967b-8486c5715dfe&amp;email=charlotte.dahl@seb.se\" rel=\"self\"> </link> <link href=\"http://sebconnections.sebank.se/communities/service/atom/community/members?communityUuid=18afc9d8-2c47-4df7-967b-8486c5715dfe&amp;email=charlotte.dahl@seb.se\" rel=\"edit\"> </link> <snx:role component=\"http://www.ibm.com/xmlns/prod/sn/communities\">owner</snx:role> <content type=\"xhtml\"> <div xmlns=\"http://www.w3.org/1999/xhtml\"> <span> <a href=\"http://sebconnections.sebank.se/profiles/atom/profile.do?email=charlotte.dahl@seb.se\" class=\"url fn\">Charlotte Dahl</a> <div> <a href=\"mailto:charlotte.dahl@seb.se\" class=\"email\">charlotte.dahl@seb.se</a> </div> <div class=\"x-guid\">e9791840-3809-1030-99c2-b0a08e8fa2fe</div> <div class=\"x-community-role\">owner</div> </span> </div> </content> </entry> <entry> <id>http://communities.ibm.com:2006/service/atom/community/members?communityUuid=18afc9d8-2c47-4df7-967b-8486c5715dfe&amp;email=emma.hallin@seb.se</id> <title type=\"text\">Emma Hallin</title> <summary type=\"text\">Emma Hallin</summary> <link href=\"http://sebconnections.sebank.se/profiles/atom/profile.do?email=emma.hallin@seb.se\" type=\"application/atom+xml\"> </link> <link href=\"http://sebconnections.sebank.se/profiles/vcard/profile.do?email=emma.hallin@seb.se\" type=\"text/directory\"> </link> <published>2012-02-21T08:03:35.924Z</published> <updated>2012-02-21T08:03:35.924Z</updated> <contributor> <email>emma.hallin@seb.se</email> <snx:userid>305127c0-2a52-102e-9460-8151333fdc6d</snx:userid> <snx:userState>active</snx:userState> <name>Emma Hallin</name> </contributor> <link href=\"http://sebconnections.sebank.se/communities/service/atom/community/members?communityUuid=18afc9d8-2c47-4df7-967b-8486c5715dfe&amp;email=emma.hallin@seb.se\" rel=\"self\"> </link> <link href=\"http://sebconnections.sebank.se/communities/service/atom/community/members?communityUuid=18afc9d8-2c47-4df7-967b-8486c5715dfe&amp;email=emma.hallin@seb.se\" rel=\"edit\"> </link> <snx:role component=\"http://www.ibm.com/xmlns/prod/sn/communities\">owner</snx:role> <content type=\"xhtml\"> <div xmlns=\"http://www.w3.org/1999/xhtml\"> <span> <a href=\"http://sebconnections.sebank.se/profiles/atom/profile.do?email=emma.hallin@seb.se\" class=\"url fn\">Emma Hallin</a> <div> <a href=\"mailto:emma.hallin@seb.se\" class=\"email\">emma.hallin@seb.se</a> </div> <div class=\"x-guid\">305127c0-2a52-102e-9460-8151333fdc6d</div> <div class=\"x-community-role\">owner</div> </span> </div> </content> </entry> <entry> <id>http://communities.ibm.com:2006/service/atom/community/members?communityUuid=18afc9d8-2c47-4df7-967b-8486c5715dfe&amp;email=james.royal-lawson@seb.se</id> <title type=\"text\">James Royal-Lawson</title> <summary type=\"text\">James Royal-Lawson</summary> <link href=\"http://sebconnections.sebank.se/profiles/atom/profile.do?email=james.royal-lawson@seb.se\" type=\"application/atom+xml\"> </link> <link href=\"http://sebconnections.sebank.se/profiles/vcard/profile.do?email=james.royal-lawson@seb.se\" type=\"text/directory\"> </link> <published>2012-02-21T08:03:35.931Z</published> <updated>2012-02-21T08:03:35.931Z</updated> <contributor> <email>james.royal-lawson@seb.se</email> <snx:userid>229ba240-2a52-102e-87e0-8151333fdc6d</snx:userid> <snx:userState>active</snx:userState> <name>James Royal-Lawson</name> </contributor> <link href=\"http://sebconnections.sebank.se/communities/service/atom/community/members?communityUuid=18afc9d8-2c47-4df7-967b-8486c5715dfe&amp;email=james.royal-lawson@seb.se\" rel=\"self\"> </link> <link href=\"http://sebconnections.sebank.se/communities/service/atom/community/members?communityUuid=18afc9d8-2c47-4df7-967b-8486c5715dfe&amp;email=james.royal-lawson@seb.se\" rel=\"edit\"> </link> <snx:role component=\"http://www.ibm.com/xmlns/prod/sn/communities\">owner</snx:role> <content type=\"xhtml\"> <div xmlns=\"http://www.w3.org/1999/xhtml\"> <span> <a href=\"http://sebconnections.sebank.se/profiles/atom/profile.do?email=james.royal-lawson@seb.se\" class=\"url fn\">James Royal-Lawson</a> <div> <a href=\"mailto:james.royal-lawson@seb.se\" class=\"email\">james.royal-lawson@seb.se</a> </div> <div class=\"x-guid\">229ba240-2a52-102e-87e0-8151333fdc6d</div> <div class=\"x-community-role\">owner</div> </span> </div> </content> </entry> <entry> <id>http://communities.ibm.com:2006/service/atom/community/members?communityUuid=18afc9d8-2c47-4df7-967b-8486c5715dfe&amp;email=joakim.hallin@seb.se</id> <title type=\"text\">Joakim Hallin</title> <summary type=\"text\">Joakim Hallin</summary> <link href=\"http://sebconnections.sebank.se/profiles/atom/profile.do?email=joakim.hallin@seb.se\" type=\"application/atom+xml\"> </link> <link href=\"http://sebconnections.sebank.se/profiles/vcard/profile.do?email=joakim.hallin@seb.se\" type=\"text/directory\"> </link> <published>2012-02-21T08:03:35.927Z</published> <updated>2012-02-21T08:03:35.927Z</updated> <contributor> <email>joakim.hallin@seb.se</email> <snx:userid>144c86c0-2a50-102e-991e-8151333fdc6d</snx:userid> <snx:userState>active</snx:userState> <name>Joakim Hallin</name> </contributor> <link href=\"http://sebconnections.sebank.se/communities/service/atom/community/members?communityUuid=18afc9d8-2c47-4df7-967b-8486c5715dfe&amp;email=joakim.hallin@seb.se\" rel=\"self\"> </link> <link href=\"http://sebconnections.sebank.se/communities/service/atom/community/members?communityUuid=18afc9d8-2c47-4df7-967b-8486c5715dfe&amp;email=joakim.hallin@seb.se\" rel=\"edit\"> </link> <snx:role component=\"http://www.ibm.com/xmlns/prod/sn/communities\">owner</snx:role> <content type=\"xhtml\"> <div xmlns=\"http://www.w3.org/1999/xhtml\"> <span> <a href=\"http://sebconnections.sebank.se/profiles/atom/profile.do?email=joakim.hallin@seb.se\" class=\"url fn\">Joakim Hallin</a> <div> <a href=\"mailto:joakim.hallin@seb.se\" class=\"email\">joakim.hallin@seb.se</a> </div> <div class=\"x-guid\">144c86c0-2a50-102e-991e-8151333fdc6d</div> <div class=\"x-community-role\">owner</div> </span> </div> </content> </entry> <entry> <id>http://communities.ibm.com:2006/service/atom/community/members?communityUuid=18afc9d8-2c47-4df7-967b-8486c5715dfe&amp;email=monica.sundstrom@seb.se</id> <title type=\"text\">Monica Sundström</title> <summary type=\"text\">Monica Sundström</summary> <link href=\"http://sebconnections.sebank.se/profiles/atom/profile.do?email=monica.sundstrom@seb.se\" type=\"application/atom+xml\"> </link> <link href=\"http://sebconnections.sebank.se/profiles/vcard/profile.do?email=monica.sundstrom@seb.se\" type=\"text/directory\"> </link> <published>2012-02-21T08:03:35.921Z</published> <updated>2012-02-21T08:03:35.921Z</updated> <contributor> <email>monica.sundstrom@seb.se</email> <snx:userid>f7b04ec0-2a4f-102e-82d8-8151333fdc6d</snx:userid> <snx:userState>active</snx:userState> <name>Monica Sundström</name> </contributor> <link href=\"http://sebconnections.sebank.se/communities/service/atom/community/members?communityUuid=18afc9d8-2c47-4df7-967b-8486c5715dfe&amp;email=monica.sundstrom@seb.se\" rel=\"self\"> </link> <link href=\"http://sebconnections.sebank.se/communities/service/atom/community/members?communityUuid=18afc9d8-2c47-4df7-967b-8486c5715dfe&amp;email=monica.sundstrom@seb.se\" rel=\"edit\"> </link> <snx:role component=\"http://www.ibm.com/xmlns/prod/sn/communities\">owner</snx:role> <content type=\"xhtml\"> <div xmlns=\"http://www.w3.org/1999/xhtml\"> <span> <a href=\"http://sebconnections.sebank.se/profiles/atom/profile.do?email=monica.sundstrom@seb.se\" class=\"url fn\">Monica Sundstr�m</a> <div> <a href=\"mailto:monica.sundstrom@seb.se\" class=\"email\">monica.sundstrom@seb.se</a> </div> <div class=\"x-guid\">f7b04ec0-2a4f-102e-82d8-8151333fdc6d</div> <div class=\"x-community-role\">owner</div> </span> </div> </content> </entry> </feed>");
		jsoup.process(doc);
		
		String emails = "[anna.lindstrom@seb.se, charlotte.dahl@seb.se, emma.hallin@seb.se, james.royal-lawson@seb.se, joakim.hallin@seb.se, monica.sundstrom@seb.se]";
		
		assertEquals(emails, doc.getContentField("email").toString());
	}
}

