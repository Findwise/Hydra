package com.findwise.hydra.output.solr;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.common.SolrInputDocument;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import com.findwise.hydra.Document.Action;
import com.findwise.hydra.local.LocalDocument;

public class SolrOutputStageTest {

	SolrOutputStage solrOutput;
	SolrServer mockServer;

	@Before
	public void setUp() throws Exception {
		solrOutput = new SolrOutputStage();
		mockServer = Mockito.mock(SolrServer.class);
		solrOutput.setSolrServer(mockServer);
	}

	@After
	public void tearDown() throws Exception {
	}

	@Test
	public void testAdd() throws Exception {
		solrOutput.setFieldMappings(new HashMap<String, Object>());

		LocalDocument doc = new LocalDocument();
		doc.setAction(Action.ADD);
		doc.putContentField("name", "jonas");
		solrOutput.output(doc);
		Mockito.verify(mockServer).add(
				Mockito.any(SolrInputDocument.class));
	}

	@Test
	public void testDelete() throws Exception {
		LocalDocument doc = new LocalDocument();
		doc.setAction(Action.DELETE);
		doc.putContentField("name", "jonas");
		try {
			solrOutput.output(doc);
		} catch(Exception e) {}
		Mockito.verify(mockServer, Mockito.never()).deleteById(Mockito.any(String.class));
		
		doc.putContentField("id", "someid");
		solrOutput.output(doc);
		Mockito.verify(mockServer).deleteById(Mockito.any(String.class));
	}
	
	@Test
	public void testCommitWithin() throws Exception {
		solrOutput.setFieldMappings(new HashMap<String, Object>());
		solrOutput.setCommitWithin(1337);
		LocalDocument doc = new LocalDocument();
		doc.setAction(Action.ADD);
		doc.putContentField("name", "one");
		solrOutput.output(doc);
		Mockito.verify(mockServer, Mockito.times(1)).add(
				Mockito.any(SolrInputDocument.class), Mockito.eq(1337));
	}

	@Test
	public void testFieldConfig() throws Exception {
		LocalDocument doc = new LocalDocument();
		doc.putContentField("name", "jens");
		doc.putContentField("reference", "http://www.giantbomb.com");
		List<String> multiValued = new ArrayList<String>();
		multiValued.add("james bond");
		multiValued.add("heman");
		doc.putContentField("hero", multiValued);
		doc.putContentField("explode", "boom");

		Map<String, Object> fieldMappings = new HashMap<String, Object>();
		fieldMappings.put("name", "fullname");
		fieldMappings.put("reference", "url");
		fieldMappings.put("doesnotexist", "doesnotmatter");
		fieldMappings.put("hero", "heroes");
		fieldMappings.put("explode", Arrays.asList(new String[] {"explode1", "explode2", "explode3"}));

		solrOutput.setFieldMappings(fieldMappings);
		SolrInputDocument inputDoc = solrOutput
				.createSolrInputDocumentWithFieldConfig(doc);

		org.junit.Assert.assertEquals(inputDoc.getFieldValue("fullname"), doc
				.getContentField("name").toString());

		org.junit.Assert.assertArrayEquals(multiValued.toArray(), inputDoc
				.getFieldValues("heroes").toArray());
		
		org.junit.Assert.assertEquals(inputDoc.getFieldValue("explode1"), doc.getContentField("explode"));
		org.junit.Assert.assertEquals(inputDoc.getFieldValue("explode2"), doc.getContentField("explode"));
		org.junit.Assert.assertEquals(inputDoc.getFieldValue("explode3"), doc.getContentField("explode"));
	}

	@Test
	public void testFieldConfigWithSendallTrue() throws Exception {
		LocalDocument doc = new LocalDocument();
		doc.putContentField("name", "jens");
		doc.putContentField("reference", "http://www.giantbomb.com");
		List<String> multiValued = new ArrayList<String>();
		multiValued.add("james bond");
		multiValued.add("heman");
		doc.putContentField("hero", multiValued);
		solrOutput.setSendAll(true);
		SolrInputDocument inputDoc = solrOutput
				.createSolrInputDocumentWithFieldConfig(doc);

		org.junit.Assert.assertEquals(inputDoc.getFieldValue("name"), doc
				.getContentField("name").toString());

		org.junit.Assert.assertEquals(inputDoc.getFieldValue("reference"), doc
				.getContentField("reference").toString());

		org.junit.Assert.assertArrayEquals(((ArrayList<?>) doc
				.getContentField("hero")).toArray(),
				inputDoc.getFieldValues("hero").toArray());

	}
}
