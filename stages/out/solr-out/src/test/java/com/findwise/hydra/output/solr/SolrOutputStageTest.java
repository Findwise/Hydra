package com.findwise.hydra.output.solr;

import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.common.SolrInputDocument;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.mockito.Mockito;

import com.findwise.hydra.common.Document.Action;
import com.findwise.hydra.local.LocalDocument;
import com.findwise.hydra.local.RemotePipeline;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class SolrOutputStageTest {
	
	SolrOutputStage solrOutput;
	SolrServer mockServer;
	RemotePipeline mockRP;

	@Before
	public void setUp() throws Exception {
		solrOutput = new SolrOutputStage();
		mockServer = Mockito.mock(SolrServer.class);
        mockRP = Mockito.mock(RemotePipeline.class);
        solrOutput.setSolrServer(mockServer);
        solrOutput.setRemotePipeline(mockRP);
        solrOutput.init();
	}

	@After
	public void tearDown() throws Exception {

	}

	@Ignore
	@Test
	public void testSolrConnection() throws Exception {
		LocalDocument doc = new LocalDocument();
		doc.putContentField("_solrAction", "add");
		doc.putContentField("id", "someid1");
		doc.putContentField("name", "jockeh");
		solrOutput.output(doc);
		
		doc.putContentField("_solrAction", "del");
		solrOutput.output(doc);
	}

	@Test
	public void testAdd() throws Exception {
                solrOutput.setFieldMappings(new HashMap<String, String>());
                
		LocalDocument doc = new LocalDocument();
		doc.setAction(Action.ADD);
		doc.putContentField("name", "jonas");
		solrOutput.output(doc);
		Mockito.verify(mockServer).add(Mockito.anyCollectionOf(SolrInputDocument.class));
	}
	@Test
	public void testDelete() throws Exception {
		LocalDocument doc = new LocalDocument();
		doc.setAction(Action.DELETE);
		doc.putContentField("name", "jonas");
		solrOutput.output(doc);
		Mockito.verify(mockServer).deleteById(Mockito.anyListOf(String.class));
	}
	@Test
	public void testSendLimit() throws Exception {
                solrOutput.setFieldMappings(new HashMap<String, String>());
		solrOutput.setSendLimit(5);
		solrOutput.setCommitLimit(15);
		LocalDocument doc = new LocalDocument();
		doc.setAction(Action.ADD);
		doc.putContentField("name", "one");
		solrOutput.output(doc);
		doc = new LocalDocument();
		doc.setAction(Action.ADD);
		doc.putContentField("name", "two");
		solrOutput.output(doc);
		doc = new LocalDocument();
		doc.setAction(Action.ADD);
		doc.putContentField("name", "three");
		solrOutput.output(doc);
		doc = new LocalDocument();
		doc.setAction(Action.ADD);
		doc.putContentField("name", "four");
		solrOutput.output(doc);
		doc = new LocalDocument();
		doc.setAction(Action.ADD);
		doc.putContentField("name", "five");
		solrOutput.output(doc);
		Mockito.verify(mockServer,Mockito.times(1)).add(Mockito.anyCollectionOf(SolrInputDocument.class));
	}
	
	@Test
	public void testCommitLimit() throws Exception {
                solrOutput.setFieldMappings(new HashMap<String, String>());
		solrOutput.setSendLimit(15);
		solrOutput.setCommitLimit(5);
		LocalDocument doc = new LocalDocument();
		doc.setAction(Action.ADD);
		doc.putContentField("name", "one");
		solrOutput.output(doc);
		doc = new LocalDocument();
		doc.setAction(Action.ADD);
		doc.putContentField("name", "two");
		solrOutput.output(doc);
		doc = new LocalDocument();
		doc.setAction(Action.ADD);
		doc.putContentField("name", "three");
		solrOutput.output(doc);
		doc = new LocalDocument();
		doc.setAction(Action.ADD);
		doc.putContentField("name", "four");
		solrOutput.output(doc);
		doc = new LocalDocument();
		doc.setAction(Action.ADD);
		doc.putContentField("name", "five");
		solrOutput.output(doc);
		Mockito.verify(mockServer,Mockito.times(1)).commit();
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
                                           
                Map<String, String> fieldMappings = new HashMap<String, String>();
                fieldMappings.put("name", "fullname");
                fieldMappings.put("reference", "url");
                fieldMappings.put("doesnotexist", "doesnotmatter");
                fieldMappings.put("hero", "heroes");
                
                solrOutput.setFieldMappings(fieldMappings);
                                
                SolrInputDocument inputDoc = solrOutput.createSolrInputDocumentWithFieldConfig(doc);
   
                org.junit.Assert.assertEquals(inputDoc.getFieldValue("fullname"),
                        doc.getContentField("name").toString());
                              
                org.junit.Assert.assertArrayEquals(multiValued.toArray(), 
                        inputDoc.getFieldValues("heroes").toArray());
               
	}
}
