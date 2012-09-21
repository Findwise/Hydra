package com.findwise.hydra.output.elasticsearch;

import org.elasticsearch.client.Client;
import org.elasticsearch.node.Node;
import org.elasticsearch.node.NodeBuilder;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.Mockito;

import com.findwise.hydra.common.Document.Action;
import com.findwise.hydra.local.LocalDocument;

public class ElasticsearchOutputStageTest {

	@Mock Client client;
	ElasticsearchOutputStage stage;
	LocalDocument document;
	
	@Before
	public void setUp() {
		document = new LocalDocument();
		document.setAction(Action.ADD);
		document.putContentField("field", "value");
		stage = new ElasticsearchOutputStage();
		
		stage.setClient(client);
	}
	
	@After
	public void tearDown() {
	}
	
	@Ignore
	@Test
	public void testAdd() {
		stage.output(document);
		
	}
	
	@Ignore
	@Test
	public void testDelete() {
		
	}
	
}
