package com.findwise.hydra.output.elasticsearch;

import org.elasticsearch.client.Client;
import org.elasticsearch.node.Node;
import org.elasticsearch.node.NodeBuilder;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import com.findwise.hydra.local.LocalDocument;

public class ElasticsearchOutputStageTest {

	Node node;
	Client client;
	ElasticsearchOutputStage stage;
	LocalDocument document;
	
	@Before
	public void setUp() {
		node = NodeBuilder.nodeBuilder().local(true).node();
		client = node.client();
		document = new LocalDocument();
		document.putContentField("field", "value");
	}
	
	@After
	public void tearDown() {
		client.close();
		node.close();
	}
	
	
	@Test
	public void testAdd() {
		stage.output(document);
		
	}
	
	@Test
	public void testDelete() {
		
	}
	
}
