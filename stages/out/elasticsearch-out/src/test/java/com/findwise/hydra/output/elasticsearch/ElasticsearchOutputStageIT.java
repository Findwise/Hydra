package com.findwise.hydra.output.elasticsearch;

import org.junit.Assert;

import org.elasticsearch.action.ListenableActionFuture;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.node.Node;
import org.elasticsearch.node.NodeBuilder;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import com.findwise.hydra.Document.Action;
import com.findwise.hydra.local.LocalDocument;

public class ElasticsearchOutputStageIT {
	
	@Rule
	public TemporaryFolder dataDir = new TemporaryFolder();
	
	ElasticsearchOutputStage stage;
	Node node;
	Client client;
	LocalDocument addDocument;
	LocalDocument deleteDocument;
	
	private String clusterName = "es-out-stage-it-cluster";
	
	@Before
	public void setup() {
		NodeBuilder nodeBuilder = NodeBuilder.nodeBuilder()
				.client(false)
				.clusterName(clusterName)
				.loadConfigSettings(false)
				.local(true);
		nodeBuilder.settings().put("path.data", dataDir.getRoot().getAbsolutePath());
		node = nodeBuilder.build();
		node.start();
		client = node.client();

		stage = new ElasticsearchOutputStage();
		stage.setClient(client);

		addDocument = new LocalDocument();
		addDocument.setAction(Action.ADD);
		addDocument.putContentField(stage.getIdField(), "document");
		addDocument.putContentField("field", "value");
		
		deleteDocument = new LocalDocument();
		deleteDocument.setAction(Action.DELETE);
		deleteDocument.putContentField(stage.getIdField(), "document");
		deleteDocument.putContentField("field", "value");
	}
	
	@Test
	public void testCanAddAndDelete() {
		stage.output(addDocument);
		
		ListenableActionFuture<GetResponse> addActionFuture = client.prepareGet()
				.setIndex(stage.getIndex())
				.setType(stage.getType())
				.setId(stage.getDocumentId(addDocument))
				.execute();
		GetResponse addResponse = addActionFuture.actionGet();
		Assert.assertTrue("The document should have been added to the index", addResponse.exists());
		Assert.assertEquals("Document should be added to correct index", stage.getIndex(), addResponse.getIndex());
		Assert.assertEquals("Document should have correct type", stage.getType(), addResponse.getType());

		stage.output(deleteDocument);
		
		ListenableActionFuture<GetResponse> deleteActionFuture = client.prepareGet()
				.setIndex(stage.getIndex())
				.setType(stage.getType())
				.setId(stage.getDocumentId(addDocument))
				.execute();
		GetResponse deleteResponse = deleteActionFuture.actionGet();
		Assert.assertFalse("The document should not be in the index", deleteResponse.exists());
	}
	
	@After
	public void tearDown() {
		client.close();
		node.close();
	}
}
