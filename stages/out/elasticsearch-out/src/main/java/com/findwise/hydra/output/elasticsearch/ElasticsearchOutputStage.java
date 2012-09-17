package com.findwise.hydra.output.elasticsearch;

import java.io.IOException;
import java.util.Date;

import org.elasticsearch.ElasticSearchException;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.node.Node;
import org.elasticsearch.node.NodeBuilder;

import com.findwise.hydra.common.Document.Action;
import com.findwise.hydra.local.LocalDocument;
import com.findwise.hydra.stage.AbstractOutputStage;
import com.findwise.hydra.stage.Parameter;
import com.findwise.hydra.stage.RequiredArgumentMissingException;
import com.findwise.hydra.stage.Stage;

@Stage(description = "A stage that writes documents to elasticsearch")
public class ElasticsearchOutputStage extends AbstractOutputStage {

	@Parameter(description = "URI for an elasticsearch node")
	private String uri;
	
	@Parameter(description = "Name of the cluster to join")
	private String clusterName;
	
	
	private Node node;
	
	private Client client;
	
	@Override
	public void init() throws RequiredArgumentMissingException {
		node = NodeBuilder.nodeBuilder()
				.data(false)
				.client(true)
				.clusterName(clusterName)
				.node();
		
		client = node.client();
	}
	
	@Override
	public void output(LocalDocument document) {
		final Action action = document.getAction();
		
		try {
			if (action == Action.ADD) {
				add(document);
			}
			else if (action == Action.DELETE) {
				delete(document);
			}
		} catch (ElasticSearchException e) {
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	
	private void add(LocalDocument document) throws ElasticSearchException, IOException {
		IndexResponse response = client.prepareIndex("twitter", "tweet", "1")
				.setSource(document.toMap())
				.execute()
				.actionGet();
		
	}
	
	private void delete(LocalDocument document) {
		
	}
}
