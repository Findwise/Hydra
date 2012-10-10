package com.findwise.hydra.output.elasticsearch;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.elasticsearch.ElasticSearchException;
import org.elasticsearch.action.ListenableActionFuture;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import com.findwise.hydra.common.Document.Action;
import com.findwise.hydra.common.Logger;
import com.findwise.hydra.local.LocalDocument;
import com.findwise.hydra.stage.AbstractOutputStage;
import com.findwise.hydra.stage.Parameter;
import com.findwise.hydra.stage.RequiredArgumentMissingException;
import com.findwise.hydra.stage.Stage;

@Stage(description = "A stage that writes documents to elasticsearch")
public class ElasticsearchOutputStage extends AbstractOutputStage {

	@Parameter(description = "List of elasticsearch nodes to connect to")
	private List<String> esNodes = new ArrayList<String>();

	@Parameter(description = "Transport port of the elasticsearch nodes")
	private int transportPort = 9300;
	
	@Parameter(description = "Name of the cluster to join")
	private String clusterName = "elasticsearch";
	
	@Parameter(description = "Name of elasticsearch index")
	private String documentIndex = "main";
	
	@Parameter(description = "Document type")
	private String documentType = "default";
	
	@Parameter(description = "ID field in the document")
	private String documentIdField = "docId";
	
	@Parameter(description = "Timeout for requests in millis")
	private int requestTimeout = 10000;
	
	private Client client;
	
	@Override
	public void init() throws RequiredArgumentMissingException {
		client = constructClient();
	}
	
	@Override
	public void output(LocalDocument document) {
		final Action action = document.getAction();
		
		try {
			Logger.debug(action.toString());
			switch (action) {
			case ADD:
				add(document);
				break;
			case DELETE:
				delete(document);
				break;
			case UPDATE:
				update(document);
				break;
			default:
				fail(document);
				break;
			}
		} catch (ElasticSearchException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	
	private void update(LocalDocument document) {
		// TODO Auto-generated method stub
		
	}

	private void add(LocalDocument document) throws ElasticSearchException, IOException {
		String docId = (String) document.getContentField(documentIdField);
		String json = document.contentFieldsToJson(document.getContentFields());
		Logger.debug("Indexing document to index " + documentIndex + " with type " + documentType);
		ListenableActionFuture<IndexResponse> actionFuture = client.prepareIndex(documentIndex, documentType, docId)
			.setSource(json)
			.execute();
		IndexResponse response = actionFuture.actionGet(requestTimeout);
		Logger.debug("Got response for docId " + response.getId());
	}
	
	private void delete(LocalDocument document) throws IOException {
		
		String docId = (String) document.getContentField(documentIdField);
		
		ListenableActionFuture<DeleteResponse> actionFuture = client.prepareDelete(documentIndex, documentType, docId)
				.execute();
		DeleteResponse response = actionFuture.actionGet(requestTimeout);
		if (response.isNotFound()) {
			Logger.debug("Delete failed, document not found");
		}
		else {
			Logger.debug("Deleted document with id " + response.getId());
		}
	}

	private Client constructClient() {
		Settings settings = ImmutableSettings.settingsBuilder()
				.put("cluster.name", clusterName)
				.build();
		TransportClient tclient = new TransportClient(settings);
		for (String node : esNodes) {
			tclient.addTransportAddress(new InetSocketTransportAddress(node, transportPort));
		}
		return tclient;
	}
	
	public void setClient(Client client) {
		this.client = client;
	}
	
	public String getClusterName() {
		return clusterName;
	}

	public void setClusterName(String clusterName) {
		this.clusterName = clusterName;
	}

	public List<String> getEsNodes() {
		return esNodes;
	}

	public void setEsNodes(List<String> esNodes) {
		this.esNodes = esNodes;
	}
}
