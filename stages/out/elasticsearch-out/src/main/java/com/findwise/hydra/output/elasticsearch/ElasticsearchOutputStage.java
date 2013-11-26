package com.findwise.hydra.output.elasticsearch;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import com.findwise.hydra.stage.InitFailedException;
import org.elasticsearch.ElasticSearchException;
import org.elasticsearch.action.ListenableActionFuture;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import com.findwise.hydra.Document.Action;
import com.findwise.hydra.local.LocalDocument;
import com.findwise.hydra.stage.AbstractOutputStage;
import com.findwise.hydra.stage.Parameter;
import com.findwise.hydra.stage.RequiredArgumentMissingException;
import com.findwise.hydra.stage.Stage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Writes to elasticsearch via the Transport protocol.
 * 
 * @author olof.nilsson
 *
 */
@Stage(description = "A stage that writes documents to elasticsearch")
public class ElasticsearchOutputStage extends AbstractOutputStage {
    private static Logger logger = LoggerFactory.getLogger(ElasticsearchOutputStage.class);

	@Parameter(required = true, description = "List of elasticsearch node URLs to connect to")
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
	public void init() throws RequiredArgumentMissingException, InitFailedException {
		try {
			client = constructClient();
		} catch (Exception e) {
			throw new InitFailedException("Could not construct client", e);
		}
	}

	@Override
	public void output(LocalDocument document) {
		final Action action = document.getAction();
		
		try {
			logger.debug(action.toString());
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
				failDocument(document, new RequiredArgumentMissingException("Action must be ADD, DELETE or UPDATE."));
				break;
			}
		} catch (ElasticSearchException e) {
			failDocument(document, e);
		} catch (IOException e) {
			failDocument(document, e);
		}
	}

	private void update(LocalDocument document) throws ElasticSearchException, IOException {
		add(document);
	}

	private void add(LocalDocument document) throws ElasticSearchException, IOException {
		String docId = getDocumentId(document);
		String json = document.contentFieldsToJson(document.getContentFields());
		logger.debug("Indexing document " + getDocumentId(document) + " to index " + documentIndex + " with type " + documentType);
		ListenableActionFuture<IndexResponse> actionFuture = client.prepareIndex(documentIndex, documentType, docId)
			.setSource(json)
			.execute();
		IndexResponse response = actionFuture.actionGet(requestTimeout);
		logger.debug("Got response for docId " + response.getId());
		accept(document);
	}

	private void delete(LocalDocument document) throws IOException {
		
		String docId = getDocumentId(document);
		
		ListenableActionFuture<DeleteResponse> actionFuture = client.prepareDelete(documentIndex, documentType, docId)
				.execute();
		DeleteResponse response = actionFuture.actionGet(requestTimeout);
		if (response.isNotFound()) {
			logger.debug("Delete failed, document not found");
		}
		else {
			logger.debug("Deleted document with id " + response.getId());
		}
		accept(document);
	}

	private void failDocument(LocalDocument doc, Throwable reason) {
		try {
			logger.error("Failing document " + doc.getID(), reason);
			fail(doc, reason);
		} catch (Exception e) {
			logger.error("Could not fail document with hydra id: " + doc.getID(), e);
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

	protected String getDocumentId(LocalDocument document) {
		return (String) document.getContentField(documentIdField);
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
		this.esNodes.clear();
		this.esNodes.addAll(esNodes);
	}

	public String getIndex() {
		return this.documentIndex;
	}

	public String getType() {
		return this.documentType;
	}

	public String getIdField() {
		return this.documentIdField;
	}
}
