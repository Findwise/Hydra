package com.findwise.hydra.output.solr;

import java.io.IOException;
import java.net.MalformedURLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.http.HttpException;
import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.HttpSolrServer;
import org.apache.solr.common.SolrInputDocument;

import com.findwise.hydra.common.Document;
import com.findwise.hydra.common.Document.Action;
import com.findwise.hydra.common.Logger;
import com.findwise.hydra.local.LocalDocument;
import com.findwise.hydra.stage.AbstractOutputStage;
import com.findwise.hydra.stage.Parameter;
import com.findwise.hydra.stage.RequiredArgumentMissingException;
import com.findwise.hydra.stage.Stage;

@Stage
public class SolrOutputStage extends AbstractOutputStage {

	@Parameter
	private String solrDeployPath;
	@Parameter
	private Map<String, Object> fieldMappings = new HashMap<String, Object>();
	@Parameter
	private boolean sendAll = false;
	@Parameter
	private String idField = "id";
	@Parameter
	private int commitWithin = 0;
	@Parameter
	private int sendLimit;
	@Parameter
	private int sendTimeout;

	private int itemCounterSend;
	private long lastSend = 0;
	private SolrServer solr;
	private List<LocalDocument> addDocuments = new ArrayList<LocalDocument>();
	private List<LocalDocument> removeDocuments = new ArrayList<LocalDocument>();

	@Override
	public void output(LocalDocument doc) {
		final Action action = doc.getAction();

		if (action == Action.ADD) {
			addAction(doc);
		} else if (action == Action.DELETE) {
			deleteAction(doc);
		}
		checkTimeouts();
	}

	@Override
	public void init() throws RequiredArgumentMissingException {
		try {
			solr = getSolrServer();
		} catch (MalformedURLException e) {
			Logger.error("Solr URL malformed.");
		}
	}

	private synchronized void addAction(LocalDocument doc) {
		addDocuments.add(doc);
		countUpSend();
	}

	private synchronized void deleteAction(LocalDocument doc) {
		removeDocuments.add(doc);
		countUpSend();
	}

	protected void notifyInternal() {
		try {
			checkTimeouts();
		} catch (Exception e) {
			Logger.error("Failed to send or commit, I should probably crash.",
					e);
		}
	}

	private synchronized void checkTimeouts() {
		long now = new Date().getTime();
		if (now - lastSend >= sendTimeout && sendTimeout > 0) {
			send();
		}
	}

	private void countUpSend() {
		itemCounterSend++;
		if (itemCounterSend >= sendLimit) {
			send();
		}
	}

	protected SolrInputDocument createSolrInputDocumentWithFieldConfig(
			Document doc) {
		SolrInputDocument docToAdd = new SolrInputDocument();

		if (sendAll) {
			for (String inField : doc.getContentFields()) {
				docToAdd.addField(inField, doc.getContentField(inField));
			}
		} else {
			for (String inField : fieldMappings.keySet()) {
				addField(doc, docToAdd, inField);
			}
		}
		return docToAdd;
	}
	
	@SuppressWarnings("unchecked")
	private void addField(Document doc, SolrInputDocument inputDoc, String field) {
		if (doc.hasContentField(field)) {
			Object toField = fieldMappings.get(field);
			if(toField instanceof String) {
				inputDoc.addField((String) toField,
						doc.getContentField(field));
			} else if(toField instanceof List){
				for(String s : (List<String>) toField) {
					inputDoc.addField(s, doc.getContentField(field));
				}
			}
		}
	}

	private void send() {
		sendAdds();
		sendRemoves();
		itemCounterSend = 0;
		lastSend = new Date().getTime();
	}

	private void sendAdds() {
		Map<LocalDocument, SolrInputDocument> solrDocMap = mapToSolrDocuments(addDocuments);
		try {
			addDocs(solrDocMap.values());
			acceptDocuments(solrDocMap.keySet());
			

		} catch (Exception e) {
			Logger.warn(
					"Could not add documents in Solr, trying to add them individually...",
					e);
			sendAddsIndividually(solrDocMap);
		}
		finally{
			addDocuments.clear();
		}

	}

	private void sendAddsIndividually(
			Map<LocalDocument, SolrInputDocument> solrDocMap) {
		for (Entry<LocalDocument, SolrInputDocument> docEntry : solrDocMap
				.entrySet()) {
			try {
				addDocs(Collections.singleton(docEntry.getValue()));
			} catch (Exception e) {
				failDocument(docEntry.getKey());
			}
		}
	}

	private void addDocs(Collection<SolrInputDocument> docs)
			throws SolrServerException, IOException {
		if (getCommitWithin() != 0) {
			solr.add(docs, getCommitWithin());
		} else {
			solr.add(docs);
		}
	}

	private Map<LocalDocument, SolrInputDocument> mapToSolrDocuments(
			List<LocalDocument> docs) {
		Map<LocalDocument, SolrInputDocument> solrDocMap = new HashMap<LocalDocument, SolrInputDocument>();
		for (LocalDocument d : docs) {
			solrDocMap.put(d, createSolrInputDocumentWithFieldConfig(d));
		}
		return solrDocMap;
	}

	private void sendRemoves() {
		Map<LocalDocument, String> idMap = mapToIds(removeDocuments);
		try {
			solr.deleteById(new ArrayList<String>(idMap.values()));
			acceptDocuments(idMap.keySet());
		} catch (Exception e) {
			Logger.warn(
					"Could not remove documents in Solr, trying to remove them individually...",
					e);
			sendRemovesIndividually(idMap);
		} finally {
			removeDocuments.clear();
		}
	}

	private void acceptDocuments(Collection<LocalDocument> docs)
			throws IOException, HttpException {
		for (LocalDocument doc : docs) {
			accept(doc);
		}
	}

	private void sendRemovesIndividually(Map<LocalDocument, String> idMap) {
		for (Entry<LocalDocument, String> docEntry : idMap.entrySet()) {
			try {
				solr.deleteById(docEntry.getValue());
			} catch (Exception e) {
				failDocument(docEntry.getKey());
			}

		}
	}

	private Map<LocalDocument, String> mapToIds(List<LocalDocument> docs) {
		Map<LocalDocument, String> idMap = new HashMap<LocalDocument, String>();
		for (LocalDocument d : docs) {
			String id = (String) d.getContentField(idField);
			idMap.put(d, id);
		}
		return idMap;
	}

	private void failDocument(LocalDocument doc) {
		try {
			fail(doc);
		} catch (Exception e) {
			Logger.error(
					"Could not fail document with hydra id: " + doc.getID(), e);
		}
	}

	private SolrServer getSolrServer() throws MalformedURLException {
		return new HttpSolrServer(solrDeployPath);
	}

	public void close() throws Exception {

		if (itemCounterSend > 0) {
			send();
		}
	}

	public int getSendLimit() {
		return sendLimit;
	}

	public void setSendLimit(int sendLimit) {
		this.sendLimit = sendLimit;
	}

	public int getSendTimeout() {
		return sendTimeout;
	}

	public void setSendTimeout(int sendTimeout) {
		this.sendTimeout = sendTimeout;
	}

	public Map<String, Object> getFieldMappings() {
		return fieldMappings;
	}

	public void setFieldMappings(Map<String, Object> fieldConfigs) {
		this.fieldMappings = fieldConfigs;
	}

	/**
	 * Only used by the junit test
	 * 
	 * @param sendAll
	 */
	public void setSendAll(boolean sendAll) {
		this.sendAll = sendAll;
	}

	protected void setSolrDeployPath(String solrDeployPath) {
		this.solrDeployPath = solrDeployPath;
	}

	protected void setSolrServer(SolrServer solrInstance) {
		solr = solrInstance;
	}

	public int getCommitWithin() {
		return commitWithin;
	}

	public void setCommitWithin(int commitWithin) {
		this.commitWithin = commitWithin;
	}
}
