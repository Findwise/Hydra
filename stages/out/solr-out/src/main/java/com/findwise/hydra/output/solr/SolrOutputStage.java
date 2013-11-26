package com.findwise.hydra.output.solr;

import java.io.IOException;
import java.net.MalformedURLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.findwise.hydra.stage.InitFailedException;
import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.HttpSolrServer;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrInputDocument;

import com.findwise.hydra.Document;
import com.findwise.hydra.Document.Action;
import com.findwise.hydra.local.LocalDocument;
import com.findwise.hydra.stage.AbstractOutputStage;
import com.findwise.hydra.stage.Parameter;
import com.findwise.hydra.stage.RequiredArgumentMissingException;
import com.findwise.hydra.stage.Stage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Stage(description="Writes documents to Solr")
public class SolrOutputStage extends AbstractOutputStage {
    private static Logger logger = LoggerFactory.getLogger(SolrOutputStage.class);

	@Parameter(required = true, description = "The URL of the Solr to which this stage will post data")
	private String solrDeployPath;
	@Parameter(description = "A map specifying which fields in the Hydra document becomes which fields in Solr. The value of an entry must be one of either String or List<String>.")
	private Map<String, Object> fieldMappings = new HashMap<String, Object>();
	@Parameter(description = "If set, fieldMappings will be ignored and all fields will be sent to Solr.")
	private boolean sendAll = false;
	@Parameter
	private String idField = "id";
	@Parameter
	private int commitWithin = 0;

	private SolrServer solr;

	@Override
	public void output(LocalDocument doc) {
		final Action action = doc.getAction();

		try {
			if (action == Action.ADD || action == Action.UPDATE) {
				add(doc);
			} else if (action == Action.DELETE) {
				delete(doc);
			} else {
				failDocument(doc, new RequiredArgumentMissingException("action not set in document. This document would never be sent to solr"));
			}
		} catch (SolrException e) {
			failDocument(doc, e);
		} catch (SolrServerException e) {
			failDocument(doc, e);
		} catch (IOException e) {
			failDocument(doc, e);
		} catch (RequiredArgumentMissingException e) {
			failDocument(doc, e);
		}
	}

	@Override
	public void init() throws RequiredArgumentMissingException, InitFailedException {
		try {
			solr = getSolrServer();
		} catch (MalformedURLException e) {
			throw new InitFailedException("Solr URL malformed", e);
		}
	}
	
	private void add(LocalDocument doc) throws SolrException, SolrServerException, IOException {
		SolrInputDocument solrdoc = createSolrInputDocumentWithFieldConfig(doc);
		if (getCommitWithin() != 0) {
			solr.add(solrdoc, getCommitWithin());
		}
		else {
			solr.add(solrdoc);
		}
		accept(doc);
	}
	
	private void delete(LocalDocument doc) throws SolrServerException, IOException, RequiredArgumentMissingException {
		if(!doc.hasContentField(idField)) {
			throw new RequiredArgumentMissingException("Document has no ID field");
		}
		if (getCommitWithin() != 0) {
			solr.deleteById(doc.getContentField(idField).toString(), getCommitWithin());
		} else {
			solr.deleteById(doc.getContentField(idField).toString());
		}
		accept(doc);
	}
	

	protected SolrInputDocument createSolrInputDocumentWithFieldConfig(
			Document<?> doc) {
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
	private void addField(Document<?> doc, SolrInputDocument inputDoc, String field) {
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

	private void failDocument(LocalDocument doc, Throwable reason) {
		try {
			logger.error("Failing document "+doc.getID(), reason);
			fail(doc, reason);
		} catch (Exception e) {
			logger.error("Could not fail document with hydra id: " + doc.getID(), e);
		}
	}

	private SolrServer getSolrServer() throws MalformedURLException {
		return new HttpSolrServer(solrDeployPath);
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
	protected void setSendAll(boolean sendAll) {
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
