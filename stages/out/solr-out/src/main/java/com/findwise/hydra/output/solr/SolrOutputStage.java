package com.findwise.hydra.output.solr;

import java.io.IOException;
import java.net.MalformedURLException;
import java.util.ArrayList;
import java.util.Date;
import java.util.LinkedList;
import java.util.List;

import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.impl.CommonsHttpSolrServer;
import org.apache.solr.common.SolrInputDocument;

import com.findwise.hydra.common.Document;
import com.findwise.hydra.common.Document.Action;
import com.findwise.hydra.common.Logger;
import com.findwise.hydra.local.LocalDocument;
import com.findwise.hydra.output.DocumentAction;
import com.findwise.hydra.stage.AbstractOutputStage;
import com.findwise.hydra.stage.Parameter;
import com.findwise.hydra.stage.RequiredArgumentMissingException;
import com.findwise.hydra.stage.Stage;
import java.util.*;

@Stage
public class SolrOutputStage extends AbstractOutputStage {

    @Parameter
    private String solrDeployPath;
    @Parameter
    private Map<String, String> fieldMappings;
    @Parameter
    private boolean sendAll = false;
    private int itemCounterCommit;
    private int itemCounterSend;
    private int commitLimit;
    private int sendLimit;
    private int sendTimeout;
    private int commitTimeout;
    private long lastSend = 0;
    private long lastCommit = 0;
    private SolrServer solr;
    private List<DocumentAction> documents;

    protected void setSolrDeployPath(String solrDeployPath) {
        this.solrDeployPath = solrDeployPath;
    }

    protected void setSolrServer(SolrServer solrInstance) {
        solr = solrInstance;
    }

    @Override
    public void init() throws RequiredArgumentMissingException {
        documents = new ArrayList<DocumentAction>();
        fieldMappings = new HashMap<String, String>();


        try {
            solr = getSolrServer();
        } catch (MalformedURLException e) {
            Logger.error("Solr URL malformed.");
        }
    }

    private synchronized void addAction(LocalDocument doc) throws Exception {
        boolean newlist = false;
        if (!documents.isEmpty()) {
            DocumentAction o = documents.get(documents.size() - 1);
            if (o.getType().equals(DocumentAction.types.list_add)) {
                o.getAddlist().add(doc);
            } else {
                newlist = true;
            }
        } else {
            newlist = true;
        }
        if (newlist) {
            List<LocalDocument> newList = new LinkedList<LocalDocument>();
            newList.add(doc);
            documents.add(new DocumentAction(DocumentAction.types.list_add, newList));
        }

        countUpSend();
        countUpCommit();
    }

    private synchronized void deleteAction(LocalDocument doc) throws Exception {

        boolean newlist = false;
        if (!documents.isEmpty()) {
            DocumentAction o = documents.get(documents.size() - 1);
            if (o.getType().equals(DocumentAction.types.list_delete)) {
                o.getRemovelist().add(doc);
            } else {
                newlist = true;
            }
        } else {
            newlist = true;
        }
        if (newlist) {
            List<LocalDocument> newList = new LinkedList<LocalDocument>();
            newList.add(doc);
            documents.add(new DocumentAction(DocumentAction.types.list_delete, newList));
        }
        countUpSend();
        countUpCommit();
    }

    protected void notifyInternal() {
        try {
            checkTimeouts();
        } catch (Exception e) {
            Logger.error("Failed to send or commit, I should probably crash.", e);
        }
    }

    private synchronized void checkTimeouts() throws Exception {
        long now = new Date().getTime();
        if (now - lastSend >= sendTimeout && sendTimeout > 0) {
            send();
        }

        if (now - lastCommit >= commitTimeout && commitTimeout > 0) {
            commit(true);
        }
    }

    private void countUpSend() throws Exception {
        itemCounterSend++;
        if (itemCounterSend >= sendLimit) {
            send();
        }
    }

    private void acceptDocuments(List<LocalDocument> docs) {
        try {
            for (LocalDocument doc : docs) {
                accept(doc);
            }
        } catch (Exception e) {
        }
    }

    private List<SolrInputDocument> getSolrInputDocuments(List<LocalDocument> docs) {
        List<SolrInputDocument> toAdd = new LinkedList<SolrInputDocument>();
        for (Document d : docs) {
            toAdd.add(createSolrInputDocumentWithFieldConfig(d));
        }
        return toAdd;
    }

    protected SolrInputDocument createSolrInputDocumentWithFieldConfig(Document doc) {
        SolrInputDocument docToAdd = new SolrInputDocument();

        if (sendAll) {
            for (String inField : doc.getContentFields()) {
                docToAdd.addField(inField, doc.getContentField(inField));
            }
        } else {
            for (String inField : fieldMappings.keySet()) {
                if (doc.hasContentField(inField)) {
                    docToAdd.addField(fieldMappings.get(inField), doc.getContentField(inField));
                }
            }
        }
        return docToAdd;
    }

    private List<String> getIDs(List<LocalDocument> docs) {
        List<String> toDelete = new LinkedList<String>();
        for (Document d : docs) {
            String id = (String) d.getContentField("id");
            if (id == null) {
                id = (String) d.getContentField("ID");
            }
            toDelete.add(id);
        }
        return toDelete;
    }

    private void send() throws Exception {
        if (solr == null) {
            solr = getSolrServer();
        }

        if (documents.size() > 0 && itemCounterSend > 0) {
            try {
                Logger.info("Running send() to Solr");
                for (DocumentAction da : documents) {
                    if (da.getType().equals(DocumentAction.types.list_add)) {
                        solr.add(getSolrInputDocuments(da.getAddlist()));
                        acceptDocuments(da.getAddlist());
                    } else if (da.getType().equals(DocumentAction.types.list_delete)) {
                        commit(false);
                        solr.deleteById(getIDs(da.getRemovelist()));
                        acceptDocuments(da.getRemovelist());
                    }
                }
                itemCounterSend = 0;
                documents = new LinkedList<DocumentAction>();

            } catch (IOException e) {
                itemCounterSend = 0;
                documents = new LinkedList<DocumentAction>();
                throw e;
            }
        }
        lastSend = new Date().getTime();
    }

    private void countUpCommit() throws Exception {
        itemCounterCommit++;
        if (commitLimit >= 0 && itemCounterCommit >= commitLimit) { //Only send commits if limit is greater than zero
            commit(true);
        }
    }

    private void commit(Boolean send) throws Exception {
        if (solr == null) {
            solr = getSolrServer();
        }
        if (itemCounterCommit > 0) {
            Logger.info("Running commit to Solr");
            if (send) {
                send();
            }
            solr.commit();
            itemCounterCommit = 0;
        }
        lastCommit = new Date().getTime();
    }

    private SolrServer getSolrServer() throws MalformedURLException {
        return new CommonsHttpSolrServer(solrDeployPath);
    }

    @Override
    public void output(LocalDocument doc) {
        final Action action = doc.getAction();

        try {
            if (action == Action.ADD) {
                addAction(doc);
            } else if (action == Action.DELETE) {
                deleteAction(doc);
            }
            checkTimeouts();
        } catch (Exception e) {
            //I should crash shouldn't I?
            Logger.error("I should crash shouldn't I?", e);
        }
    }

    public void close() throws Exception {

        if (itemCounterSend > 0) {
            send();
        }
        if (itemCounterCommit > 0) {
            commit(true);
        }
    }

    public int getCommitLimit() {
        return commitLimit;
    }

    public void setCommitLimit(int commitLimit) {
        this.commitLimit = commitLimit;
    }

    public int getCommitTimeout() {
        return commitTimeout;
    }

    public void setCommitTimeout(int commitTimeout) {
        this.commitTimeout = commitTimeout;
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

    public Map<String, String> getFieldMappings() {
        return fieldMappings;
    }

    public void setFieldMappings(Map<String, String> fieldConfigs) {
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
}
