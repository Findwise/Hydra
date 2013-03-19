package com.findwise.hydra.stage;

import java.io.IOException;

import com.findwise.hydra.local.LocalDocument;
import com.findwise.hydra.local.LocalQuery;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class AbstractInputStage extends AbstractStage {
    Logger logger = LoggerFactory.getLogger(AbstractInputStage.class);
	@Parameter
	protected String idField;
	@Parameter
	protected boolean discardOldDocuments=true;
	
	@Override
	public void run() {

		setContinueRunning(true);
		while (isContinueRunning()) {
			try {
				Thread.sleep(DEFAULT_HOLD_INTERVAL);
			} catch (Exception e) {
				logger.error("Caught exception while running", e);
				Runtime.getRuntime().removeShutdownHook(getShutDownHook());
				System.exit(1);
			}
		}
	}
	
	protected void discardOld(LocalDocument ld) throws RequiredArgumentMissingException {

		if(!discardOldDocuments) {
			return;
		}
		if(idField == null) {
			throw new RequiredArgumentMissingException("Input stage is set to discard old documents but idField is not specified.");
		}
		
		logger.debug("idField: " + idField + " value: " + ld.getContentField(idField));
		discardDocumentsWithValue(idField, ld.getContentField(idField));
	}

	private void discardDocumentsWithValue(String fieldName, Object fieldValue) {
		logger.debug("Discard");
		LocalDocument ld;
		LocalQuery lq = new LocalQuery();
		lq.requireContentFieldEquals(fieldName, fieldValue);

		logger.debug("Local query is: " + lq.toJson());

		try {
			ld = getRemotePipeline().getDocument(lq);
			while (ld != null) {
				logger.debug("Found document: " + ld.getID());
				getRemotePipeline().markDiscarded(ld);
				logger.debug("Discarded document: " + ld.toJson());
				ld = getRemotePipeline().getDocument(lq);
			}
		} catch (IOException e) {
			logger.error("IOException while trying to discard");
			throw new RuntimeException(e);
		}
	}

	public String getIdField() {
		return idField;
	}

	public void setIdField(String idField) {
		this.idField = idField;
	}

	public boolean isDiscardOldDocuments() {
		return discardOldDocuments;
	}

	public void setDiscardOldDocuments(boolean discardOldDocuments) {
		this.discardOldDocuments = discardOldDocuments;
	}
}
