package com.findwise.hydra.stage;

import java.io.IOException;

import com.findwise.hydra.common.Logger;
import com.findwise.hydra.local.LocalDocument;
import com.findwise.hydra.local.LocalQuery;

public abstract class AbstractInputStage extends AbstractStage {

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
				Logger.error("Caught excpetion while running", e);
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
		
		Logger.debug("idField: " + idField + " value: " + ld.getContentField(idField));
		discardDocumentsWithValue(idField, ld.getContentField(idField));
	}

	private void discardDocumentsWithValue(String fieldName, Object fieldValue) {
		Logger.debug("Discard");
		LocalDocument ld;
		LocalQuery lq = new LocalQuery();
		lq.requireContentFieldEquals(fieldName, fieldValue);

		Logger.debug("Local query is: " + lq.toJson());

		try {
			ld = getRemotePipeline().getDocument(lq);
			while (ld != null) {
				Logger.debug("Found document: " + ld.getID());
				getRemotePipeline().markDiscarded(ld);
				Logger.debug("Discarded document: " + ld.toJson());
				ld = getRemotePipeline().getDocument(lq);
			}
		} catch (IOException e) {
			Logger.error("IOException while trying to discard");
			e.printStackTrace();
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
