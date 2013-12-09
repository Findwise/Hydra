package com.findwise.hydra.stage;

import com.findwise.hydra.local.LocalDocument;

public abstract class AbstractOutputStage extends AbstractProcessStage {
	public void process(LocalDocument document) throws ProcessException {
		output(document);
	}

	// For backwards compatibility, we keep this method.
	public abstract void output(LocalDocument document) throws ProcessException;

	@Override
	public boolean isFailDocumentOnProcessException() {
		return true;
	}
}
