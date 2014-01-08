package com.findwise.hydra.stage;

import com.findwise.hydra.local.LocalDocument;

@Stage
public class InitRequiredStage extends AbstractProcessStage {

	private boolean hasInitialized = false;

	@Override
	public void init() throws RequiredArgumentMissingException, InitFailedException {
		this.hasInitialized = true;
	}

	@Override
	public void process(LocalDocument document) throws Exception {
		if (!hasInitialized) {
			throw new RuntimeException("Stage not initialized");
		}
	}
}
