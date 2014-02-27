package com.findwise.hydra.stage;

import com.findwise.hydra.local.LocalDocument;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Stage
public class NullOutputStage extends AbstractOutputStage {
	Logger logger = LoggerFactory.getLogger(NullOutputStage.class);

	@Override
	public void output(LocalDocument document) {
		logger.info("Accepting document: " + document.getID());
	}
}
