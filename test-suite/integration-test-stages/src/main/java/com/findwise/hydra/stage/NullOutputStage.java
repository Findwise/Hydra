package com.findwise.hydra.stage;

import com.findwise.hydra.local.LocalDocument;
import com.findwise.hydra.stage.AbstractOutputStage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

@Stage
public class NullOutputStage extends AbstractOutputStage {
	Logger logger = LoggerFactory.getLogger(NullOutputStage.class);

	@Override
	public void output(LocalDocument document) {
		try {
			logger.info("Accepting document: " + document.getID());
			accept(document);
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}
}
