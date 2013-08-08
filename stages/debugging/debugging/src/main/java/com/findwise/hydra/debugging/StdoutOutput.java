package com.findwise.hydra.debugging;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.findwise.hydra.local.LocalDocument;
import com.findwise.hydra.stage.AbstractOutputStage;
import com.findwise.hydra.stage.Stage;

@Stage(description="A demo output stage that prints the documents to standard out")
public class StdoutOutput extends AbstractOutputStage {

	Logger logger = LoggerFactory.getLogger(StdoutOutput.class);

	@Override
	public void output(LocalDocument document) {
		System.out.println(document.toString());
		try {
			accept(document);
		} catch (Exception e) {
			logger.error("Could not accept document with hydra id: " + document.getID(), e);
		}
	}

}
