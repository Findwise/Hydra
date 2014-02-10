package com.findwise.hydra.debugging;

import com.findwise.hydra.local.LocalDocument;
import com.findwise.hydra.stage.AbstractOutputStage;
import com.findwise.hydra.stage.Stage;

@Stage(description="A demo output stage that prints the documents to standard out")
public class StdoutOutput extends AbstractOutputStage {
	@Override
	public void output(LocalDocument document) {
		System.out.println(document.toString());
	}
}
