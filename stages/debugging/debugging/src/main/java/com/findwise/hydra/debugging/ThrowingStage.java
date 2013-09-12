package com.findwise.hydra.debugging;

import com.findwise.hydra.local.LocalDocument;
import com.findwise.hydra.stage.AbstractProcessStage;
import com.findwise.hydra.stage.ProcessException;
import com.findwise.hydra.stage.Stage;

@Stage
public class ThrowingStage extends AbstractProcessStage {

	@Override
	public void process(LocalDocument doc) throws ProcessException {
		throw new RuntimeException();
	}

}
