package com.findwise.hydra.stage;

import com.findwise.hydra.local.LocalDocument;

@Stage
class ImmediateStoppingStage extends AbstractProcessStage {

	@Override
	public void process(LocalDocument doc) throws ProcessException {
		stopStage();
	}
}
