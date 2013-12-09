package com.findwise.hydra.stage;

import com.findwise.hydra.local.LocalDocument;

@Stage
class InterruptibleWaitingStage extends AbstractProcessStage {

	@Override
	public void process(LocalDocument doc) throws ProcessException {
		try {
			while (true) {
				Thread.sleep(100);
			}
		} catch (InterruptedException e) {
			return;
		}
	}
}
