package com.findwise.hydra.stage;

import com.findwise.hydra.local.LocalDocument;

@Stage
class LimitedCountInterruptibleWaitingStage extends AbstractProcessStage {

	private int count = 0;
	private final int limit;

	public LimitedCountInterruptibleWaitingStage(int limit) {
		this.limit = limit;
	}

	@Override
	public void process(LocalDocument doc) throws ProcessException {
		try {
			count++;
			while (true) {
				sleep(100);
			}
		} catch (InterruptedException e) {
			return;
		} finally {
			if (count >= limit) {
				stopStage();
			}
		}
	}
}
