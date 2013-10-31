package com.findwise.hydra.stage;

import com.findwise.hydra.local.LocalDocument;

@Stage
class LimitedCountExceptionStage extends AbstractProcessStage {

	private int count = 0;
	private final int limit;

	public LimitedCountExceptionStage(int limit) {
		super();
		this.limit = limit;
	}

	@Override
	public void process(LocalDocument doc) throws ProcessException {
		try {
			count++;
			throw new ProcessException("processed: " + count);
		} finally {
			if (count >= limit) {
				stopStage();
			}
		}

	}
}
