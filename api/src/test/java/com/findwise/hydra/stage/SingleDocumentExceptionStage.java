package com.findwise.hydra.stage;

import com.findwise.hydra.local.LocalDocument;

@Stage
class SingleDocumentExceptionStage extends AbstractProcessStage {

	@Override
	public void process(LocalDocument doc) throws ProcessException {
		try {
			throw new ProcessException("msg");
		} finally {
			stopStage();
		}
	}
}
