package com.findwise.hydra.debugging;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.findwise.hydra.local.LocalDocument;
import com.findwise.hydra.stage.AbstractProcessStage;
import com.findwise.hydra.stage.ProcessException;
import com.findwise.hydra.stage.Stage;

@Stage(description = "A stage that gets stuck in an infinite loop and never returns. Warning: This stage cannot be interrupted")
public class InfiniteLoopStage extends AbstractProcessStage {

	Logger logger = LoggerFactory.getLogger(InfiniteLoopStage.class);

	@Override
	public void process(LocalDocument doc) throws ProcessException {
		for (int i = 0; true; i++) {
			doc.putContentField("field" + i, i);
			try {
				Thread.sleep(1000);
			} catch (InterruptedException e) {
			}
		}
	}

}
