package com.findwise.hydra.stage;

import com.findwise.hydra.local.LocalDocument;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Stage
public class HangingStage extends AbstractProcessStage {
	Logger logger = LoggerFactory.getLogger(HangingStage.class);

	@Override
	public void process(LocalDocument doc) {
		while (true) {
			try {
				Thread.sleep(1000);
			} catch (InterruptedException e) {}
		}
	}
}
