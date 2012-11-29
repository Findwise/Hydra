package com.findwise.hydra.stage;
import java.util.Random;

import com.findwise.hydra.local.LocalDocument;
import com.findwise.hydra.stage.AbstractProcessStage;
import com.findwise.hydra.stage.Parameter;
import com.findwise.hydra.stage.ProcessException;
import com.findwise.hydra.stage.Stage;

@Stage
public class SleepyStage extends AbstractProcessStage {
	@Parameter
	private int timeToSleep = 500;
	
	private Random r = new Random();
	
	@Override
	public void process(LocalDocument doc) throws ProcessException {
		try {
			Thread.sleep(timeToSleep);
		} catch(InterruptedException e) {
			throw new ProcessException(e);
		}
		doc.putContentField("sleepy", doc.getContentFields().toArray()[r.nextInt(doc.getContentFields().size())]);
		doc.putContentField("sleepy2", doc.getContentFields().toArray()[r.nextInt(doc.getContentFields().size())]);
		doc.putContentField("sleepy3", doc.getContentFields().toArray()[r.nextInt(doc.getContentFields().size())]);
	}
	
}
