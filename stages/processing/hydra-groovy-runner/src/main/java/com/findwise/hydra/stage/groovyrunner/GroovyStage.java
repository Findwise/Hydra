package com.findwise.hydra.stage.groovyrunner;

import com.findwise.hydra.local.LocalDocument;
import com.findwise.hydra.stage.ProcessException;

public interface GroovyStage {
	public void process(LocalDocument doc) throws ProcessException;
}
