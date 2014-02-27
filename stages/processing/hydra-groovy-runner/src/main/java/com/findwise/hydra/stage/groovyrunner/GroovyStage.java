package com.findwise.hydra.stage.groovyrunner;

import com.findwise.hydra.local.LocalDocument;

public interface GroovyStage {
	public void process(LocalDocument doc) throws Exception;
}
