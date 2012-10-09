package com.findwise.hydra.stage.groovyrunner;

import groovy.lang.GroovyClassLoader;

import com.findwise.hydra.local.LocalDocument;
import com.findwise.hydra.stage.AbstractProcessStage;
import com.findwise.hydra.stage.ProcessException;

public class GroovyRunnerStage extends AbstractProcessStage {

	private String groovyScript;
	private GroovyStage instantiatedGroovyStage;

	@Override
	public void process(LocalDocument doc) throws ProcessException {
		instantiatedGroovyStage.process(doc);
	}

	@Override
	public void init() {
		try {
			this.instantiatedGroovyStage = loadGroovyStage(groovyScript);
		} catch (Exception e) {
			throw new RuntimeException("Could not load groovy stage", e);
		}
	}

	@SuppressWarnings("unchecked")
	public GroovyStage loadGroovyStage(String script)
			throws InstantiationException, IllegalAccessException {
		GroovyClassLoader loader = new GroovyClassLoader(this.getClass()
				.getClassLoader());
		Class<GroovyStage> parsedClass = loader.parseClass(script);
		return parsedClass.newInstance();
	}

	public void setGroovyScript(String script) {
		this.groovyScript = script;
	}

	public String getGroovyScript() {
		return this.groovyScript;
	}

}
