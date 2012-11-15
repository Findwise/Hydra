package com.findwise.hydra.stage.groovyrunner;

import groovy.lang.GroovyClassLoader;

import com.findwise.hydra.local.LocalDocument;
import com.findwise.hydra.stage.AbstractProcessStage;
import com.findwise.hydra.stage.Parameter;
import com.findwise.hydra.stage.ProcessException;
import com.findwise.hydra.stage.Stage;

@Stage(description = "Runs a specified Groovy script which may modify a Document")
public class GroovyRunnerStage extends AbstractProcessStage {

	@Parameter(description = "The script to be run. The script should be written " +
			"in Groovy and contain an implementation of " +
			"com.findwise.hydra.stage.groovyrunner.GroovyStage. " +
			"The implementation will be given a Document, and changes to that " +
			"Document will be honored.")
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
