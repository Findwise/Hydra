package com.findwise.hydra.stage.groovyrunner;

import java.io.IOException;
import java.io.InputStream;

import junit.framework.Assert;
import org.apache.commons.io.IOUtils;
import org.junit.Test;

import com.findwise.hydra.local.LocalDocument;
import com.findwise.hydra.stage.ProcessException;
import com.findwise.hydra.stage.RequiredArgumentMissingException;

import static org.junit.Assert.assertEquals;

public class GroovyRunnerStageTest {

	@Test
	public void testCanGetSetGroovyScript() {
		GroovyRunnerStage stage = new GroovyRunnerStage();
		String script = "nothing";
		stage.setGroovyScript(script);
		Assert.assertEquals(script, stage.getGroovyScript());
	}

	@Test
	public void testCanLoadGroovyStage() throws InstantiationException,
			IllegalAccessException {
		GroovyRunnerStage runner = new GroovyRunnerStage();
		String className = "DumbGroovyStage";

		String script = "import com.findwise.hydra.local.LocalDocument;"
				+ "import com.findwise.hydra.stage.groovyrunner.GroovyStage;"
				+ " public class " + className + " implements GroovyStage{"
				+ "public void process(LocalDocument doc){}" + "}";
		GroovyStage stage = runner.loadGroovyStage(script);
		Assert.assertEquals(className, stage.getClass().getCanonicalName());
	}

	@Test
	public void testGroovyScriptCanModifyDocument() throws IOException,
			InstantiationException, IllegalAccessException, ProcessException,
			RequiredArgumentMissingException {

		GroovyRunnerStage runner = new GroovyRunnerStage();
		String script = readStringFromFile("com" + IOUtils.DIR_SEPARATOR
				+ "findwise" + IOUtils.DIR_SEPARATOR + "hydra"
				+ IOUtils.DIR_SEPARATOR + "stage" + IOUtils.DIR_SEPARATOR
				+ "groovyrunner" + IOUtils.DIR_SEPARATOR
				+ "FieldAddingGroovyStage.groovy");
		runner.setGroovyScript(script);
		runner.init();

		LocalDocument doc = new LocalDocument();
		runner.process(doc);
		String actualValue = (String) doc.getContentField("fieldName");
		assertEquals("value", actualValue);
	}

	private String readStringFromFile(String fileName) throws IOException {
		InputStream stream = this.getClass().getClassLoader()
				.getResourceAsStream(fileName);
		return IOUtils.toString(stream);
	}
}
