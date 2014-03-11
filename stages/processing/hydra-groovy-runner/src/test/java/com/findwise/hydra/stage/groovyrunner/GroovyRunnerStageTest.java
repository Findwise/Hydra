package com.findwise.hydra.stage.groovyrunner;

import java.io.InputStream;

import org.apache.commons.io.IOUtils;
import org.junit.Test;

import com.findwise.hydra.local.LocalDocument;

import static org.junit.Assert.assertEquals;

public class GroovyRunnerStageTest {

	@Test
	public void testCanGetSetGroovyScript() {
		GroovyRunnerStage stage = new GroovyRunnerStage();
		String script = "nothing";
		stage.setGroovyScript(script);
		assertEquals(script, stage.getGroovyScript());
	}

	@Test
	public void testCanLoadGroovyStage() throws Exception {
		GroovyRunnerStage runner = new GroovyRunnerStage();
		String className = "DumbGroovyStage";

		String script = "import com.findwise.hydra.local.LocalDocument;"
				+ "import com.findwise.hydra.stage.groovyrunner.GroovyStage;"
				+ " public class " + className + " implements GroovyStage{"
				+ "public void process(LocalDocument doc){}" + "}";
		GroovyStage stage = runner.loadGroovyStage(script);
		assertEquals(className, stage.getClass().getCanonicalName());
	}

	@Test
	public void testGroovyScriptCanModifyDocument() throws Exception {

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

	private String readStringFromFile(String fileName) throws Exception {
		InputStream stream = this.getClass().getClassLoader()
				.getResourceAsStream(fileName);
		return IOUtils.toString(stream);
	}
}
