package com.findwise.hydra.stage.groovyrunner;

import static org.junit.Assert.*;
import org.junit.Test;

import com.findwise.hydra.local.LocalDocument;

class FieldAddingGroovyStageTest {

	@Test
	def void testIsAddingField(){
		FieldAddingGroovyStage stage = new FieldAddingGroovyStage()
		LocalDocument doc = new LocalDocument()
		stage.process(doc)
		def actualValue = doc.getContentField("fieldName")
		assertEquals("value", actualValue)
	}
}
