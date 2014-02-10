package com.findwise.hydra.stage.groovyrunner

import com.findwise.hydra.local.LocalDocument
import org.junit.Test

import static org.junit.Assert.assertEquals;

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
