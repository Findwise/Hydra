package com.findwise.hydra.stage.groovyrunner

import com.findwise.hydra.local.LocalDocument;

class FieldAddingGroovyStage implements GroovyStage{
	
	String fieldName, value
	def FieldAddingGroovyStage(){
		this.fieldName = "fieldName"
		this.value = "value"
	}

	def void process(LocalDocument doc){
		doc.putContentField(fieldName, value)
	}
}
