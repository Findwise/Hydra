package com.findwise.hydra.stage.rules;

import com.findwise.hydra.common.Document;

/**
 * @author joel.westberg
 */
public class FieldExistsRule implements FieldOutputRule {
	private String field;
	
	public FieldExistsRule(String field) {
		this.field = field;
	}
	
	@Override
	public String getField() {
		return field;
	}
	
	@Override
	public boolean verify(Document d) {
		if(!d.hasContentField(field)) {
			return false;
		}
		return true;
	}

	public String toString() {
		return "FieldExistsRule("+field+")";
	}
}
