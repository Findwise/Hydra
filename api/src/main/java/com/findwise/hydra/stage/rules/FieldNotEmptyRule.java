package com.findwise.hydra.stage.rules;

import com.findwise.hydra.common.Document;

/**
 * @author joel.westberg
 */
public class FieldNotEmptyRule implements FieldOutputRule {
	private String field;
	
	public FieldNotEmptyRule(String field) {
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
		if(d.getContentField(field).toString().trim().equals("")) {
			return false;
		}
		return true;
	}

	public String toString() {
		return "FieldNotEmptyRule("+field+")";
	}

}
