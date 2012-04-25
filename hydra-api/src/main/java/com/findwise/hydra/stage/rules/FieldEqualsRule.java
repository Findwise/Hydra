package com.findwise.hydra.stage.rules;

import com.findwise.hydra.common.Document;

/**
 * @author joel.westberg
 */
public class FieldEqualsRule implements FieldOutputRule {
	private String field;
	private Object value;
	
	public FieldEqualsRule(String field, Object value) {
		this.field = field;
		this.value = value;
	}
	
	@Override
	public boolean verify(Document d) {
		if(!d.hasContentField(field)) {
			return false;
		}
		return d.getContentField(field).equals(value);
	}

	@Override
	public String getField() {
		return field;
	}
	
}
