package com.findwise.hydra.stage;

import java.util.ArrayList;
import java.util.List;

import com.findwise.hydra.local.LocalDocument;

@Stage(description = "Allows merging of the content of several different fields into one: either by explicit numeration or by field names matching a regular expression. Can merge by: concatenation (default), adding to list or by arithmetic addition.")
public class MergeFieldsStage extends AbstractProcessStage {
	@Parameter(required = true, description = "The field in which to store the merged fields")
	private String outputField;

	@Parameter(required = true, description = "The fields from which the merge should happen. Should be regular expression patterns, e.g. field[0-9]* will merge all fields named field<number>.")
	private List<String> fromFields;

	@Parameter(description = "If set, clears the designated outputField before " +
			"merging any content into it")
	private boolean clearOutputField = false;

	@Parameter(description = "When merging as strings, this element will be " +
			"used to separate the component parts. Defaults to whitespace (' ')")
	private String separator = " ";

	@Parameter(description = "If set to true, instead of merging fromFields as " +
			"strings, attempts to do arithmetic add as long as all components are " +
			"numbers. If any field is of a non-numeric type, the output may be " +
			"partially added and partially concatenated, in no predictable order")
	private boolean additionIfNumbers = false;

	@Parameter(description = "If set to true, creates a list in the target field " +
			"instead of merging into one String object. If there is already a list " +
			"in the outputField, this list will be appended to. If the outputField " +
			"contains something else, it will become the first element of the list.")
	private boolean createList = false;
	
	public void init() throws RequiredArgumentMissingException {
		if(outputField == null) {
			throw new RequiredArgumentMissingException("Missing required parameter 'outputField'");
		}
		if(fromFields == null) {
			throw new RequiredArgumentMissingException("Missing required parameter 'fromFields'");
		}
	}
	
	@Override
	public void process(LocalDocument doc) throws ProcessException {
		if(clearOutputField) {
			doc.putContentField(outputField, null);
		}
		String[] documentFields = doc.getContentFields().toArray(new String[doc.getContentFields().size()]);
		for(String docFieldName : documentFields) {
			for(String fieldRegex : fromFields) {
				if(docFieldName.matches(fieldRegex)) {
					Object content = doc.getContentField(docFieldName);
					if(content instanceof Iterable<?>) {
						for(Object o : (Iterable<?>)content) {
							addObjectToOutput(doc, o);
						}
					} else {
						addObjectToOutput(doc, content);	
					}
					break;
				}
			}
		}
	}
	
	private void addObjectToOutput(LocalDocument doc, Object content) {
		if(createList) {
			addToOutputList(doc, content);
		} else {
			if(!doc.hasContentField(outputField) || doc.getContentField(outputField) == null) {
				doc.putContentField(outputField, content);
			} else {
				if(additionIfNumbers && content instanceof Number && doc.getContentField(outputField) instanceof Number) {
					doc.putContentField(outputField, addNumbers((Number)doc.getContentField(outputField), (Number) content));
				} else {
					doc.putContentField(outputField, doc.getContentField(outputField)+separator+content);
				}
			}
		}
	}
	
	@SuppressWarnings("unchecked")
	private void addToOutputList(LocalDocument doc, Object content) {
		List<Object> list = new ArrayList<Object>();
		if(doc.hasContentField(outputField)) {
			if(!(doc.getContentField(outputField) instanceof List)) {
				list.add(doc.getContentField(outputField));
			}
			else {
				list = (List<Object>) doc.getContentField(outputField);
			}
		}
		list.add(content);
		doc.putContentField(outputField, list);
	}

	private Number addNumbers(Number x, Number y) {
		if(x instanceof Double && y instanceof Double) {
			return x.doubleValue() + y.doubleValue();
		} else if (x instanceof Double && (y instanceof Long || y instanceof Integer)) {
			return x.doubleValue() + y.longValue();
		} else if ((x instanceof Long || x instanceof Integer) && y instanceof Double) {
			return y.doubleValue() + x.longValue();
		} else {
			return addIntegers(x, y);
		}
	}
	
	private Number addIntegers(Number x, Number y) {
		if(x instanceof Long || y instanceof Long) {
			return x.longValue() + y.longValue();
		} else {
			return x.intValue() + y.intValue();
		}
	}
}
