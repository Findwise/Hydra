package com.findwise.hydra.stage;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import com.findwise.hydra.local.LocalDocument;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Adds a field with a specified value to the document.
 * 
 * @author simon.stenstrom
 */
@Stage(description = "Modifies a field with a static value. Can append values to a list, ")
public class SetStaticFieldStage extends AbstractProcessStage {
    private static Logger logger = LoggerFactory.getLogger(SetStaticFieldStage.class);

	private static final int OVERWRITE = 0;
	private static final int SKIP = 1;
	private static final int THROW = 2;
	private static final int ADD = 3;

	private static final int DEFAULTOVERWRITEPOLICY = SetStaticFieldStage.ADD;

	private int overwritePolicy = SetStaticFieldStage.DEFAULTOVERWRITEPOLICY;

	@Parameter(name = "fieldNames", required = true, description = "List of fields to modify")
	private List<String> fieldNames;
	@Parameter(name = "fieldValues", required = true, description = "List of values to modify each respective field with")
	private List<String> fieldValues;
	@Parameter(name = "overwrite", 
			description = "Switch for behaviour when modifying; " +
			"0 = overwrite content, " +
			"1 = skip if there is content, " +
			"2 = throw exception if there is content, " +
			"3 = append to content (default)")
	private String overwrite;

	public void init() throws RequiredArgumentMissingException {
		if (this.fieldNames == null) {
			throw new RequiredArgumentMissingException("fieldName missing");
		}
		if (this.fieldValues == null) {
			throw new RequiredArgumentMissingException("fieldValue missing");
		}
		if (overwrite != null) {
			try {
				overwritePolicy = Integer.parseInt(overwrite);
			} catch (NumberFormatException e) {
				logger.error("The overwrite field could not be parsed. Using default");
				overwritePolicy = SetStaticFieldStage.DEFAULTOVERWRITEPOLICY;
			}
		}
	}

	@Override
	public void process(LocalDocument doc) throws ProcessException {
		if (fieldNames == null || fieldValues == null) {
			throw new ProcessException(
					"fieldName and/or fieldValue was not set. Probably because init() have not been called");
		}

		for (int i = 0; i < fieldNames.size(); i++) {
			if (overwritePolicy == ADD) {
				addValueToField(doc, fieldNames.get(i), fieldValues.get(i));
				logger.debug("Added field " + fieldNames + " with value "
						+ fieldValues);
			} else if (overwritePolicy == OVERWRITE) {
				setValueToField(doc, fieldNames.get(i), fieldValues.get(i));
			} else if (overwritePolicy == SKIP) {
				if (!hasValue(doc, fieldNames.get(i))) {
					setValueToField(doc, fieldNames.get(i), fieldValues.get(i));
				}
			} else if (overwritePolicy == THROW) {
				if (hasValue(doc, fieldNames.get(i))) {
					throw new ProcessException("Field " + fieldNames.get(i)
							+ " already has a value!");
				}
			}
		}
	}

	private void addValueToField(LocalDocument doc, String fieldName,
			String fieldValue) {

		Object object = doc.getContentField(fieldName);
		ArrayList<String> valueList;
		if (object == null) {
			valueList = new ArrayList<String>();
		} else if (object instanceof String[]) {
			String[] values;
			values = (String[]) object;

			valueList = new ArrayList<String>(
					Arrays.asList(values));

		} else if (object instanceof String) {
			valueList = new ArrayList<String>();
			valueList.add((String) object);
		} else {
			logger.info("Deleting value " + object + ". Object in field " + fieldName + " was not of type String[] nor String.");
			valueList = new ArrayList<String>();
		}
		valueList.add(fieldValue);
		doc.putContentField(fieldName, valueList.toArray());

	}

	private void setValueToField(LocalDocument doc, String fieldName,
			String fieldValue) {
		String[] values = { fieldValue };
		doc.putContentField(fieldName, values);
	}

	private boolean hasValue(LocalDocument doc, String field) {

		Object data = doc.getContentField(field);
		
		if (data instanceof String[] || data instanceof String) {
			return (data != null);
		} else {
			logger.info("Data ins field " + field + " was not of type String[] nor String. hasValue returned false");
			return false;
		}
	}

}
