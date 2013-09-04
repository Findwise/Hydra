package com.findwise.hydra.stage;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import com.findwise.hydra.local.LocalDocument;

/**
 * Adds a field with a specified value to the document.
 *
 * @author joel.westberg
 */
@Stage(description = "Modifies a field with a static value. Can append values to lists, and will create lists if configured to do so.")
public class SetStaticFieldStage extends AbstractProcessStage {
	public static enum Policy {
		OVERWRITE, SKIP, THROW, ADD
	}

	@Parameter(required = true, name = "fieldValueMap", description = "A map of fields to modify, and the values to write to them")
	private Map<String, Object> fieldValueMap;
	@Parameter(name = "overwritePolicy",
			description = "Switch for behaviour when modifying. Available options: 0/OVERWRITE = overwrite content, 1/SKIP = skip if there is content, 2/THROW = throw exception if there is content, 3/ADD = append to content, converting the content to a list if necessary (default)")
	private Policy overwritePolicy = Policy.ADD;

	@Override
	public void process(LocalDocument doc) throws ProcessException {
		for (Map.Entry<String, Object> entry : fieldValueMap.entrySet()) {
			final String key = entry.getKey();
			if (!hasContent(doc, key) || overwritePolicy == Policy.OVERWRITE) {
				doc.putContentField(key, entry.getValue());
			} else if (overwritePolicy == Policy.ADD) {
				addValueToField(doc, key, entry.getValue());
			} else if (overwritePolicy == Policy.THROW) {
				throw new ProcessException("Field " + key
						+ " already has a value!");
			}
		}
	}

	private static boolean hasContent(LocalDocument doc, String key) {
		if (!doc.hasContentField(key)) {
			return false;
		}

		Object value = doc.getContentField(key);
		if (value instanceof String) {
			return !((String) value).isEmpty();
		} else if (value instanceof Collection) {
			return !((Collection) value).isEmpty();
		} else {
			return true;
		}
	}

	@SuppressWarnings("unchecked")
	private void addValueToField(LocalDocument doc, String fieldName,
			Object fieldValue) {
		if (!doc.hasContentField(fieldName)) {
			doc.putContentField(fieldName, fieldValue);
		} else {
			Object value = doc.getContentField(fieldName);
			List<Object> list;
			if (value instanceof List) {
				list = (List<Object>) value;
			} else {
				list = new ArrayList<Object>();
				list.add(value);
			}
			list.add(fieldValue);
			doc.putContentField(fieldName, list);
		}
	}
}
