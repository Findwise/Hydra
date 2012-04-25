package com.findwise.hydra.stage;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import com.findwise.hydra.common.Logger;
import com.findwise.hydra.local.LocalDocument;

/**
 * A basic Copy-stage
 */
@Stage(description = "A stage that maps one exact value to another")
public class MapStringsStage extends AbstractProcessStage {
	@Parameter(description="The field to read the input from")
	private String inField;
	@Parameter(description="The field to save the output in")
	private String outField;
	@Parameter(description="A map with the values that should be mapped")
	private Map<String, String> map;

	@Override
	public void process(LocalDocument doc) throws ProcessException {

		Object value = doc.getContentField(inField);
		if (value == null) {
			Logger.debug("Did not have a " + inField + "-field");
			return;
		}
		if (value instanceof String) {

			String stringValue = (String) value;
			String replace = getMap().get(stringValue);
			doc.putContentField(outField, replace == null ? stringValue
					: replace);

		} else if (value instanceof List<?>) {
			List<String> outData = new ArrayList<String>();
			for (Object listValue : (List<?>) value) {
				if (listValue instanceof String) {
					String stringValue = (String) listValue;
					String replace = getMap().get(stringValue);
					outData.add(replace == null ? stringValue : replace);
				} else {
					Logger.warn("List did not contain all Strings");
				}
			}
			doc.putContentField(outField, outData);
		} else {
			Logger.warn("Field type of inField was not recognized. Valid field types are String and List<String>");
		}
	}

	@Override
	public void init() throws RequiredArgumentMissingException {
		if (inField == null || outField == null || getMap() == null) {
			throw new RequiredArgumentMissingException("Missing argument!");
		}
	}

	public void setInField(String inField) {
		this.inField = inField;
	}

	public void setOutField(String outField) {
		this.outField = outField;
	}

	public Map<String, String> getMap() {
		return map;
	}

	public void setMap(Map<String, String> map) {
		this.map = map;
	}

}
