package com.findwise.hydra.stage;

import com.findwise.hydra.Logger;
import com.findwise.hydra.local.LocalDocument;

/**
 * A basic Copy-stage
 */
@Stage(description = "Copies values from one field to another.")
public class CopyStage extends AbstractMappingProcessStage {
	@Override
	public void stageInit() throws RequiredArgumentMissingException { }

	@Override
	public void processField(LocalDocument doc, String fromField, String toField)
			throws ProcessException {
		Object val = doc.getContentField(fromField);
		doc.putContentField(toField, val);
		String valString = (val == null) ? null : val.toString();
		Logger.debug("Copying field " + fromField + " to field " + toField
				+ " value: " + valString);
	}
}
