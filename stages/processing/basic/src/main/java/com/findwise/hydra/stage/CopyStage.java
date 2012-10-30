package com.findwise.hydra.stage;

import com.findwise.hydra.common.Logger;
import com.findwise.hydra.local.LocalDocument;

/**
 * A basic Copy-stage.
 */
@Stage(description = "Copies values from one field to another.")
public class CopyStage extends AbstractMappingProcessStage {

    @Parameter(name = "prefix", description = "Input field prefix")
    private String prefix = "";
    @Parameter(name = "postfix", description = "Input field postfix")
    private String postfix = "";

    @Override
    public void stageInit() throws RequiredArgumentMissingException {
    }

    @Override
    public void processField(LocalDocument doc, String fromField, String toField)
            throws ProcessException {
        String fromFieldStr = prefix + fromField + postfix;
        Object val = doc.getContentField(fromFieldStr);
        doc.putContentField(toField, val);
        String valString = (val == null) ? null : val.toString();
        Logger.debug("Copying field " + fromFieldStr + " to field " + toField
                + " value: " + valString);
    }

    public void setPostfix(String postfix) {
        this.postfix = postfix;
    }

    public void setPrefix(String prefix) {
        this.prefix = prefix;
    }
}
