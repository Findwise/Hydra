package com.findwise.hydra.stage;

import com.findwise.hydra.common.Logger;
import com.findwise.hydra.local.LocalDocument;
import java.util.List;

/**
 * A stage that sets the desired fields to null (removing them)
 * @author Roar Granevang
 */
@Stage(description = "Removes fields. (Sets them to null)")
public class RemoveFields extends AbstractProcessStage {

    @Parameter(name = "removeFields", description = "What fields to remove from a hydra document")
    private List<String> removeFields;

    @Override
    public void init() throws RequiredArgumentMissingException {
        if (removeFields == null){
            throw new RequiredArgumentMissingException("You need to specify what fields to remove");
        }
    }

    @Override
    public void process(LocalDocument doc)
            throws ProcessException {
        for (String field : removeFields) {
            if (doc.getContentField(field) != null){
                Logger.debug("removing field "+field);
                doc.putContentField(field, null);                                        
            }
        }
    }

    public void setRemoveFields(List<String> removeFields) {
        this.removeFields = removeFields;
    }
}
