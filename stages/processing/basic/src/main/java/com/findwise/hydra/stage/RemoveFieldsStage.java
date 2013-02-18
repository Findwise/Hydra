package com.findwise.hydra.stage;

import com.findwise.hydra.Logger;
import com.findwise.hydra.local.LocalDocument;
import java.util.List;

/**
 * A stage that deletes fields matching a list of regular expressions.
 * @author Roar Granevang & Joel Westberg
 */
@Stage(description = "Removes fields specified by regular expressions.")
public class RemoveFieldsStage extends AbstractProcessStage {

    @Parameter(name = "removeFields", description = "List of regular expressions defining what fields to remove from the document")
    private List<String> removeFields;

    @Override
    public void init() throws RequiredArgumentMissingException {
        if (removeFields == null){
            throw new RequiredArgumentMissingException("Required parameter 'removeFields' is missing from configuration");
        }
    }

    @Override
    public void process(LocalDocument doc) throws ProcessException {
    	for(String field : doc.getContentFields()) {
    		for (String regex : removeFields) {
    			if(field.matches(regex)) {
    				Logger.debug("Removing field '"+field+"' matching regular expression '"+regex+"'");
    				doc.removeContentField(field);
    			}
            }
    	}
    }

    public void setRemoveFields(List<String> removeFields) {
        this.removeFields = removeFields;
    }
}
