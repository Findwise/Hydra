package com.findwise.hydra.stage;

import com.findwise.hydra.Document.Action;
import com.findwise.hydra.local.LocalDocument;

@Stage(description="A stage that sets a static action to all document it processes")
public class SetStaticActionStage extends AbstractProcessStage {

    @Parameter(required=true, description="The action to set to the documents")
    private String action;
    
    @Override
    public void process(LocalDocument doc) throws ProcessException {
        doc.setAction(Action.valueOf(action));
    }

}
