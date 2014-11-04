package com.findwise.hydra.stage;

import java.util.Arrays;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.findwise.hydra.local.IncorrectFieldTypeException;
import com.findwise.hydra.local.LocalDocument;

@Stage
public class SplitStage extends AbstractProcessStage {

    private Logger logger = LoggerFactory.getLogger(SplitStage.class);

    @Parameter(required = true, name = "inField", description = "Field to split")
    private String inField;

    @Parameter(required = true, name = "outField", description = "Field to store the output in")
    private String outField;

    @Parameter(required = true, name = "splitRegex", description = "The regex to split on")
    private String splitRegex;

    @Override
    public void process(LocalDocument doc) throws ProcessException {
        try {
            String content = doc.getContentFieldAsString(inField);
            String[] parts = content.split(splitRegex);
            doc.putContentField(outField, Arrays.asList(parts));
        } catch (IncorrectFieldTypeException e) {
            logger.error("Expected String in " + inField, e);
            throw new ProcessException("Expected String in " + inField, e);
        }
    }

}
