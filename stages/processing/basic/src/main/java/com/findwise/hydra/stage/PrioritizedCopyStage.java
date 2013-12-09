package com.findwise.hydra.stage;

import com.findwise.hydra.local.LocalDocument;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * This stage will copy the first field found in the specifed order
 *
 * The config is as follows:
 *
 * inputFields:[field1,field2,field3] outputField:field0 defaultValue:unknown
 *
 * This will result in:
 *
 * 1. if field0 exists, keep this field 2. if field0 does not exist, copy field1
 * to field0 3. if field1 does not exist, copy field2 to field0 4. ...unt so
 * weiter
 *
 * If no value at all is found the value from defaultvalue is used if set
 * 
 * If the output field already has a value, don't overwrite unless overWrite is true
 *
 * @author Sture Svensson
 * @author Roar Granevang
 */
@Stage(description = "This stage will copy the first field found in the specifed order.")
public class PrioritizedCopyStage extends AbstractProcessStage {
    private Logger logger = LoggerFactory.getLogger(PrioritizedCopyStage.class);

    @Parameter(name = "conditionField", description = "The field where to find the condition")
    private String conditionField = null;
    @Parameter(name = "conditionValue", description = "The condition for the actual copy")
    private String conditionValue = null;
    @Parameter(name = "outputField", required = true, description = "The field to populate")
    private String outputField;
    @Parameter(name = "inputFields", required = true, description = "the prioritized fields to populate from")
    private List<String> inputFields;
    @Parameter(name = "defaultValue", description = "the default value if nothing is found")
    private String defaultValue = null;
    @Parameter(name = "overWrite", description = "if set to true, will overwrite the contents of "
            + "the outputField even if it alrady has a value. Default false")
    private boolean overWrite = false;

    @Override
    public void init() throws RequiredArgumentMissingException {
    }

    @Override
    public void process(LocalDocument doc) throws ProcessException {

        logger.debug("Starting processing");

        if (hasValueAndNotEmptyString(doc, outputField) && !overWrite) {
            logger.debug("Field" + outputField + " already populated, no need to copy");
            return;
        }

        for (String field : inputFields) {
            if (hasValueAndNotEmptyString(doc, field)) {
                if (meetsCondition(doc, field)) {
                    logger.debug("Found value in field: " + field);
                    doc.putContentField(outputField, doc.getContentField(field));
                    return;
                } else {
                    logger.debug("Field" + outputField + " doesn't meet the condition of "+conditionField + "being" + conditionValue);
                }

            }
        }

        if (defaultValue != null) {
            doc.putContentField(outputField, defaultValue);
            logger.debug("no value found, using default: " + defaultValue);
        }
    }

    private boolean hasValueAndNotEmptyString(LocalDocument doc, String field) {
        Object data = doc.getContentField(field);
        if (data == null || data.equals("")) {
            return false;
        } else {
            return true;
        }
    }

    public void setDefaultValue(String defaultValue) {
        this.defaultValue = defaultValue;
    }

    public void setInputFields(List<String> inputFields) {
        this.inputFields = inputFields;
    }

    public void setOutputField(String outputField) {
        this.outputField = outputField;
    }

    public void setConditionField(String conditionField) {
        this.conditionField = conditionField;
    }

    public void setConditionValue(String conditionValue) {
        this.conditionValue = conditionValue;
    }

    public void setOverWrite(boolean overWrite) {
        this.overWrite = overWrite;
    }        
    
    

    private boolean meetsCondition(LocalDocument doc, String field) {
        if (conditionField == null && conditionValue == null){
            return true;
        }
        else if (doc.getContentField(conditionField) == null){
            logger.debug("conditionfield "+conditionField +" is empty....skipping");
            return true;
        }
        else{
            if (doc.getContentField(conditionField).equals(conditionValue)){
                return true;
            }
            else return false;
        }
    }
}
