package com.findwise.hydra.stage;

import com.findwise.hydra.common.Logger;
import com.findwise.hydra.local.LocalDocument;
import java.util.ArrayList;
import java.util.List;
import org.springframework.web.util.HtmlUtils;

/**
 * 
 * @author Sture Svensson
 */
@Stage(description = "This stage will unescape html characters")
public class HtmlUnescaperStage extends AbstractProcessStage {

    @Parameter(name = "outputField", description = "The field to populate")
    private String output_suffix = "";
    @Parameter(name = "inputFields", description = "the prioritized fields to populate from")
    private List<String> inputFields;

    @Override
    public void init() throws RequiredArgumentMissingException {
    }

    @Override
    public void process(LocalDocument doc) throws ProcessException {

        Logger.debug("Starting processing");

        for (String field : inputFields) {

            String out = field.concat(output_suffix);
            Object value = doc.getContentField(field);
            if (value != null) {
                if (value instanceof String) {
                    doc.putContentField(out, HtmlUtils.htmlUnescape((String) value));
                } else if (value instanceof List<?>) {
                    List<String> newList = new ArrayList<String>();
                    for (String v : (List<String>) value) {
                        newList.add(HtmlUtils.htmlUnescape(v));
                    }
                    doc.putContentField(out, newList);
                }
            }
        }
    }

    public void setInputFields(List<String> inputFields) {
        this.inputFields = inputFields;
    }

    public void setOutput_suffix(String output_suffix) {
        this.output_suffix = output_suffix;
    }
    
    
}
