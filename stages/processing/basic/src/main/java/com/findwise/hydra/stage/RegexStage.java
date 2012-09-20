package com.findwise.hydra.stage;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.findwise.hydra.common.Logger;
import com.findwise.hydra.local.LocalDocument;

/**
 * Configuration string in JSON format:
 * [{inField:rawcontent,outField:out,regex:\
 * "(^[A-Za-z0-9]*.[A-Za-z0-9]*).*\",substitute:\"$1.ru\"}]
 *
 * It can also contain multiple configurations:
 * [{inField:fieldname,outField:fieldname
 * ,regex:regularexpression,substitute:$x},
 * {inField:fieldname,outField:fieldname,
 * regex:regularexpression,substitute:$x}]:
 *
 * inField: The field the regex should be performed on outField: The field the
 * result from the regex stage should output to regex: The regular expression,
 * backslashes needs to be double escaped substitute: The group being replaced
 * where the group number is prefixed with $
 *
 * @author jens.bengtsson
 * @author leonard.saers
 * @author kristoffer.vinell
 *
 */
@Stage(description = "Regular expression matching on a field, where a substitute regular expression is put in the output field. Backslashes need to be double escaped!")
public class RegexStage extends AbstractProcessStage {

    @Parameter(name = "regexConfigs", description = "List of configs, where each config is a map with the keys 'regex', 'inField' and 'outField' and 'substitute'")
    private List<Map<String, String>> regexConfigs;

    @Override
    public void init() throws RequiredArgumentMissingException {

        if (this.regexConfigs == null || this.regexConfigs.size() == 0) {
            throw new RequiredArgumentMissingException(
                    "regexConf was probably not parsed correctly");
        }

    }

    @Override
    public void process(LocalDocument doc) throws ProcessException {
        for (Map<String, String> regexConf : regexConfigs) {
            Pattern pattern = Pattern.compile(regexConf.get("regex"),
                    Pattern.DOTALL);

            Object value = doc.getContentField(regexConf.get("inField"));

            if (value == null) {
                return;
            }

            if (value instanceof String) {

                String stringValue = (String) doc.getContentField(regexConf.get("inField"));

                Matcher regexMatcher = pattern.matcher(stringValue);

                if (regexMatcher.find()) {
                    doc.putContentField(regexConf.get("outField"),
                            parseString(doc, regexConf, regexMatcher));
                }
            } else if (value instanceof List<?>) {
                List<String> outData = new ArrayList<String>();
                for (Object listValue : (List<?>) value) {
                    if (listValue instanceof String) {
                        Matcher regexMatcher = pattern.matcher((String) listValue);

                        if (regexMatcher.find()) {
                            outData.add(parseString(doc, regexConf, regexMatcher));
                        }
                    } else {
                        Logger.warn("List did not contain all Strings");
                    }
                }
                doc.putContentField(regexConf.get("outField"), outData);
            } else {
                Logger.warn("Field type of inField was not recognized. Valid field types are String and List<String>");
            }
        }
    }

    public List<Map<String, String>> getRegexConfigsList() {
        return regexConfigs;
    }

    public void setRegexConfigsList(List<Map<String, String>> regexConfigsList) {
        this.regexConfigs = regexConfigsList;
    }

    private String parseString(LocalDocument doc,
            Map<String, String> regexConf, Matcher regexMatcher) {
        String substitute = regexConf.get("substitute");
        
        //This will only work with either many groups but only one next find
        //or with one group and many next finds.
        for (int i = 1; i <= regexMatcher.groupCount(); i++) {
            substitute = substitute.replace("$" + i, regexMatcher.group(i));
            if (regexMatcher.groupCount() == 1) {
                while (regexMatcher.find()) {
                    substitute += regexMatcher.group(i);
                }
            }
        }
        return substitute;
    }
}
