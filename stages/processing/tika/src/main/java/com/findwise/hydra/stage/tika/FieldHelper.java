package com.findwise.hydra.stage.tika;

import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class FieldHelper {
    public static Map<String, Object> getFieldMatchingPattern(Map<String, Object> fields,
                                                              String pattern) {
        Map<String, Object> fieldToUrl = new HashMap<String, Object>();

        for (String field : fields.keySet()) {
            Pattern p = Pattern.compile(pattern);
            Matcher m = p.matcher(field);
            if (m.matches()) {
                String toField;
                if (m.groupCount() >= 1) {
                    toField = m.group(1);
                } else {
                    toField = m.group();
                }
                fieldToUrl.put(toField, fields.get(field));
            }
        }
        return fieldToUrl;
    }
}
