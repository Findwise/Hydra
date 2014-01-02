package com.findwise.hydra.stage;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import com.findwise.hydra.local.LocalDocument;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.regex.Pattern;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Converts epoch time and ISO 8601 with different time zones to Zulu time. 
 * Zulu time is ISO 8601 where the time zone is +0. The format has the following
 * pattern YYYY-MM-DDTHH:MM:SS.000Z
 * 
 *
 * Sample configuration:
 * <pre>
 * {@code 
        {
          "stageClass": "com.findwise.hydra.stage.ZuluDateFormatter",
          "query": {
              "touched" : {"remove-blank-fields": true}
          },
          "dateFields":
              ["updated","created"]
        }
 * }
 * </pre>
 * 
 * @author magnus.ebbesson
 * @author leonard.saers
 */
@Stage(description = "A stage that transfroms EPOCH and ISO8601 time to "
        + "Zulu Date format")
public class ZuluDateFormatter extends AbstractProcessStage {

    private Logger logger = LoggerFactory.getLogger(ZuluDateFormatter.class);
    
    private static final String EPOCH_PATTERN = "^[0-9]+$";
    private static final String ISO8601_PATTERN = 
            "^(\\d{4}\\-\\d\\d\\-\\d\\d([tT][\\d:\\.]*)?)"
            + "([zZ]|([+\\-])(\\d\\d):?(\\d\\d))?$";
    Pattern epochPattern;
    Pattern iso8601Pattern;
    
    @Parameter(description = "List<String> - A list of field names that are to "
            + "be considered date fields")
    List<String> dateFields = new ArrayList<String>();

    Set<String> dateFieldsSet;

    @Override
    public void init() {
        dateFieldsSet = new HashSet<String>(dateFields);
        
        epochPattern = Pattern.compile(EPOCH_PATTERN);
        iso8601Pattern = Pattern.compile(ISO8601_PATTERN);
    }

    @Override
    public void process(LocalDocument doc) throws ProcessException {
        Map<String, Object> contentMap = doc.getContentMap();

        for (Entry<String, Object> contentField : contentMap.entrySet()) {
            if (dateFieldsSet.contains(contentField.getKey())) {
                
                if ((contentField.getValue() instanceof String)) {
                                
                    parseDate(contentField, doc);
                }
            }
        }
    }

    private void parseDate
        (Entry<String, Object> contentField, LocalDocument doc) {
        String dateStr = (String) contentField.getValue();
        
        if(epochPattern.matcher(dateStr).matches()){
            dateStr = fromEpocToZuluDate((String) contentField.getValue());
        } else if(iso8601Pattern.matcher(dateStr).matches()){
            dateStr = fromISO8601ToZuluDate((String) contentField.getValue());
        } else {
            logger.info("Faild to parse date: " + dateStr + 
                    " to Zulu date format");
        }
        
        doc.putContentField(contentField.getKey(), dateStr);
    }

    private String fromISO8601ToZuluDate(String date) {
        DateTimeFormatter formatter = 
                DateTimeFormat.forPattern("yyyy-MM-dd'T'HH:mm:ssZ");
        DateTime d = formatter.parseDateTime(date).withZone(DateTimeZone.UTC);
        return d.toString(DateTimeFormat.forPattern("yyyy-MM-dd'T'HH:mm:ss'Z"));
    }

    private String fromEpocToZuluDate(String epoc) {
        SimpleDateFormat formatter = 
                new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ssZ");
        long timestampInSec = Long.parseLong(epoc) * 1000;
        String formatedWithTimezoneInfo = 
                formatter.format(new Date(timestampInSec));
        String[] atTimeZone = formatedWithTimezoneInfo.split("\\+");
        return atTimeZone[0] + "Z";
    }
}
