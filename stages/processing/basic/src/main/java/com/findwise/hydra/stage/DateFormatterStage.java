package com.findwise.hydra.stage;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import com.findwise.hydra.local.LocalDocument;
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
 * At the moment, this stage only transforms date to zulu date. The plan is
 * to extend functionality to transform from one given format to another given 
 * format. See issue https://github.com/Findwise/Hydra/pull/296
 * 
 *
 * Sample configuration:
 * <pre>
 * {@code 
        {
          "stageClass": "com.findwise.hydra.stage.DateFormatterStage",
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
public class DateFormatterStage extends AbstractProcessStage {

    private static final String ZULU_DATE_FORMAT = "yyyy-MM-dd'T'HH:mm:ss'Z";
    private Logger logger = LoggerFactory.getLogger(DateFormatterStage.class);

    private static final String EPOCH_PATTERN = "^[0-9]+$";
    private static final String ISO8601_DATE_FORMAT = "yyyy-MM-dd'T'HH:mm:ssZ";
    private static final String ISO8601_PATTERN =
            "^(\\d{4}\\-\\d\\d\\-\\d\\d([tT][\\d:\\.]*)?)"
            + "([zZ]|([+\\-])(\\d\\d):?(\\d\\d))?$";
    Pattern epochPattern;
    Pattern iso8601Pattern;
    
    @Parameter(description = "List<String> - A list of field names that are to "
            + "be considered date fields")
    List<String> dateFields = new ArrayList<String>();

    @Parameter(description = "Time zone to use for epoch time date fields. Default: UTC")
    String epochTimeZone = "UTC";

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

    private void parseDate(Entry<String, Object> contentField, LocalDocument doc) {
        String dateStr = (String) contentField.getValue();
        DateTimeFormatter zuluFormat = DateTimeFormat.forPattern(ZULU_DATE_FORMAT);
        if(epochPattern.matcher(dateStr).matches()){
            dateStr = fromEpochToZuluDate((String) contentField.getValue()).toString(zuluFormat);
        } else if(iso8601Pattern.matcher(dateStr).matches()){
            dateStr = fromISO8601ToZuluDate((String) contentField.getValue()).toString(zuluFormat);
        } else {
            logger.info("Failed to parse date: " + dateStr);
        }
        doc.putContentField(contentField.getKey(), dateStr);
    }

    private DateTime fromISO8601ToZuluDate(String date) {
        return DateTimeFormat.forPattern(ISO8601_DATE_FORMAT)
            .parseDateTime(date)
            .withZone(DateTimeZone.UTC);
    }

    private DateTime fromEpochToZuluDate(String epoch) {
        long epochInMilliSeconds = Long.parseLong(epoch) * 1000;
        return new DateTime(epochInMilliSeconds)
            .withZone(DateTimeZone.forID(epochTimeZone));
    }
}
