package com.findwise.utils.tika;

import org.apache.tika.language.LanguageIdentifier;
import org.apache.tika.metadata.Metadata;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

/**
 * Container for data parsed by Tika
 */
public class ParsedData {

    private final String content;

    private final Metadata metadata;

    private final TextSanitizer textSanitizer;

    public ParsedData(String content, Metadata metadata) {
        this.content = content;
        this.metadata = metadata;
        this.textSanitizer = new TextSanitizer();
    }

    /**
     * @return sanitized content from the parsed data
     */
    public String getContent() {
        return textSanitizer.filterInvalidChars(content);
    }

    /**
     * @return a map with metadata fields, with sanitized field values
     */
    public Map<String, Object> getMetadata() {
        Map<String, Object> fields = new HashMap<String, Object>();
        for (String name : metadata.names()) {
            if (metadata.getValues(name).length > 1) {
                String[] metadataValues = metadata.getValues(name);
                fields.put(name, textSanitizer.filterInvalidChars(Arrays.asList(metadataValues)));
            } else {
                fields.put(name, textSanitizer.filterInvalidChars(metadata.get(name)));
            }
        }
        return fields;
    }

    /**
     * @return the identified language of the content
     */
    public String getLanguage() {
        return new LanguageIdentifier(content).getLanguage();
    }
}
