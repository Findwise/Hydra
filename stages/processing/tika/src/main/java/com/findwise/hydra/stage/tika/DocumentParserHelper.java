package com.findwise.hydra.stage.tika;

import com.findwise.hydra.local.LocalDocument;
import com.findwise.utils.tika.ParsedData;

import java.util.Map;

public class DocumentParserHelper {

    private final boolean addMetaData;

    private final boolean addLanguage;

    public DocumentParserHelper(boolean addMetaData, boolean addLanguage) {
        this.addMetaData = addMetaData;
        this.addLanguage = addLanguage;
    }

    /**
     * Adds the parsed data as fields to the document, using a prefix for the field names
     *
     * @param parsedData results of parsing something
     * @param doc the document to modify
     * @param prefix field name prefix
     */
    public void addParsedDataToDocument(ParsedData parsedData, LocalDocument doc, String prefix) {
        doc.putContentField(prefix + "content", parsedData.getContent());

        if (addMetaData) {
            Map<String, Object> metadata = parsedData.getMetadata();
            for (String metadataField : metadata.keySet()) {
                doc.putContentField(prefix + metadataField, metadata.get(metadataField));
            }
        }
        if (addLanguage) {
            doc.putContentField(prefix + "language", parsedData.getLanguage());
        }
    }
}
