package com.findwise.hydra.stage;

import com.findwise.hydra.local.LocalDocument;
import com.findwise.hydra.stage.AbstractProcessStage;
import com.findwise.hydra.stage.Parameter;
import com.findwise.hydra.stage.ProcessException;
import com.findwise.hydra.stage.RequiredArgumentMissingException;
import com.findwise.hydra.stage.Stage;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/**
 * This stage look for the collection fields and create a gard_qc and/or
 * bridge_qc according to what it finds. Then it will copy the contents of
 * copyFields into the new fieldnames it has created
 *
 * The config is as follows:
 *
 * collectionName:collection copyFields:[title]
 * queryCompleteionPrefix:qc_ (default qc_)
 *
 * This will result in:
 *
 * collection gard and collection bridge and title Battleships will result in
 * qc_gard and qc_bridge being created and the title will be copied to both
 * fields.
 * 
 * You can also add static mappings as:
 * staticMappingList: ["aa:bb","cc:dd"]
 *
 * @author Roar Granevang
 * @author Sture Svensson
 */
@Stage(description = "This stage look for the collection fields and create a qc_gard and/or qc_bridge according to what it finds.")
public class PopulateQueryCompletionFields extends AbstractProcessStage {

    @Parameter(name = "collectionName", description = "Where to look for the fieldnames")
    private String collectionName;
    @Parameter(name = "staticMappingList", description = "Static mappings based on fieldvalue. This is to be used if AAA is stored "
            + "in the collection, but you want the queryCompletionField to be news_qc instead of AAA_qc")
    private List<String> staticMappingList;
    @Parameter(name = "copyFields", description = "What field-contents to copy into the generated fields")
    private List<String> copyFields;
    @Parameter(name = "queryCompletionPrefix", description = "The prefix to add to the generated fields")
    private String queryCompletionPostfix = "_qc";
    
    private Map<String,String> staticMapping;

    @Override
    public void init() throws RequiredArgumentMissingException {
        if (collectionName == null) {
            throw new RequiredArgumentMissingException("collectionName missing. Need to know where to look for collection names");
        }
        if (copyFields == null) {
            throw new RequiredArgumentMissingException("copyFields missing. Need to know what values to copy into the newly created fields");
        }

        if(staticMappingList != null){
            staticMapping = new HashMap<String, String>();
            for (String s: staticMappingList){
                String[] parts = s.split(":");
                staticMapping.put(parts[0], parts[1]);  
            }
        }
    }

    @Override
    public void process(LocalDocument doc) throws ProcessException {

        Object cf = doc.getContentField(collectionName);
        if (cf instanceof List) {
            List<String> contentFields = (List<String>) cf;

            for (String f : contentFields) {
                if (staticMapping != null) {
                    System.out.println(staticMapping);
                    System.out.println(f);
                    if (staticMapping.containsKey(f)) {
                        createAndPopulateQCField(staticMapping.get(f), doc);
                    }
                } else {
                    createAndPopulateQCField(f, doc);
                }
            }
        } else if (cf instanceof String) {
            if (staticMapping != null) {
                if (staticMapping.containsKey((String)cf)) {
                    createAndPopulateQCField(staticMapping.get((String)cf), doc);
                }
            } else {
                createAndPopulateQCField((String) cf, doc);
            }
        }
    }

    public void setCollectionName(String collectionName) {
        this.collectionName = collectionName;
    }

    public void setCopyFields(List<String> copyFields) {
        this.copyFields = copyFields;
    }

    public void setQueryCompletionPostfix(String queryCompletionPostfix) {
        this.queryCompletionPostfix = queryCompletionPostfix;
    }

    public void setStaticMappingList(List<String> staticMappingList) {
        this.staticMappingList = staticMappingList;
    }


    
    

    /**
     * Goes through the list of copyFields and creates one new field_qc with the
     * fieldName or a linked list if there are many. gard_qc = [Battleship, Flagship]
     *
     * @param fieldName
     * @param doc
     */
    private void createAndPopulateQCField(String fieldName, LocalDocument doc) {
        //if only one copyField, store it as a string, otherwise as a linked list
        if (copyFields.size() == 1) {
            doc.putContentField(fieldName + queryCompletionPostfix, doc.getContentField(copyFields.get(0)));
        } else {
            List<String> list = new LinkedList<String>();
            for (String copyField : copyFields) {

                list.add((String) doc.getContentField(copyField));
            }
            doc.putContentField(fieldName + queryCompletionPostfix, list);
        }
    }
}
