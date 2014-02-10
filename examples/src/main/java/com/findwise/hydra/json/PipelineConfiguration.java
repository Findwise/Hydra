package com.findwise.hydra.json;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * Representation of a pipeline configuration. The corresponding json format
 * could appear something like the following
 * 
 * <pre>
 * {
 *   "pipelineName": "contacts",
 *   "stages": [
 *   {
 *       "stageId": "foo",
 *       "jarId": "solr-writer-0.4.2",
 *       "stageClass": "com.findwise.hydra.stage.SetStaticFieldStage",
 *       "queryOptions": ["touched(extractTitles,true)"],
 *       "fieldNames": ["source"],
 *       "fieldValues": ["web"]
 *   },
 *   ...
 *   }]
 *}
 * </pre>
 * 
 * @author johan.sjoberg
 */
public class PipelineConfiguration implements Iterable<Map<String, Object>> {

    private String pipelineName;
    // A stage is only a Map<String, Object> for ease of parsing
    private List<Map<String, Object>> stages = new ArrayList<Map<String, Object>>();

    public String getPipelineName() {
        return pipelineName;
    }

    public void setPipelineName(String pipelineName) {
        this.pipelineName = pipelineName;
    }

    public List<Map<String, Object>> getStages() {
        return stages;
    }

    public void setStages(List<Map<String, Object>> stages) {
        this.stages = stages;
    }

    public void addStage(Map<String, Object> stage) {
        stages.add(stage);
    }

    @Override
    public Iterator<Map<String, Object>> iterator() {
        return stages.iterator();
    }

    @Override
    public String toString() {
        return "PipelineConfiguration{" + "pipelineName=" + pipelineName + ", stages=" + stages + '}';
    }
}
