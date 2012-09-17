package com.findwise.hydra.json;

import com.findwise.hydra.DatabaseFile;
import com.findwise.hydra.Stage;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.bson.types.ObjectId;

/**
 * Constructs stages from a pipeline configuration
 * 
 * @author johan.sjoberg
 */
public class StageFactory {

    public static final String JAR_ID = "jarId";
    public static final String STAGE_NAME = "stageName";

    public List<Stage> createStages(PipelineConfiguration pipelineConfig) {
        List<Stage> stages = new ArrayList<Stage>();
        for (Map<String, Object> stageConfig : pipelineConfig) {
            String jarId = (String) stageConfig.get(JAR_ID);
            String stageName = (String) stageConfig.get(STAGE_NAME);

            DatabaseFile df = new DatabaseFile();
            df.setId(jarId);
            Stage s = new Stage(stageName, df);
            s.setProperties(stageConfig);
            s.setMode(Stage.Mode.ACTIVE);
            stages.add(s);
        }
        return stages;
    }
}
