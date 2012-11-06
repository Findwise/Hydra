package com.findwise.hydra.admin.stages;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.bson.types.ObjectId;

import com.findwise.hydra.DatabaseConnector;
import com.findwise.hydra.DatabaseFile;
import com.findwise.hydra.DatabaseType;
import com.findwise.hydra.Pipeline;
import com.findwise.hydra.Stage;
import com.findwise.hydra.common.JsonException;
import com.findwise.hydra.common.SerializationUtils;

public class StagesService<T extends DatabaseType> {

	private DatabaseConnector<T> connector;

	public StagesService(DatabaseConnector<T> connector) {
		this.connector = connector;
	}

	public DatabaseConnector<T> getConnector() {
		return connector;
	}
	
	public Map<String, List<Stage>> getStages() {
		Map<String, List<Stage>> ret = new HashMap<String, List<Stage>>();
		ret.put("stages", connector.getPipelineReader().getPipeline().getStages());
		return ret;
	}

	public Map<String, Object> addStage(String libraryId, String name,
			String jsonConfig) throws JsonException, IOException {
		Map<String, Object> ret = new HashMap<String, Object>();
		addStage(libraryId, name, jsonConfig, false);
		ret.put("stageStatus", "Added");
		return ret;
	}

	public void addStage(String libraryId, String name, String jsonConfig,
			boolean debug) throws JsonException, IOException {
		DatabaseFile df = new DatabaseFile();
		try {
			df.setId(new ObjectId(libraryId));
		} catch (Exception e) {
			df.setId(libraryId);
		}
		Stage s = new Stage(name, df);
		s.setProperties(SerializationUtils.fromJson(jsonConfig));
		if (debug) {
			s.setMode(Stage.Mode.DEBUG);
		} else {
			s.setMode(Stage.Mode.ACTIVE);
		}

		Pipeline<Stage> pipeline = connector.getPipelineReader().getPipeline();
		pipeline.addStage(s);
		connector.getPipelineWriter().write(pipeline);

	}

	public Stage getStageInfo(String stageName) {
		return connector.getPipelineReader().getPipeline().getStage(stageName);
	}

}
