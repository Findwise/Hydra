package com.findwise.hydra.admin.stages;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.bson.types.ObjectId;

import com.findwise.hydra.DatabaseConnector;
import com.findwise.hydra.DatabaseFile;
import com.findwise.hydra.DatabaseType;
import com.findwise.hydra.Pipeline;
import com.findwise.hydra.Stage;
import com.findwise.hydra.StageGroup;
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
		ret.put("stages", new ArrayList<Stage>(connector.getPipelineReader().getPipeline().getStages()));
		return ret;
	}
	
	public void addStage(Stage stage) throws IOException {
		addStage(stage, null);
	}
	
	public void addStage(Stage stage, String groupName) throws IOException {

		Pipeline pipeline = connector.getPipelineReader().getPipeline();
		if(groupName == null) {
			groupName = stage.getName();
		}
		if(!pipeline.hasGroup(groupName)) {
			pipeline.addGroup(new StageGroup(groupName));
		}
		pipeline.getGroup(groupName).addStage(stage);
		connector.getPipelineWriter().write(pipeline);
	}
	
	private DatabaseFile toDatabaseFile(String libraryId) {
		DatabaseFile df = new DatabaseFile();
		try {
			df.setId(new ObjectId(libraryId));
		} catch (Exception e) {
			df.setId(libraryId);
		}
		return df;
	}

	/**
	 * @param groupName may be <pre>null</pre> to indicate the same group name as the name of the stage
	 */
	public Map<String, Object> addStage(String libraryId, String groupName, String name,
			String jsonConfig) throws JsonException, IOException {
		Map<String, Object> ret = new HashMap<String, Object>();
		addStage(libraryId, groupName, name, jsonConfig, false);
		ret.put("stageStatus", "Added");
		return ret;
	}

	/**
	 * @param groupName may be <pre>null</pre> to indicate the same group name as the name of the stage
	 */
	public void addStage(String libraryId, String groupName, String name, String jsonConfig, boolean debug) throws JsonException, IOException {

		Stage s = new Stage(name, toDatabaseFile(libraryId));
		s.setProperties(SerializationUtils.fromJson(jsonConfig));
		if (debug) {
			s.setMode(Stage.Mode.DEBUG);
		} else {
			s.setMode(Stage.Mode.ACTIVE);
		}

		addStage(s, groupName);
	}

	public Stage getStageInfo(String stageName) {
		return connector.getPipelineReader().getPipeline().getStage(stageName);
	}

}
