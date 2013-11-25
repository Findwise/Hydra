package com.findwise.hydra.admin.stages;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.google.gson.JsonParseException;
import org.bson.types.ObjectId;

import com.findwise.hydra.DatabaseConnector;
import com.findwise.hydra.DatabaseFile;
import com.findwise.hydra.DatabaseType;
import com.findwise.hydra.Pipeline;
import com.findwise.hydra.Stage;
import com.findwise.hydra.StageGroup;
import com.findwise.hydra.JsonException;
import com.findwise.hydra.SerializationUtils;

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

	public Map<String, List<StageGroup>> getStageGroups() {
		Map<String, List<StageGroup>> ret = new HashMap<String, List<StageGroup>>();
		ret.put("stagegroups", new ArrayList<StageGroup>(connector.getPipelineReader().getPipeline().getStageGroups()));
		return ret;
	}

	public StageGroup getStageGroup(String groupName) {
		return connector.getPipelineReader().getPipeline().getGroup(groupName);
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
		Map<String, Object> config = SerializationUtils.fromJson(jsonConfig);
		if (null == config) {
			throw new JsonException(new JsonParseException("Configuration was empty"));
		} else if (!config.containsKey("stageClass")) {
			throw new JsonException(new JsonParseException("Required configuration parameter 'stageClass' missing"));
		}
		s.setProperties(config);
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

	public Map<String, Object> deleteStage(String stageName) throws IOException {
		return deleteStage(stageName, null);
	}
	
	public Map<String, Object> deleteStage(String stageName, String groupName) throws IOException {
		Stage stageToDelete = getStageInfo(stageName);
		Map<String, Object> ret = new HashMap<String, Object>();
		if (stageToDelete == null) {
			ret.put("stageStatus", "Could not find stage " + stageName);
		} else {
			Pipeline pipeline = connector.getPipelineReader().getPipeline();
			if(groupName == null) {
				groupName = getStageGroupForStage(stageToDelete);
			}
			if(!pipeline.hasGroup(groupName)) {
				pipeline.addGroup(new StageGroup(groupName));
			}
			pipeline.getGroup(groupName).removeStage(stageName);
			connector.getPipelineWriter().write(pipeline);
		
			ret.put("stageStatus", "Deleted stage " + stageName);
		}
		return ret;
	}

	private String getStageGroupForStage(Stage stageToDelete) {
		StageGroup groupForStage = connector.getPipelineReader().getPipeline().getGroupForStage(stageToDelete.getName());
		if (groupForStage != null) {
			return groupForStage.getName();
		} else {
			return stageToDelete.getName();
		}
	}
}
