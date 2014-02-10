package com.findwise.hydra;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.findwise.hydra.mongodb.MongoConnector;

class LinearPipelineBuilder {
	private boolean useOneStageGroupPerStage = true;
	private List<Stage> stages = new ArrayList<Stage>();
	private String stageGroupName;

	public LinearPipelineBuilder useOneStageGroupPerStage(boolean b) {
		this.useOneStageGroupPerStage = b;
		return this;
	}

	public LinearPipelineBuilder stageGroupName(String stageGroupName) {
		this.stageGroupName = stageGroupName;
		return this;
	}

	public LinearPipelineBuilder addStages(Stage... stages) {
		this.stages.addAll(Arrays.asList(stages));
		return this;
	}

	public void buildAndSave(MongoConnector mongoConnector) throws IOException {
		setStageQueries();
		for (StageGroup stageGroup : getStageGroups()) {
			addStageGroupToMongo(mongoConnector, stageGroup);
		}
	}

	private List<StageGroup> getStageGroups() {
		List<StageGroup> stageGroups = new ArrayList<StageGroup>();
		if(useOneStageGroupPerStage) {
			for (Stage stage : stages) {
				StageGroup stageGroup = new StageGroup(stage.getName());
				stageGroup.addStage(stage);
				stageGroups.add(stageGroup);
			}
		} else {
			assert(stageGroupName != null);
			StageGroup stageGroup = new StageGroup(stageGroupName);
			for (Stage stage : stages) {
				stageGroup.addStage(stage);
			}
			stageGroups.add(stageGroup);
		}
		return stageGroups;
	}

	private void addStageGroupToMongo(MongoConnector mongoConnector, StageGroup stageGroup) throws IOException {
		Pipeline pipeline = mongoConnector.getPipelineReader().getPipeline();
		pipeline.addGroup(stageGroup);
		mongoConnector.getPipelineWriter().write(pipeline);
	}

	private void setStageQueries() {
		Stage previousStage = null;
		for (Stage stage : stages) {
			if(previousStage != null) {
				setQuery(previousStage, stage);
			}
			previousStage = stage;
		}
	}

	private void setQuery(Stage previousStage, Stage stage) {
		Map<String, Object> stageProperties = stage.getProperties();
		Map<String, Object> touchedParams = new HashMap<String, Object>();
		touchedParams.put(previousStage.getName(), true);
		Map<String, Object> queryParams = new HashMap<String, Object>();
		queryParams.put("touched", touchedParams);
		stageProperties.put("query", queryParams);
	}
}
