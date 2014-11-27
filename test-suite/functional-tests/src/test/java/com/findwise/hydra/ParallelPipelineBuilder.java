package com.findwise.hydra;

import com.findwise.hydra.mongodb.MongoConnector;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ParallelPipelineBuilder {
	private String stageGroupName;
	private boolean useOneStageGroupPerStage = true;
	private List<Stage> stages = new ArrayList<Stage>();
	private Stage outputStage;

	public ParallelPipelineBuilder useOneStageGroupPerStage(boolean b) {
		this.useOneStageGroupPerStage = b;
		return this;
	}

	public ParallelPipelineBuilder stageGroupName(String stageGroupName) {
		this.stageGroupName = stageGroupName;
		return this;
	}

	public ParallelPipelineBuilder addStages(Stage... stages) {
		this.stages.addAll(Arrays.asList(stages));
		return this;
	}

	public ParallelPipelineBuilder setOutputStage(Stage outputStage) {
		this.outputStage = outputStage;
		return this;
	}

	public void buildAndSave(MongoConnector mongoConnector) throws IOException {
		setStageQueriesToOutputStage(outputStage, stages);
		for (StageGroup stageGroup : getStageGroups()) {
			addStageGroupToMongo(mongoConnector, stageGroup);
		}
	}

	private List<StageGroup> getStageGroups() {
		List<StageGroup> stageGroups = new ArrayList<StageGroup>();
		if (useOneStageGroupPerStage) {
			for (Stage stage : stages) {
				StageGroup stageGroup = new StageGroup(stage.getName());
				stageGroup.addStage(stage);
				stageGroups.add(stageGroup);
			}
			StageGroup stageGroup = new StageGroup(outputStage.getName());
			stageGroup.addStage(outputStage);
			stageGroups.add(stageGroup);
		} else {
			assert (stageGroupName != null);
			StageGroup stageGroup = new StageGroup(stageGroupName);
			for (Stage stage : stages) {
				stageGroup.addStage(stage);
			}
			stageGroup.addStage(outputStage);
			stageGroups.add(stageGroup);
		}
		return stageGroups;
	}

	private void addStageGroupToMongo(MongoConnector mongoConnector, StageGroup stageGroup) throws IOException {
		Pipeline pipeline = mongoConnector.getPipelineReader().getPipeline();
		pipeline.addGroup(stageGroup);
		mongoConnector.getPipelineWriter().write(pipeline);
	}

	private void setStageQueriesToOutputStage(Stage outputStage, Collection<Stage> stages) {
		Map<String, Object> stageProperties = outputStage.getProperties();
		Map<String, Object> touchedParams = new HashMap<String, Object>();

		for (Stage stage : stages) {
			touchedParams.put(stage.getName(), true);
		}

		Map<String, Object> queryParams = new HashMap<String, Object>();
		queryParams.put("touched", touchedParams);
		stageProperties.put("query", queryParams);
	}
}
