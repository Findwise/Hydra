package com.findwise.hydra;

import java.util.HashMap;
import java.util.Map;

public class StageBuilder {
	private String stageName;
	private String className;
	private String libraryId;
	private Map<String, Object> stageProperties = new HashMap<String, Object>();

	public StageBuilder stageName(String stageName) {
		this.stageName = stageName;
		return this;
	}

	public StageBuilder className(String className) {
		this.className = className;
		return this;
	}

	public StageBuilder libraryId(String libraryId) {
		this.libraryId = libraryId;
		return this;
	}

	public StageBuilder stageProperties(Map<String, Object> stageProperties) {
		this.stageProperties = stageProperties;
		return this;
	}

	public Stage build() {
		assert(stageName != null);
		assert(className != null);
		assert(libraryId != null);

		DatabaseFile df = new DatabaseFile();
		df.setId(libraryId);
		Stage stage = new Stage(stageName, df);
		stageProperties.put("stageClass", className);
		stage.setProperties(stageProperties);
		return stage;
	}
}
