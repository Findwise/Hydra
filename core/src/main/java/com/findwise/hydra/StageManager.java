package com.findwise.hydra;

import java.util.HashMap;
import java.util.Map;

public final class StageManager {
	
	private static volatile StageManager self = null;

	private Map<String, StageRunner> runnerMap;
	
	private StageManager() {
		runnerMap = new HashMap<String, StageRunner>();
	}
	
	public static StageManager getStageManager() {
		if(self==null) {
			self = new StageManager();
		}
		return self;
	}
	
	public StageRunner getRunner(String stageName) {
		if(runnerMap.containsKey(stageName)) {
			return runnerMap.get(stageName);
		}
		return null;
	}
	
	
	public boolean hasRunner(String stageName) {
		return getRunner(stageName)!=null;
	}
	
	public void addRunner(StoredStage stage, StageRunner stageWrapper) {
		runnerMap.put(stage.getName(), stageWrapper);
	}
	
	public StageRunner removeRunner(String stageName) {
		if(runnerMap.containsKey(stageName)) {
			StageRunner ret = runnerMap.get(stageName);
			runnerMap.remove(stageName);
			return ret;
		}
		return null;
	}
	
	public void findAndDestroy(String stageName) {
		StageRunner sw = removeRunner(stageName);
		if(sw!=null) {
			sw.destroy();
		}
	}
}
