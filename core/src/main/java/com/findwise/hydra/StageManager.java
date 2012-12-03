package com.findwise.hydra;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

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
	
	public StageRunner getRunner(String groupName) {
		if(runnerMap.containsKey(groupName)) {
			return runnerMap.get(groupName);
		}
		return null;
	}
	
	public boolean hasRunner(String groupName) {
		return getRunner(groupName)!=null;
	}
	
	public StageRunner getRunnerForStage(String stageName) {
		for(StageRunner runner : runnerMap.values()) {
			if(runner.getStageGroup().hasStage(stageName)) {
				return runner;
			}
		}
		return null;
	}
	
	public boolean hasRunnerForStage(String stage) {
		return getRunnerForStage(stage)!=null;
	}
	
	public void addRunner(StageRunner runner) {
		runnerMap.put(runner.getStageGroup().getName(), runner);
	}
	
	public StageRunner removeRunner(String groupName) {
		if(runnerMap.containsKey(groupName)) {
			StageRunner ret = runnerMap.get(groupName);
			runnerMap.remove(groupName);
			return ret;
		}
		return null;
	}
	
	public void findAndDestroy(String groupName) {
		StageRunner sw = removeRunner(groupName);
		if(sw!=null) {
			sw.destroy();
		}
	}
	
	public Set<StageRunner> getRunners() {
		return new HashSet<StageRunner>(runnerMap.values());
	}
}
