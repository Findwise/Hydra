package com.findwise.hydra;

import java.util.HashMap;
import java.util.Map;

public final class StageManager {
	
	private static volatile StageManager self = null;

	private Map<String, StageRunner> wrapperMap;
	
	private StageManager() {
		wrapperMap = new HashMap<String, StageRunner>();
	}
	
	public static StageManager getStageManager() {
		if(self==null) {
			self = new StageManager();
		}
		return self;
	}
	
	public StageRunner getWrapper(StoredStage stage) {
		if(wrapperMap.containsKey(stage.getName())) {
			return wrapperMap.get(stage.getName());
		}
		return null;
	}
	
	
	public boolean wrapperExists(StoredStage stage) {
		return getWrapper(stage)!=null;
	}
	
	public void addWrapper(StoredStage stage, StageRunner stageWrapper) {
		wrapperMap.put(stage.getName(), stageWrapper);
	}
	
	public StageRunner removeWrapper(StoredStage stage) {
		if(wrapperMap.containsKey(stage.getName())) {
			StageRunner ret = wrapperMap.get(stage.getName());
			wrapperMap.remove(stage.getName());
			return ret;
		}
		return null;
	}
	
	public void findAndDestroy(StoredStage stage) {
		StageRunner sw = removeWrapper(stage);
		if(sw!=null) {
			sw.destroy();
		}
	}
}
