package com.findwise.hydra;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import com.findwise.hydra.common.SerializationUtils;

public class Pipeline<T extends Stage> {
	
	private Map<String, T> stages;
	
	public Pipeline() {
		stages = new HashMap<String, T>();
	}
	
	public boolean removeStage(T s) {
		if(!stages.containsKey(s.getName())) {
			return false;
		}
		return stages.remove(s.getName())!=null;
	}
	
	/**
	 * Creates a new stage in this pipeline and returns it.
	 * 
	 * @return A new stage in this pipeline, or null if an exception occurred
	 */
	public T addStage(T stage) {
		return stages.put(stage.getName(), stage);
	}
	
	public Stage getStage(String stage) {
		return stages.get(stage);
	}
	
	public boolean hasStage(String stage) {
		return stages.containsKey(stage);
	}

	public List<T> getStages() {
		List<T> stageList = new ArrayList<T>();
		
		Iterator<T> it = stages.values().iterator();
		while(it.hasNext()) {
			stageList.add(it.next());
		}
		
		return stageList;
	}
	
	protected Map<String, T> getStageMap() {
		return stages;
	}
	
	public boolean isEqual(Pipeline<? extends Stage> p) {
		if(p.getStages().size()==getStages().size()) {
			for(Stage s : p.getStages()) {
				if(!hasStage(s.getName()) || !s.isEqual(getStage(s.getName()))) {
					return false;
				}
			}
			return true;
		}
		return false;
	}
	
	@Override
	public String toString() {
		return SerializationUtils.toJson(getStages());
	}
}
