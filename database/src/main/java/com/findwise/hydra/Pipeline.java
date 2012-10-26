package com.findwise.hydra;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.findwise.hydra.common.SerializationUtils;

public class Pipeline {
	
	private Map<String, Stage> stages;
	private Map<String, StageGroup> stageGroups;
	
	
	public Pipeline() {
		stages = new HashMap<String, Stage>();
		stageGroups = new HashMap<String, StageGroup>();
	}
	
	public boolean removeStage(Stage s) {
		if(!stages.containsKey(s.getName())) {
			return false;
		}
		removeFromGroup(s);
		return stages.remove(s.getName())!=null;
	}
	
	/**
	 * Creates a new stage in this pipeline and returns it.
	 * 
	 * @return A new stage in this pipeline, or null if an exception occurred
	 */
	public Stage addStage(Stage stage) {
		addToGroup(stage);
		return stages.put(stage.getName(), stage);
	}
	
	public Stage getStage(String stage) {
		return stages.get(stage);
	}
	
	public boolean hasStage(String stage) {
		return stages.containsKey(stage);
	}

	public List<Stage> getStages() {
		List<Stage> stageList = new ArrayList<Stage>();
		
		Iterator<Stage> it = stages.values().iterator();
		while(it.hasNext()) {
			stageList.add(it.next());
		}
		
		return stageList;
	}
	
	protected Map<String, Stage> getStageMap() {
		return stages;
	}
	
	public boolean isEqual(Pipeline p) {
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
	
	public Set<StageGroup> getStageGroups() {
		return new HashSet<StageGroup>(stageGroups.values());
	}
	
	public Set<Stage> getStages(String groupName) {
		return stageGroups.get(groupName);
	}
	
	public boolean hasStageGroup(String groupName) {
		return stageGroups.containsKey(groupName);
	}
	
	public StageGroup getStageGroup(String groupName) {
		return stageGroups.get(groupName);
	}
	
	private void addToGroup(Stage stage) {
		if(!stageGroups.containsKey(stage.getGroupName())) {
			stageGroups.put(stage.getGroupName(), new StageGroup(stage.getGroupName()));
		}
		stageGroups.get(stage.getGroupName()).add(stage);
	}
	
	private void removeFromGroup(Stage stage) {
		if(stageGroups.get(stage.getGroupName()).size()==1) {
			stageGroups.remove(stage.getGroupName());
		} else {
			stageGroups.get(stage.getGroupName()).remove(stage.getName());
		}
	}
	
	public void removeGroup(String group) {
		for(Stage stage : getStages(group)) {
			removeStage(getStage(stage.getName()));
		}
	}

	public void addGroup(StageGroup g) {
		stageGroups.put(g.getName(), g);
		for(Stage s : g) {
			stages.put(s.getName(), s);
		}
	}
}
