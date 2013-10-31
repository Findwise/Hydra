package com.findwise.hydra;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class Pipeline {
	private Map<String, StageGroup> stageGroups;
	
	public Pipeline() {
		stageGroups = new HashMap<String, StageGroup>();
	}
	
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result
				+ ((stageGroups == null) ? 0 : stageGroups.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj) {
			return true;
		}
		if (obj == null || getClass() != obj.getClass()) {
			return false;
		}
		
		Pipeline other = (Pipeline) obj;
		if(other.getStageGroups().size()==getStageGroups().size()) {
			for(StageGroup g : other.getStageGroups()) {
				if(!hasGroup(g.getName()) || !g.equals(getGroup(g.getName()))) {
					return false;
				}
			}
			return true;
		}
		return false;
	}
	
	@Override
	public String toString() {
		return SerializationUtils.toJson(stageGroups.values());
	}
	
	public Set<StageGroup> getStageGroups() {
		return new HashSet<StageGroup>(stageGroups.values());
	}
	
	public boolean hasGroup(String groupName) {
		return stageGroups.containsKey(groupName);
	}
	
	public StageGroup getGroup(String groupName) {
		return stageGroups.get(groupName);
	}
	
	public void removeGroup(String group) {
		stageGroups.remove(group);
	}

	public void addGroup(StageGroup g) {
		stageGroups.put(g.getName(), g);
	}
	
	public Set<Stage> getStages() {
		HashSet<Stage> stages = new HashSet<Stage>();
		for(StageGroup g : getStageGroups()) {
			stages.addAll(g.getStages());
		}
		return stages;
	}
	
	public Stage getStage(String name) {
		for(Stage s : getStages()) {
			if(s.getName().equals(name)) {
				return s;
			}
		}
		return null;
	}
	
	public StageGroup getGroupForStage(String name) {
		for (StageGroup g : getStageGroups()) {
			if (g.hasStage(name)) {
				return g;
			}
		}
		return null;
	}
	
	public boolean hasStage(String name) {
		return getStage(name) != null;
	}
}