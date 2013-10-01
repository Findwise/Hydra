package com.findwise.hydra;

import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.Collections;

import org.slf4j.LoggerFactory;
import org.slf4j.Logger;

/**
 * Represents a set of stages that will be run inside the same JVM.
 * 
 * @author joel.westberg
 */
public class StageGroup {	
	public static final String JVM_PARAMETERS_KEY = "jvm_parameters";
	public static final String RETRIES_KEY = "retries";
	public static final String LOGGING_KEY = "logging";
	public static final String CMDLINE_ARGS_KEY = "cmdline_args";
	public static final String CLASSPATH_KEY = "classpath";
	public static final String JAVA_LOCATION_KEY = "java_location";
	
	private final Map<String, Stage> stages;
	
	private String jvmParameters;
	private String classpath;
	private int retries = -1;
	private String cmdlineArgs;
	private boolean logging = false;
	private boolean changedProperties = false;
	private Date propertiesModifiedDate;
	private String name;
	private String javaLocation;
	
	private static final Logger logger = LoggerFactory.getLogger(StageGroup.class);
	
	public StageGroup(String name) {
		stages = new HashMap<String, Stage>();
		this.name = name;
	}
	
	public StageGroup(String name, Map<String, Object> propertiesMap) {
		this(name);
		setProperties(propertiesMap);
	}
	
	public String getName() {
		return name;
	}
	
	public void setName(String name) {
		this.name = name;
	}
	
	public String getJvmParameters() {
		return jvmParameters;
	}
	
	public void setJvmParameters(String jvmParameters) {
		this.jvmParameters = jvmParameters;
	}
	
	public int getRetries() {
		return retries;
	}
	
	public void setRetries(int retries) {
		this.retries = retries;
	}
	
	public String getCmdlineArgs() {
		return cmdlineArgs;
	}
	
	public void setCmdlineArgs(String cmdlineArgs) {
		this.cmdlineArgs = cmdlineArgs;
	}
	
	public boolean isLogging() {
		return logging;
	}
	
	public void setLogging(boolean logging) {
		this.logging = logging;
	}

	public boolean isPropertiesChanged() {
		return changedProperties;
	}
	
	public void setClasspath(String classpath) {
		this.classpath = classpath;
	}
	
	public String getClasspath() {
		return classpath;
	}
	
	public void setJavaLocation(String location) {
		this.javaLocation = location;
	}
	
	public String getJavaLocation() {
		return javaLocation;
	}
	
	public Map<String, Object> toPropertiesMap() {
		HashMap<String, Object> map = new HashMap<String, Object>();
		map.put(JVM_PARAMETERS_KEY, getJvmParameters());
		map.put(RETRIES_KEY, getRetries());
		map.put(LOGGING_KEY, isLogging());
		map.put(CMDLINE_ARGS_KEY, getCmdlineArgs());
		map.put(CLASSPATH_KEY, getClasspath());
		map.put(JAVA_LOCATION_KEY, getJavaLocation());
		return map;
	}

	public void setPropertiesModifiedDate(Date propertiesModifiedDate) {
		this.propertiesModifiedDate = propertiesModifiedDate;
	}

	public Date getPropertiesModifiedDate() {
		return propertiesModifiedDate;
	}

	public void setProperties(Map<String, Object> propertiesMap) {
		setJvmParameters((String)propertiesMap.get(JVM_PARAMETERS_KEY));
		setRetries((Integer)propertiesMap.get(RETRIES_KEY));
		setLogging((Boolean)propertiesMap.get(LOGGING_KEY));
		setCmdlineArgs((String)propertiesMap.get(CMDLINE_ARGS_KEY));
		setClasspath((String)propertiesMap.get(CLASSPATH_KEY));
		setJavaLocation((String)propertiesMap.get(JAVA_LOCATION_KEY));
	}
	
	/**
	 * May return null if and only if a referenced stage library was not found in the database.
	 */
	public Set<DatabaseFile> getDatabaseFiles() {
		Map<Object, DatabaseFile> files = new HashMap<Object, DatabaseFile>();
		
		for(Stage s : stages.values()) {
			if(s.getDatabaseFile()==null) {
				logger.error("Stage group '"+s.getName()+"' is missing it's library file");
				return null;
			}
			else if(!files.containsKey(s.getDatabaseFile().getId())) {
				files.put(s.getDatabaseFile().getId(), s.getDatabaseFile());
			}
		}
		
		return new HashSet<DatabaseFile>(files.values());
	}
	
	public Set<String> getStageNames() {
		return Collections.unmodifiableSet(new HashSet<String>(stages.keySet()));
	}
	
	public boolean hasStage(String name) {
		return stages.containsKey(name);
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((name == null) ? 0 : name.hashCode());
		result = prime * result + ((propertiesModifiedDate == null) ? 0 : propertiesModifiedDate.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj) {
			return true;
		}
		if (getClass() != obj.getClass()) {
			return false;
		}
		
		StageGroup g = (StageGroup) obj;
		if(g.getSize() == stages.size()) {
			for(Stage s : g.getStages()) {
				if(!hasStage(s.getName()) || !getStage(s.getName()).equals(s)) {
					return false;
				}
			}
			if((getPropertiesModifiedDate()==null) != (g.getPropertiesModifiedDate()==null)) {
				return false;
			} else if (getPropertiesModifiedDate()==null && g.getPropertiesModifiedDate() == null) {
				return true;
			}
			return getPropertiesModifiedDate().equals(g.getPropertiesModifiedDate());
		}
		return false;
	}

	public Stage getStage(String name) {
		return stages.get(name);
	}
	
	public Stage removeStage(String stageName) {
		return stages.remove(stageName);
	}

	public Set<Stage> getStages() {
		return Collections.unmodifiableSet(new HashSet<Stage>(stages.values()));
	}
	
	public void addStage(Stage stage) {
		stages.put(stage.getName(), stage);
	}

	public int getSize() {
		return stages.size();
	}

	public boolean isEmpty() {
		return stages.isEmpty();
	}
}
