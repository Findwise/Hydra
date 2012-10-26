package com.findwise.hydra;

import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class StageGroup extends HashSet<Stage>{
	private static final long serialVersionUID = -2928443572343949817L;
	
	public static final String JVM_PARAMETERS_KEY = "jvm_parameters";
	public static final String RETRIES_KEY = "retries";
	public static final String LOGGING_KEY = "logging";
	public static final String CMDLINE_ARGS_KEY = "cmdline_args";
	public static final String CLASSPATH_KEY = "classpath";
	public static final String JAVA_LOCATION_KEY = "java_location";
	
	private String jvmParameters;
	private String classpath;
	private int retries = -1;
	private String cmdlineArgs;
	private boolean logging = false;
	private boolean changedProperties = false;
	private Date propertiesModifiedDate;
	private String name;
	private String javaLocation;
	
	public StageGroup(String name) {
		this.name = name;
	}
	
	public StageGroup(String name, Map<String, Object> propertiesMap) {
		this.name = name;
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
		if(propertiesModifiedDate==null) {
			propertiesModifiedDate = new Date();
		}
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
	
	public Set<DatabaseFile> getDatabaseFiles() {
		Map<Object, DatabaseFile> files = new HashMap<Object, DatabaseFile>();
		
		for(Stage s : this) {
			if(!files.containsKey(s.getDatabaseFile().getId())) {
				files.put(s.getDatabaseFile().getId(), s.getDatabaseFile());
			}
		}
		
		return new HashSet<DatabaseFile>(files.values());
	}
}
