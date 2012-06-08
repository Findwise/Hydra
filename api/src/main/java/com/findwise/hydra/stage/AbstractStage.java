package com.findwise.hydra.stage;

import java.io.IOException;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.apache.http.HttpException;

import com.findwise.hydra.common.JsonException;
import com.findwise.hydra.common.Logger;
import com.findwise.hydra.common.SerializationUtils;
import com.findwise.hydra.local.RemotePipeline;

/**
 * 
 * Base class for handling stages in Hydra. Implementations of this
 * class get the RemotePipeline communication as well as Thread handling for
 * free.  
 * 
 * @author anton.hagerstrand
 * @author simon.stenstrom
 * @author anders.rask
 * 
 */
public abstract class AbstractStage extends Thread {

	public static final String DEV_MODE_STRING = "DEV_MODE";
	public static final String ARG_NAME_STAGE_CLASS = "stageClass";
	public static final String PROPERTY_NAME_COMMANDLINE_ARGS = "cmdline_args";
	
	@Parameter
	private List<String> queryOptions;
	
	public List<String> getQueryOptions() {
		return queryOptions;
	}
	
	public static final int CMDLINE_STAGE_NAME_PARAM = 0;
	public static final int CMDLINE_PIPELINE_HOST_PARAM = 1;
	public static final int CMDLINE_PIPELINE_PORT_PARAM = 2;
	
	public static final int DEFAULT_HOLD_INTERVAL = 2000;
	private RemotePipeline remotePipeline = null;
	private Thread shutDownHook;
	
	/**
	 * Initiates an implementation of AbstractDocument. When this method is
	 * called, and Object of the class has been initialized. The arguments
	 * provided when starting this step is available via getArgument(String
	 * argName)
	 * 
	 * @throws RequiredArgumentMissingException
	 */
	public void init() throws RequiredArgumentMissingException {
		
	}

	public Thread getShutDownHook() {
		return shutDownHook;
	}

	public void setShutDownHook(Thread shutDownHook) {
		this.shutDownHook = shutDownHook;
	}

	private String stageName;
	private boolean continueRunning;

	/**
	 * 
	 * @return Returns the RemotePipeline instances used by this Stage
	 */
	public RemotePipeline getRemotePipeline() {
		return remotePipeline;
	}

	/**
	 * 
	 * @param rp
	 *            A RemotePipeline to use
	 */
	public void setRemotePipeline(RemotePipeline rp) {
		this.remotePipeline = rp;
	}

	/**
	 * Method to stop execution of current stage. Calling this method will
	 * result in that no more documents will be processed, after the current
	 * document. Processing of the current document will not be interrupted.
	 */
	public synchronized void stopStage() {
		continueRunning = false;
	}

	protected synchronized boolean isContinueRunning() {
		return continueRunning;
	}

	protected synchronized void setContinueRunning(boolean val) {
		continueRunning = val;
	}

	public void setStageName(String stageName) {
		this.stageName = stageName;
	}

	public String getStageName() {
		return stageName;
	}

	
	/**
	 * Injects the parameters found in the map to any fields annotated with @Stage, whose names matches
	 * the keys in this map.
	 * @param map
	 * @throws IllegalArgumentException
	 * @throws IllegalAccessException
	 */
	public void setParameters(Map<String, Object> map) throws IllegalArgumentException, IllegalAccessException {
		if (getClass().isAnnotationPresent(Stage.class)) {
			for(Field field : getParameterFields()) {
				if (map.containsKey(field.getName())) {
					boolean prevAccessible = field.isAccessible();
					if (!prevAccessible) {
						field.setAccessible(true);
					}
					field.set(this, map.get(field.getName()));
					field.setAccessible(prevAccessible);
				}
			}
		}
	}
	
	public List<Field> getParameterFields() {
		List<Field> list = new ArrayList<Field>();
		addParameterFields(list, getClass());
		return list;
	}
	
	private void addParameterFields(List<Field> list, Class<?> startClass) {
		for (Field field : startClass.getDeclaredFields()) {
			if (field.isAnnotationPresent(Parameter.class)) {
				list.add(field);
			}
		}
		Class<?> superClass = startClass.getSuperclass();
		if(!superClass.equals(Object.class)) {
			addParameterFields(list, superClass);
		}
	}

	public void setUp(RemotePipeline rp, Map<String, Object> properties) throws IllegalArgumentException, IllegalAccessException, JsonException, IOException, HttpException {
		setRemotePipeline(rp);
		setParameters(properties);
		this.createAndApplyShutDownHook();
	}
	
	@SuppressWarnings("unchecked")
	public static AbstractStage getInstance(String[] args) {
		Logger.debug("Starting AbstractStage with args: " + Arrays.toString(args));
		if (args.length < 1) {
			Logger.error("No stage name found", new RequiredArgumentMissingException("No stage name was specified"));
			System.exit(1);
		} 
		try {
			int addToIndex = 0;
			
			Map<String, Object> properties = null;
			if(args[0].equals(DEV_MODE_STRING)) {
				//args[0] is DEV_MODE_STRING, args[1] stageName, args[2]+args[3] host+port, args[4] and on is config
				addToIndex = 1;
				String jsonString = "";
				for(int i=4; i<args.length; i++) {
					jsonString += args[i]+" ";
				}
				properties = SerializationUtils.fromJson(jsonString);
			}
			String stageName = (CMDLINE_STAGE_NAME_PARAM+addToIndex<args.length) ? args[CMDLINE_STAGE_NAME_PARAM+addToIndex] : null;
			String hostName = (CMDLINE_PIPELINE_HOST_PARAM+addToIndex<args.length) ? args[CMDLINE_PIPELINE_HOST_PARAM+addToIndex] : RemotePipeline.DEFAULT_HOST; 
			String port = (CMDLINE_PIPELINE_PORT_PARAM+addToIndex<args.length) ? args[CMDLINE_PIPELINE_PORT_PARAM+addToIndex] : ""+RemotePipeline.DEFAULT_PORT; 
			
			RemotePipeline rp = new RemotePipeline(hostName, Integer.parseInt(port), stageName);
			if(properties==null) {				
				properties = rp.getProperties();
			}
	
			String stageClass;
			if(properties.containsKey(ARG_NAME_STAGE_CLASS)) {
				stageClass = (String) properties.get(ARG_NAME_STAGE_CLASS);
			}
			else {
				throw new RequiredArgumentMissingException("No class specified in the '"+ARG_NAME_STAGE_CLASS+"' property.");
			}
			
			Class<? extends AbstractStage> actualClass = (Class<? extends AbstractStage>) Class
					.forName(stageClass);
			AbstractStage stage = actualClass.newInstance();
			
			stage.setName(stageName);
			stage.setUp(rp, properties);
			stage.init();
			
			return stage;

		} catch (RequiredArgumentMissingException e) {
			Logger.error("Failed to read arguments", e);
		} catch (ClassNotFoundException e) {
			Logger.error("Could not find the Stage class in classpath", e);
		} catch (InstantiationException e) {
			Logger.error("Could not instantiate the Stage class", e);
		} catch (IllegalAccessException e) {
			Logger.error("Could not access constructor of Stage class", e);
		} catch (JsonException e) {
			Logger.error("Communication failiure when reading properties", e);
		} catch (HttpException e) {
			Logger.error("Communication failiure when reading properties", e);
		} catch (IOException e) {
			Logger.error("Communication failiure when reading properties", e);
		}
		return null;
	}

	public static void main(String args[]) {
		AbstractStage stage = getInstance(args);
		if(stage==null) {
			System.exit(1);
		}
		stage.start();
		Logger.info("Started stage: " + stage.getName());
	}

	/**
	 * This method should (not sure if it will work in all environments) run
	 * when the Stage is terminated. If it is run, it is run after letting a
	 * process(LocalDocuement doc) return.
	 * 
	 * The default implementation of this method does nothing, so override it if
	 * you need to do something (for example close file readers etc).
	 */
	public void onDestroy() {
	}

	public Thread createAndApplyShutDownHook() {
		shutDownHook = new OnDestroyThread();
		Runtime.getRuntime().addShutdownHook(shutDownHook);
		return shutDownHook;

	}

	private class OnDestroyThread extends Thread {

		public void run() {

			Logger.info("Shutting down stage: " + getStageName());
			if (AbstractStage.this.isAlive()) {
				AbstractStage.this.setContinueRunning(false);
				try {
					AbstractStage.this.join();
				}
				catch (InterruptedException e) {
				}
			}
			AbstractStage.this.onDestroy();
		}
	}
}
