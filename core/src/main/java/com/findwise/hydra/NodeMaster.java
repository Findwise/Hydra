package com.findwise.hydra;

import java.io.File;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.findwise.hydra.mongodb.MongoConnector;
import com.findwise.hydra.mongodb.MongoType;
import com.google.inject.Inject;
import com.google.inject.Singleton;

@Singleton
public final class NodeMaster extends Thread {
	public static final int DEFAULT_POLLING_INTERVAL = 60; //Seconds
	private Logger logger = LoggerFactory.getLogger(NodeMaster.class);
	
	private DatabaseConnector<MongoType> dbc;
	
	private Pipeline pipeline;
	private int pollingInterval = DEFAULT_POLLING_INTERVAL;
	private StageManager sm;
	
	private String namespace;

	private int port;
	
	@Inject
	private NodeMaster(CoreConfiguration conf, MongoConnector dbc, Pipeline pipeline) {
		this.dbc = dbc;
		sm = StageManager.getStageManager();
		this.pipeline = pipeline;
		this.pollingInterval = conf.getPollingInterval();
		this.port = conf.getRestPort();
		this.namespace = conf.getNamespace();
	}
	/**
	 * Starts the NodeMaster.
	 */
	public void blockingStart() throws IOException {

		try {
			dbc.connect();
		} catch (IOException e) {
			logger.error("Unable to establish connection to database.", e);
			throw e;
		}
		
		super.start();
	}

	public void run() {
		while (!isInterrupted()) {
			Pipeline newPipeline = dbc.getPipelineReader().getPipeline();
			if(!pipeline.equals(newPipeline)) {
				logger.info("Pipeline has been updated");
				try {
					updatePipeline(newPipeline);
					startStopped();
				} catch (IOException e) {
					logger.error("An IOException occurred while updating the pipeline");
					throw new IllegalStateException(e);
				}
			} else {
				logger.debug("No updates found");
			}
			
			try {
				Thread.sleep(pollingInterval * 1000L);
			} catch (InterruptedException e) {
				logger.error("Polling thread interrupted", e);
				interrupt();
			}
		}
	}
	
	private void startStopped() throws IOException {
		for(StageRunner runner : sm.getRunners()) {
			if(!runner.isAlive()) {
				runner.prepare();
				runner.start();
			}
		}
	}
	
	/**
	 * Updates pipeline and stops all stages that have been changed. After this method has run
	 * those runners will need to be recreated.
	 * @param newPipeline
	 * @throws IOException
	 */
	private void updatePipeline(Pipeline newPipeline) throws IOException {
		for(String group : getChangedGroups(newPipeline)) {
			stopGroup(group);
			removeGroup(group);
		}
		
		addMissingGroups(newPipeline);
	}
	
	private Set<String> getChangedGroups(Pipeline newPipeline) {
		HashSet<String> list = new HashSet<String>();
		
		for(StageGroup group : pipeline.getStageGroups()) {
			if(!newPipeline.hasGroup(group.getName()) || !group.equals(newPipeline.getGroup(group.getName()))) {
				list.add(group.getName());
			}
		}
		
		return list;
	}
	
	private void stopGroup(String group) {
		if(sm.getRunner(group).isAlive()) {
			sm.getRunner(group).destroy();
		} else {
			logger.debug("StageGroup "+group+" had already terminated");
		}
	}
	
	private void removeGroup(String group) {
		pipeline.removeGroup(group);
		sm.removeRunner(group);
	}
	
	private void addMissingGroups(Pipeline newPipeline) {
		for(StageGroup group : newPipeline.getStageGroups()) {
			if(!pipeline.hasGroup(group.getName())) {
				pipeline.addGroup(group);
				attachFiles(group);
				sm.addRunner(new StageRunner(group, new File(namespace), port));
			}
		}
	}
	
	private void attachFiles(StageGroup group) {
		for(DatabaseFile file : group.getDatabaseFiles()) {
			file.attach(dbc.getPipelineReader().getStream(file));
		}
	}
	
	public DatabaseConnector<MongoType> getDatabaseConnector() {
		return dbc;
	}
	
	public Pipeline getPipeline() {
		return pipeline;
	}
}
