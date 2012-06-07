package com.findwise.hydra;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.findwise.hydra.mongodb.MongoConnector;
import com.findwise.hydra.mongodb.MongoType;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.google.inject.name.Named;

@Singleton
public final class NodeMaster extends Thread {
	public static final int DEFAULT_POLLING_INTERVAL = 60; //Seconds
	private Logger logger = LoggerFactory.getLogger(NodeMaster.class);
	
	private DatabaseConnector<MongoType> dbc;
	
	private StoredPipeline pipeline;
	private int pollingInterval = DEFAULT_POLLING_INTERVAL;
	private StageManager sm;

	private int port;
	
	@Inject
	private NodeMaster(@Named(Configuration.POLLING_INTERVAL_PARAM) int pollingInterval, MongoConnector dbc, StoredPipeline pipeline, @Named(Configuration.REST_PORT_PARAM) int port) {
		this.dbc = dbc;
		sm = StageManager.getStageManager();
		this.pipeline = pipeline;
		this.pollingInterval = pollingInterval;
		this.port = port;
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

	/** 
	 * Check if the JAR-file is there, in that case start it. 
	 **/
	private boolean launchStage(StoredStage stage) {
		StageRunner wrapper = new StageRunner(stage, port);
		sm.addWrapper(stage, wrapper);
		wrapper.start();
		return true;
	}
	
	private void stopStage(StoredStage stage) {
		StageRunner sw = sm.getWrapper(stage);
		if(sw!=null) {
			sw.destroy();
		}
		else {
			logger.warn("Tried to stop stage "+stage.getName()+", but no process running it was found.");
		}
	}

	public void run() {
		while (true) {
			Pipeline<Stage> newPipeline = dbc.getPipelineReader().getPipeline();
			if(!newPipeline.isEqual(pipeline)) {
				logger.info("Pipeline has been updated");
				try {
					updatePipeline(newPipeline);
					restartStopped();
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
				System.exit(2);
			}
		}
	}
	
	private void restartStopped() {
		for(StoredStage s : pipeline.getStages()) {
			if(!sm.wrapperExists(s)) {
				launchStage(s);
			}
		}
	}
	
	/**
	 * Updates pipeline and stops all stages that have been changed. After this method has run
	 * those runners will need to be recreated.
	 * @param newPipeline
	 * @throws IOException
	 */
	private void updatePipeline(Pipeline<Stage> newPipeline) throws IOException {
		for (StoredStage s : getRemoved(newPipeline)) {
			stopAndRemove(s);
		}
		
		List<StoredStage> updated = getFileUpdated(newPipeline);
		for (StoredStage s : updated) {
			stopAndRemove(s);
			addStage(s.getName(), newPipeline);
		}

		List<Stage> newStages = getAllNew(newPipeline);
		for(Stage s : newStages) {
			addStage(s.getName(), newPipeline);
		}
		
		List<StoredStage> propUpdated = getPropertiesUpdated(newPipeline);
		for(StoredStage stage : propUpdated) {
			sm.getWrapper(stage).destroy();
			sm.removeWrapper(stage);
			addStage(stage.getName(), newPipeline);
		}
	}
	
	private void addStage(String stageName, Pipeline<Stage> newPipeline) throws IOException {
		Stage newStage = newPipeline.getStage(stageName);
		StoredStage stage = new StoredStage(stageName, newStage.getDatabaseFile());
		stage.setProperties(newStage.getProperties());
		stage.setPropertiesModifiedDate(newStage.getPropertiesModifiedDate());
		stage.setMode(newStage.getMode());
		stage.getDatabaseFile().attach(dbc.getPipelineReader().getStream(stage.getDatabaseFile()));
		pipeline.addStage(stage);
		stage.getDatabaseFile().detachInputStream();
	}
	
	private void stopAndRemove(StoredStage s) {
		if (sm.wrapperExists(s)) {
			stopStage(s);
			sm.removeWrapper(s);
		}
		patientRemoveStage(s, 500, 10);
	}
	
	/**
	 * Should the delete fail, this will retry again after waitMs milliseconds, up to numRetries times.
	 * Is needed on windows systems due to file locks...
	 */
	private void patientRemoveStage(StoredStage stage, long waitMs, int numRetries) {
		for(int i=0; !pipeline.removeStage(stage) && i<numRetries; i++) {
			try {
				Thread.sleep(waitMs);
			} catch (InterruptedException e) {
				Thread.interrupted();
			}
		}
		if(pipeline.hasStage(stage.getName())) {
			logger.error("patientRemoveStage() : Unable to remove stage "+stage.getName()+ " from the running pipeline");
		}
	}
	
	private List<StoredStage> getPropertiesUpdated(Pipeline<Stage> newPipeline) {
		List<StoredStage> updated = new ArrayList<StoredStage>();
		for(StoredStage s : pipeline.getStages()) {
			if(newPipeline.hasStage(s.getName()) && s.getPropertiesModifiedDate().before(newPipeline.getStage(s.getName()).getPropertiesModifiedDate())) {
				updated.add(s);
			}
		}
		return updated;
	}
	
	private List<Stage> getAllNew(Pipeline<Stage> newPipeline) {
		List<Stage> newStages = new ArrayList<Stage>();
		for(Stage s : newPipeline.getStages()) {
			if(!pipeline.hasStage(s.getName())) {
				newStages.add(s);
			}
		}
		return newStages;
	}
	
	private List<StoredStage> getRemoved(Pipeline<Stage> newPipeline) {
		List<StoredStage> removed = new ArrayList<StoredStage>();
		for(StoredStage s : pipeline.getStages()) {
			if(!newPipeline.hasStage(s.getName())) {
				removed.add(s);
			}
		}
		return removed;
	}
	
	private List<StoredStage> getFileUpdated(Pipeline<Stage> newPipeline) {
		List<StoredStage> updated = new ArrayList<StoredStage>();
		for(StoredStage s : pipeline.getStages()) {
			if(newPipeline.hasStage(s.getName()) && s.getDatabaseFile().getUploadDate().before(newPipeline.getStage(s.getName()).getDatabaseFile().getUploadDate())) {
				updated.add(s);
			}
		}
		return updated;
	}
	
	public DatabaseConnector<MongoType> getDatabaseConnector() {
		return dbc;
	}
	
	public StoredPipeline getPipeline() {
		return pipeline;
	}
}
