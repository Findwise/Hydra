package com.findwise.hydra;

import java.io.File;
import java.io.IOException;

import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The Configuration class represents a collection of <em>files</em> that
 * comprise all logic that is part of a pipeline configuration, typically files
 * of the types .jar and .properties.
 * 
 * @author joel.westberg
 */
public class StoredPipeline extends Pipeline<StoredStage> {
	private String directory;
	
	private Logger logger = LoggerFactory.getLogger(StoredPipeline.class);
	
	
	public StoredPipeline(String directory) throws IOException {
		this.directory = directory;
		
		File dir = new File(directory);
		if (!dir.exists()) {
			logger.info("Specified configuration directory did not exist. Creating it.");
			boolean result = dir.mkdir();
			if (!result) {
				throw new IOException("Unable to create configuration directory");
			}
		}
	}
	
	@Override
	public boolean removeStage(StoredStage stage) {
		try {
			FileUtils.deleteDirectory(getStageDirectory(stage));
			return true;
		} catch (IOException e) {
			logger.error("Unable to delete directory for stage "+stage.getName());
			return false;
		}
	}
	
	@Override
	public StoredStage addStage(StoredStage stage) {
		if(!stage.getDatabaseFile().hasInputStream()) {
			logger.error("addStage() did not have access to InputStream for stage "+stage.getName());
			return null;
		}
		try {
			File f = saveFile(stage);
			stage.setFile(f);
		} catch (IOException e) {
			logger.error("addStage() IOException occurred when reading inputstream for stage "+stage.getName(), e);
			return null;
		}
		return getStageMap().put(stage.getName(), stage);
	}
	
	
	private File saveFile(StoredStage stage) throws IOException {
		//Save to main directory/stagename
		File file = new File(getStageDirectory(stage), stage.getDatabaseFile().getFilename());
		FileUtils.copyInputStreamToFile(stage.getDatabaseFile().getInputStream(), file);
		return file;
	}
	
	private File getStageDirectory(StoredStage stage) {
		return new File(directory+File.separatorChar+stage.getName());
	}

	public String getDirectory() {
		return directory;
	}
}
