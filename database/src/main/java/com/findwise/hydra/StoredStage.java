package com.findwise.hydra;

import java.io.File;

public class StoredStage extends Stage {
	private File file;
	
	public StoredStage(String stageName, DatabaseFile databaseFile) {
		super(stageName, databaseFile);
	}
	
	public File getFile() {
		return file;
	}
	
	public void setFile(File file) {
		this.file = file;
	}
	
}
