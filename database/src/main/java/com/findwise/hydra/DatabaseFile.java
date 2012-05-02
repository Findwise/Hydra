package com.findwise.hydra;

import java.io.IOException;
import java.io.InputStream;
import java.util.Date;

public class DatabaseFile {
	private String filename;
	private Date uploadDate;
	private Object id;
	private InputStream inputStream;
	
	public Object getId() {
		return id;
	}

	public void setId(Object id) {
		this.id = id;
	}
	
	public String getFilename() {
		return filename;
	}

	public void setFilename(String filename) {
		this.filename = filename;
	}
	
	public Date getUploadDate() {
		return uploadDate;
	}

	public void setUploadDate(Date uploadDate) {
		this.uploadDate = uploadDate;
	}
	
	public void attach(InputStream inputStream) {
		this.inputStream = inputStream;
	}
	
	/**
	 * Also closes stream
	 */
	public void detachInputStream() throws IOException {
		inputStream.close();
		inputStream = null;
	}
	
	public boolean hasInputStream() {
		return inputStream != null;
	}
	
	public InputStream getInputStream() {
		return inputStream;
	}
	
	public boolean isEqual(DatabaseFile f) {
		if(f.getFilename().equals(filename) && uploadDate.equals(f.getUploadDate()) && id.equals(f.getId())) {
			return true;
		}
		return false;
	}
}
