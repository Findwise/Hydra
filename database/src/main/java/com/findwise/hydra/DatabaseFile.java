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
		if(uploadDate == null) {
			return null;
		}
		return new Date(uploadDate.getTime());
	}

	public void setUploadDate(Date uploadDate) {
		this.uploadDate = new Date(uploadDate.getTime());
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
	
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((filename == null) ? 0 : filename.hashCode());
		result = prime * result + ((id == null) ? 0 : id.hashCode());
		result = prime * result + ((uploadDate == null) ? 0 : uploadDate.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		DatabaseFile other = (DatabaseFile) obj;
		
		if(equal(other.getFilename(), filename) && equal(other.getUploadDate(), getUploadDate()) && equal(other.getId(), id)) {
			return true;
		}
		return false;
	}
	
	private boolean equal(Object o, Object o2) {
		if(o==o2) {
			return true;
		}
		if(o==null) {
			return false;
		}
		return o.equals(o2);
	}
}
