package com.findwise.hydra.common;

import java.io.InputStream;
import java.util.Date;

public class DocumentFile {
	private InputStream stream;
	private String fileName;
	private Date uploadDate;
	private Object documentId;
	
	public DocumentFile(Object documentId, String fileName, InputStream stream) {
		this(documentId, fileName, stream, null);
	}
	
	public DocumentFile(Object documentId, String fileName, InputStream stream, Date uploadDate) {
		this.documentId = documentId;
		this.fileName = fileName;
		this.stream = stream;
		this.uploadDate = uploadDate;
	}

	public InputStream getStream() {
		return stream;
	}

	public void setStream(InputStream stream) {
		this.stream = stream;
	}

	public String getFileName() {
		return fileName;
	}

	public void setFileName(String fileName) {
		this.fileName = fileName;
	}

	public Date getUploadDate() {
		return uploadDate;
	}

	public void setUploadDate(Date uploadDate) {
		this.uploadDate = uploadDate;
	}

	public Object getDocumentId() {
		return documentId;
	}

	public void setDocumentId(Object documentId) {
		this.documentId = documentId;
	}
}
