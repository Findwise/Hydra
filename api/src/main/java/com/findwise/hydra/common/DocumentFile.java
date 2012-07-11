package com.findwise.hydra.common;

import java.io.InputStream;
import java.util.Date;

public class DocumentFile {
	private InputStream stream;
	private String fileName;
	private Date uploadDate;
	private Object documentId;
	private String savedByStage;
	private String encoding = "UTF-8";
	private String mimetype;

	public DocumentFile(Object documentId, String fileName, InputStream stream) {
		this(documentId, fileName, stream, null, null);
	}
	
	public DocumentFile(Object documentId, String fileName, InputStream stream, String savedByStage) {
		this(documentId, fileName, stream, savedByStage, null);
	}
	
	public DocumentFile(Object documentId, String fileName, InputStream stream, String savedByStage, Date uploadDate) {
		this.documentId = documentId;
		this.fileName = fileName;
		this.stream = stream;
		this.uploadDate = uploadDate;
		this.savedByStage = savedByStage;
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

	public void setSavedByStage(String savedByStage) {
		this.savedByStage = savedByStage;
	}

	public String getSavedByStage() {
		return savedByStage;
	}

	public void setMimetype(String mimetype) {
		this.mimetype = mimetype;
	}

	public String getMimetype() {
		return mimetype;
	}

	public void setEncoding(String encoding) {
		this.encoding = encoding;
	}

	public String getEncoding() {
		return encoding;
	}
	
}
