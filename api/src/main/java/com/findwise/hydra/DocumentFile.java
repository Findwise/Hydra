package com.findwise.hydra;

import java.io.InputStream;
import java.util.Date;

public class DocumentFile<T> {
	private InputStream stream;
	private String fileName;
	private Date uploadDate;
	private DocumentID<T> documentId;
	private String savedByStage;
	private String encoding = "UTF-8";
	private String mimetype;

	public DocumentFile(DocumentID<T> documentId, String fileName, InputStream stream) {
		this(documentId, fileName, stream, null, null);
	}
	
	public DocumentFile(DocumentID<T> documentId, String fileName, InputStream stream, String savedByStage) {
		this(documentId, fileName, stream, savedByStage, null);
	}
	
	public DocumentFile(DocumentID<T> documentId, String fileName, InputStream stream, String savedByStage, Date uploadDate) {
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

	public DocumentID<T> getDocumentId() {
		return documentId;
	}

	public void setDocumentId(DocumentID<T> documentId) {
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
	
//	@Override
//	public String toJson() {
//		HashMap<String, Object> map = new HashMap<String, Object>();
//		
//		map.put("stream", stream);
//		map.put("fileName", fileName);
//		map.put("uploadDate", uploadDate);
//		map.put("documentId", documentId.getID());
//		map.put("savedByStage", savedByStage);
//		map.put("encoding", encoding);
//		map.put("mimetype", mimetype);
//		
//		return SerializationUtils.toJson(map);
//	}

//	@Override
//	public void fromJson(String json) throws JsonException {
//		Map<String, Object> map = SerializationUtils.fromJson(json);
//		
//		stream = (InputStream) map.get("stream");
//		fileName = (String) map.get("fileName");
//		uploadDate = (Date) map.get("uploadDate");
//		documentId = new map.get("stream");
//	}
}
