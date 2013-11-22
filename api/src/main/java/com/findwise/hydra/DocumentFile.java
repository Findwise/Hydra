package com.findwise.hydra;

import java.io.InputStream;
import java.util.Date;

/**
 * Represents a file attachment to a document
 *
 * Equality of a {@link DocumentFile} depends only on <strong>metadata</strong>,
 * not actual file content.
 *
 * @param <T>
 */
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

	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;

		DocumentFile that = (DocumentFile) o;

		if (documentId != null ? !documentId.equals(that.documentId) : that.documentId != null) return false;
		if (encoding != null ? !encoding.equals(that.encoding) : that.encoding != null) return false;
		if (fileName != null ? !fileName.equals(that.fileName) : that.fileName != null) return false;
		if (mimetype != null ? !mimetype.equals(that.mimetype) : that.mimetype != null) return false;
		if (savedByStage != null ? !savedByStage.equals(that.savedByStage) : that.savedByStage != null) return false;
		if (uploadDate != null ? uploadDate.compareTo(that.uploadDate) != 0 : that.uploadDate != null) return false;

		return true;
	}

	@Override
	public int hashCode() {
		int result = fileName != null ? fileName.hashCode() : 0;
		result = 31 * result + (uploadDate != null ? (int)uploadDate.getTime() : 0);
		result = 31 * result + (documentId != null ? documentId.hashCode() : 0);
		result = 31 * result + (savedByStage != null ? savedByStage.hashCode() : 0);
		result = 31 * result + (encoding != null ? encoding.hashCode() : 0);
		result = 31 * result + (mimetype != null ? mimetype.hashCode() : 0);
		return result;
	}
}
