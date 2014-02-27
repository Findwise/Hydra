package com.findwise.hydra;

import com.findwise.hydra.local.Local;

import java.util.List;

public interface DocumentFileRepository {
	public DocumentFile<Local> getFile(String fileName, DocumentID<Local> docid);
	public List<DocumentFile<Local>> getFiles(DocumentID<Local> docid);
	public List<String> getFileNames(DocumentID<?> docid);

	public boolean deleteFile(String fileName, DocumentID<Local> docid);

	// DocumentFile<Local> contains the docId, so no need for it here.
	public boolean saveFile(DocumentFile<Local> df);
}
