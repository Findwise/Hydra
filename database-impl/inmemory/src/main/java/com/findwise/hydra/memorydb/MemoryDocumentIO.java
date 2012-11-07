package com.findwise.hydra.memorydb;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.findwise.hydra.DatabaseDocument;
import com.findwise.hydra.DatabaseQuery;
import com.findwise.hydra.DocumentReader;
import com.findwise.hydra.DocumentWriter;
import com.findwise.hydra.TailableIterator;
import com.findwise.hydra.common.Document;
import com.findwise.hydra.common.DocumentFile;
import com.findwise.hydra.common.JsonException;
import com.findwise.hydra.common.SerializationUtils;

public class MemoryDocumentIO implements DocumentWriter<MemoryType>,
		DocumentReader<MemoryType> {

	private HashSet<MemoryDocument> set;
	private LinkedBlockingQueue<MemoryDocument> inactive;
	private boolean[] b = new boolean[1];
	
	private static Logger logger = LoggerFactory.getLogger(MemoryDocumentIO.class); 
	
	private HashSet<DocumentFile> files;

	public static final int inactiveSize = 100;

	public MemoryDocumentIO() {
		set = new HashSet<MemoryDocument>();
		files = new HashSet<DocumentFile>();
		inactive = new LinkedBlockingQueue<MemoryDocument>(inactiveSize);
		b[0] = false;
	}

	private void addInactive(MemoryDocument d) {
		if(!inactive.offer(d)) {
			inactive.poll();
			b[0] = true;
			inactive.offer(d);
		}
	}

	@Override
	public MemoryDocument getDocument(DatabaseQuery<MemoryType> q) {
		List<DatabaseDocument<MemoryType>> list = getDocuments(q, 1);
		if (list.size() == 0) {
			return null;
		}
		return (MemoryDocument) list.get(0);
	}

	@Override
	public MemoryDocument getDocumentById(Object id) {
		return (MemoryDocument) getDocumentById(id, false);
	}

	@Override
	public DatabaseDocument<MemoryType> getDocumentById(Object id,
			boolean includeInactive) {
		for (MemoryDocument d : set) {
			if (d.getID().equals(id)) {
				return d;
			}
		}
		if (includeInactive) {
			for (MemoryDocument d : inactive) {
				if (d.getID().equals(id)) {
					return d;
				}
			}
		}
		return null;
	}

	@Override
	public TailableIterator<MemoryType> getInactiveIterator() {
		return getInactiveIterator(new MemoryQuery());
	}

	@SuppressWarnings("unchecked")
	@Override
	public List<DatabaseDocument<MemoryType>> getDocuments(
			DatabaseQuery<MemoryType> q, int limit, int skip) {
		ArrayList<MemoryDocument> list = new ArrayList<MemoryDocument>();

		int matching = 0;
		for (MemoryDocument doc : set) {
			if (list.size() >= limit)
				break;
			
			if (doc.matches((MemoryQuery) q)) {
				if (matching >= skip) {
					list.add(doc);
				}
				matching++;
			}
		}

		return (List<DatabaseDocument<MemoryType>>) (Object) list;
	}
	
	@Override
	public List<DatabaseDocument<MemoryType>> getDocuments(
			DatabaseQuery<MemoryType> q, int limit) {
		return getDocuments(q, limit, 0);
	}

	@Override
	public long getNumberOfDocuments(DatabaseQuery<MemoryType> q) {
		long matching = 0;

		for (MemoryDocument doc : set) {
			if (doc.matches((MemoryQuery) q)) {
				matching++;
			}
		}

		return matching;
	}

	@Override
	public DocumentFile getDocumentFile(DatabaseDocument<MemoryType> d, String fileName) {
		for(DocumentFile f : files) {
			if(f.getDocumentId().equals(d.getID()) && f.getFileName().equals(fileName)) {
				try {
					return copy(f);
				} catch (IOException e) {
					logger.error("Error copying the streams", e);
				}
			}
		}
		return null;
	}
	
	@Override
	public boolean deleteDocumentFile(DatabaseDocument<MemoryType> d, String fileName) {
		for(DocumentFile f : files) {
			if(f.getDocumentId().equals(d.getID()) && f.getFileName().equals(fileName)) {
				files.remove(f);
				return true;
			}
		}
		return false;
	}

	@Override
	public List<String> getDocumentFileNames(DatabaseDocument<MemoryType> d) {
		ArrayList<String> list = new ArrayList<String>();
		
		for(DocumentFile f : files) {
			if(f.getDocumentId().equals(d.getID())) {
				list.add(f.getFileName());
			}
		}
		
		return list;
	}

	@Override
	public long getActiveDatabaseSize() {
		return set.size();
	}

	@Override
	public long getInactiveDatabaseSize() {
		return inactive.size();
	}

	@Override
	public MemoryDocument getAndTag(DatabaseQuery<MemoryType> query, String tag) {
		((MemoryQuery)query).requireNotFetchedBy(tag);
		MemoryDocument d = getDocument(query);
		if(d!=null) {
			d.tag(Document.FETCHED_METADATA_TAG, tag);
		}
		return d;
	}

	@Override
	public DatabaseDocument<MemoryType> getAndTagRecurring(
			DatabaseQuery<MemoryType> query, String tag) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public DatabaseDocument<MemoryType> getAndTagRecurring(
			DatabaseQuery<MemoryType> query, String tag, int intervalMillis) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public boolean markTouched(Object id, String tag) {
		MemoryDocument d = getDocumentById(id);
		if(d==null) {
			return false;
		}
		d.tag(Document.TOUCHED_METADATA_TAG, tag);
		return true;
	}

	@Override
	public boolean markProcessed(DatabaseDocument<MemoryType> d, String stage) {
		return markDone(d, stage, Document.PROCESSED_METADATA_FLAG);
	}

	@Override
	public boolean markDiscarded(DatabaseDocument<MemoryType> d, String stage) {
		return markDone(d, stage, Document.DISCARDED_METADATA_FLAG);
	}

	@Override
	public boolean markFailed(DatabaseDocument<MemoryType> d, String stage) {
		return markDone(d, stage, Document.FAILED_METADATA_FLAG);
	}
	
	private boolean markDone(DatabaseDocument<MemoryType> d, String stage, String flag) {
		MemoryDocument temp = getDocumentById(d.getID());
		if(temp==null) {
			return false;
		}
		set.remove(temp);
		((MemoryDocument)d).tag(flag, stage);
		deleteAllFiles(d);
		addInactive((MemoryDocument)d);
		return true;
	}

	@Override
	public boolean markPending(DatabaseDocument<MemoryType> d, String stage) {
		MemoryDocument temp = getDocumentById(d.getID());
		if(temp==null) {
			return false;
		}
		temp.tag(Document.PENDING_METADATA_FLAG, stage);
		
		return true;
	}

	@Override
	public boolean insert(DatabaseDocument<MemoryType> d) {
		MemoryDocument md = (MemoryDocument) d;
		md.setID(md.hashCode()+""+System.currentTimeMillis());
		
		set.add(md);
		md.markSynced();
		return true;
	}

	@Override
	public boolean update(DatabaseDocument<MemoryType> d) {
		MemoryDocument md = (MemoryDocument) d;
		
		MemoryDocument inDb = getDocumentById(d.getID());
		
		if(md.isTouchedAction()) {
			inDb.setAction(md.getAction());
		}
		
		for(String s : md.getTouchedContent()) {
			inDb.putContentField(s, md.getContentField(s));
		}
		
		for(String s : md.getTouchedMetadata()) {
			inDb.putMetadataField(s, md.getMetadataMap().get(s));
		}
		
		md.markSynced();
		
		return true;
	}

	@Override
	public void delete(DatabaseDocument<MemoryType> d) {
		deleteAllFiles(d);
		set.remove(getDocumentById(d.getID()));
	}

	private void deleteAllFiles(DatabaseDocument<MemoryType> d) {
		for(String fileName : getDocumentFileNames(d)) {
			deleteDocumentFile(d, fileName);
		}
	}

	@Override
	public void deleteAll() {
		set.clear();
	}

	@Override
	public void write(DocumentFile df) throws IOException {
		df.setUploadDate(new Date());
		files.add(copy(df));
		df.getStream().close();
	}
	
	private DocumentFile copy(DocumentFile df) throws IOException {
		String s = IOUtils.toString(df.getStream(), df.getEncoding());
		df.getStream().close();
		df.setStream(IOUtils.toInputStream(s, df.getEncoding()));
		return new DocumentFile(df.getDocumentId(), df.getFileName(), IOUtils.toInputStream(s, df.getEncoding()), df.getSavedByStage(), df.getUploadDate());
	}

	@Override
	public void prepare() {
		
	}
	
	@Override
	public Object toDocumentId(Object jsonPrimitive) {
		return jsonPrimitive.toString();
	}
	
	@Override
	public Object toDocumentIdFromJson(String json) {
		try {
			return SerializationUtils.toObject((String) json).toString();
		} catch (JsonException e) {
			logger.error("Unable to deserialize document id", e);
			return null;
		}
	}

	@Override
	public TailableIterator<MemoryType> getInactiveIterator(DatabaseQuery<MemoryType> query) {
		return new MemoryTailableIterator(inactive, b, new MemoryQuery());
	}

}
