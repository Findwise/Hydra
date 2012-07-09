package com.findwise.hydra.memorydb;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.commons.io.IOUtils;

import com.findwise.hydra.DatabaseDocument;
import com.findwise.hydra.DatabaseQuery;
import com.findwise.hydra.DocumentReader;
import com.findwise.hydra.DocumentWriter;
import com.findwise.hydra.TailableIterator;
import com.findwise.hydra.common.Document;
import com.findwise.hydra.common.DocumentFile;

public class MemoryDocumentIO implements DocumentWriter<MemoryType>,
		DocumentReader<MemoryType> {

	private HashSet<MemoryDocument> set;
	private LinkedBlockingQueue<MemoryDocument> inactive;
	private boolean[] b = new boolean[1];
	
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
		return new MemoryTailableIterator(inactive, b);
	}

	@SuppressWarnings("unchecked")
	@Override
	public List<DatabaseDocument<MemoryType>> getDocuments(
			DatabaseQuery<MemoryType> q, int limit) {
		ArrayList<MemoryDocument> list = new ArrayList<MemoryDocument>();
		
		for (MemoryDocument d : set) {
			if (list.size() >= limit) {
				break;
			}

			if(d.matches((MemoryQuery) q)) {
				list.add(d);
			}
		}
		return (List<DatabaseDocument<MemoryType>>) (Object) list;
	}

	@Override
	public DocumentFile getDocumentFile(DatabaseDocument<MemoryType> d) throws IOException {
		for(DocumentFile f : files) {
			if(f.getDocumentId().equals(d.getID())) {
				return copy(f);
			}
		}
		return null;
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
		set.remove(getDocumentById(d.getID()));
	}

	@Override
	public void deleteAll() {
		set.clear();
	}

	@Override
	public void write(DocumentFile df) throws IOException {
		files.add(copy(df));
		df.getStream().close();
	}
	
	private DocumentFile copy(DocumentFile df) throws IOException {
		String s = IOUtils.toString(df.getStream());
		df.getStream().close();
		df.setStream(IOUtils.toInputStream(s));
		return new DocumentFile(df.getDocumentId(), df.getFileName(), IOUtils.toInputStream(s), df.getUploadDate());
	}

	@Override
	public void prepare() {
		
	}

}
