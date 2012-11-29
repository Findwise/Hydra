package com.findwise.hydra;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.List;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DocumentLogger extends Thread {
	private static final Logger logger = LoggerFactory.getLogger(DocumentLogger.class);
	private DatabaseConnector<?> dbc;
	private TailableIterator<?> iterator;
	private Format format;
	
	private DateFormat df;
	
	public enum Format { SUMMARY, MULTI }
	
	public DocumentLogger(DatabaseConnector<?> dbc, Format format) {
		this.dbc = dbc;
		this.format = format;
		df = new SimpleDateFormat("MM/dd/yyyy HH:mm:ss:SSS");
	}
	
	@Override
	public void run() {
		iterator = dbc.getDocumentReader().getInactiveIterator();
		while(iterator.hasNext()) {
			if(format==Format.SUMMARY) {
				logger.info(getSummary(iterator.next()));
			} else {
				for(String event : getAllEvents(iterator.next())) {
					System.out.println(event);
				}
			}
			
		}
	}
	
	private String getSummary(DatabaseDocument<?> doc) {
		StringBuilder sb = new StringBuilder();
		sb.append(doc.getID());
		sb.append(" status=");
		sb.append(doc.getStatus().toString());

		Date inserted = doc.getTouchedTime(getInserter(doc));
		sb.append(" inserted_time=\"");
		sb.append(inserted);
		sb.append('"');
		
		Date completed = doc.getCompletedTime();
		sb.append(" completed_time=\"");
		sb.append(completed);
		sb.append('"');
		
		sb.append(" total_time=");
		sb.append(completed.getTime()-inserted.getTime());
		
		Set<String> touched = doc.getTouchedBy();
		Set<String> fetched = doc.getFetchedBy();
		for(String e : fetched) {
			sb.append(' ');
			sb.append(e);
			sb.append('=');
			if(touched.contains(e)) {
				sb.append(doc.getTouchedTime(e).getTime() - doc.getFetchedTime(e).getTime());
			} else if(e.equals(doc.getCompletedBy())){
				sb.append(doc.getCompletedTime().getTime() - doc.getFetchedTime(e).getTime());
			} else {
				sb.append('?');
			}
		}
		
		return sb.toString();
	}
	
	private List<String> getAllEvents(DatabaseDocument<?> doc) {
		List<String> list = new ArrayList<String>();
		list.add(getInsertEvent(doc));
		list.addAll(getFetchedEvents(doc));
		list.addAll(getTouchedEvents(doc));
		list.add(getDoneEvent(doc));
		return list;
	}
	
	private String getDoneEvent(DatabaseDocument<?> doc) {
		StringBuffer sb = new StringBuffer();
		
		sb.append(df.format(doc.getCompletedTime()));
		sb.append(" document=");
		sb.append(doc.getID());
		sb.append(" type=done");
		sb.append(" stage=");
		sb.append(doc.getCompletedBy());
		
		sb.append(" time=");
		sb.append(doc.getCompletedTime().getTime()-doc.getFetchedTime(doc.getCompletedBy()).getTime());
		
		sb.append(" totaltime=");
		sb.append(doc.getCompletedTime().getTime()-doc.getTouchedTime(getInserter(doc)).getTime());
		
		return sb.toString();
	}

	private Collection<? extends String> getTouchedEvents(
			DatabaseDocument<?> doc) {
		ArrayList<String> list = new ArrayList<String>();
		
		for(String stage : doc.getTouchedBy()) {
			if(doc.fetchedBy(stage)) {
				list.add(getTouchEvent(doc, stage));
			}
		}
		
		return list;
	}

	private String getTouchEvent(DatabaseDocument<?> doc, String stage) {
		StringBuffer sb = new StringBuffer();
		
		sb.append(df.format(doc.getTouchedTime(stage)));
		sb.append(" document=");
		sb.append(doc.getID());
		sb.append(" type=touch");
		sb.append(" stage=");
		sb.append(stage);
		
		sb.append(" time=");
		sb.append(doc.getTouchedTime(stage).getTime() - doc.getFetchedTime(stage).getTime());
		
		return sb.toString();
	}

	private Collection<? extends String> getFetchedEvents(DatabaseDocument<?> doc) {
		ArrayList<String> list = new ArrayList<String>();
		
		for(String stage : doc.getFetchedBy()) {
			list.add(getFetchEvent(doc, stage));
		}
		
		return list;
	}

	private String getFetchEvent(DatabaseDocument<?> doc, String stage) {
		StringBuffer sb = new StringBuffer();
		
		sb.append(df.format(doc.getFetchedTime(stage)));
		sb.append(" document=");
		sb.append(doc.getID());
		sb.append(" type=fetch");
		sb.append(" stage=");
		sb.append(stage);
		
		return sb.toString();
	}

	private String getInsertEvent(DatabaseDocument<?> doc) {
		StringBuffer sb = new StringBuffer();
		String stage = getInserter(doc);
		sb.append(df.format(doc.getTouchedTime(stage)));
		sb.append(" document=");
		sb.append(doc.getID());
		sb.append(" type=insert");
		sb.append(" stage=");
		sb.append(stage);
		return sb.toString();
	}

	private String getInserter(DatabaseDocument<?> doc) {
		for(String touched : doc.getTouchedBy()) {
			if(!doc.getFetchedBy().contains(touched)) {
				return touched;
			}
		}
		return null;
	}
}
