package com.findwise.hydra.mongodb;

import com.findwise.hydra.PipelineStatus;
import com.findwise.hydra.StatusReader;
import com.findwise.hydra.StatusWriter;
import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;

public class MongoStatusIO implements StatusReader<MongoType>, StatusWriter<MongoType> {

	public static final String HYDRA_COLLECTION_NAME = "hydra";
	
	private DBCollection collection;
	
	public MongoStatusIO(DB db) {
		collection = db.getCollection(HYDRA_COLLECTION_NAME);
		collection.setObjectClass(MongoPipelineStatus.class);
	}

	@Override
	public void increment(int processed, int failed, int discarded) {
		MongoPipelineStatus mps = getStatus();
		
		if(mps==null) {
			return;
		}
		
		mps.setProcessedCount(mps.getProcessedCount()+processed);

		mps.setFailedCount(mps.getFailedCount()+failed);

		mps.setDiscardedCount(mps.getDiscardedCount()+discarded);
		
		save(mps);
	}

	@Override
	public MongoPipelineStatus getStatus() {
		return (MongoPipelineStatus) collection.findOne();
	}

	@Override
	public void save(PipelineStatus<MongoType> status) {
		MongoPipelineStatus mps = (MongoPipelineStatus) status;
		if(mps.containsField("_id")) {
			collection.save(mps);
		} else {
			collection.remove(new BasicDBObject());
			collection.insert(mps);
		}
	}

	public boolean hasStatus() {
		return collection.count()!=0;
	}
}
