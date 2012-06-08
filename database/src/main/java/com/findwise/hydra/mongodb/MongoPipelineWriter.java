package com.findwise.hydra.mongodb;

import java.io.IOException;
import java.io.InputStream;
import java.util.Date;
import java.util.List;

import org.bson.types.ObjectId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.findwise.hydra.Pipeline;
import com.findwise.hydra.PipelineWriter;
import com.findwise.hydra.Stage;
import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.WriteConcern;
import com.mongodb.gridfs.GridFS;
import com.mongodb.gridfs.GridFSDBFile;
import com.mongodb.gridfs.GridFSInputFile;

public class MongoPipelineWriter implements PipelineWriter<MongoType> {
	private GridFS pipelinefs;
	private DBCollection stages;
	private MongoPipelineReader reader;
	private WriteConcern concern;
	
	private static Logger logger = LoggerFactory.getLogger(MongoPipelineWriter.class);
	
	public MongoPipelineWriter(MongoPipelineReader reader, WriteConcern concern) {
		this.reader = reader;
		this.concern = concern;
		pipelinefs = reader.getPipelineFS();
		stages = reader.getStagesCollection();
	}
	
	@Override
	public void prepare() {}

	@Override
	public void write(Pipeline<? extends Stage> p) throws IOException {
		for(Stage s : p.getStages()) {
			inactivate(s);
		}

		for (Stage s : p.getStages()) {
			write(s);
		}
	}
	
	public void inactivate(Stage stage) {
		DBObject q = reader.getStageQuery(stage.getName());
		stages.findAndModify(q, new BasicDBObject("$set", new BasicDBObject(MongoPipelineReader.ACTIVE_KEY, Stage.Mode.INACTIVE.toString())));
	}
	
	
	
	private DBObject getPropertyDBObject(Stage s) {
		BasicDBObject obj = new BasicDBObject();
		obj.put(MongoPipelineReader.STAGE_KEY, s.getName());
		BasicDBObject props = new BasicDBObject();
		if(s.isPropertiesChanged()) {
			props.put(MongoPipelineReader.PROPERTIES_DATE_SUBKEY, new Date());
		}
		else {
			props.put(MongoPipelineReader.PROPERTIES_DATE_SUBKEY, s.getPropertiesModifiedDate());
		}
		
		props.put(MongoPipelineReader.PROPERTIES_MAP_SUBKEY, s.getProperties());
		
		obj.put(MongoPipelineReader.PROPERTIES_KEY, props);
		obj.put(MongoPipelineReader.ACTIVE_KEY, Stage.Mode.ACTIVE.toString());
		if(s.getDatabaseFile()!=null) {
			obj.put(MongoPipelineReader.FILE_KEY, s.getDatabaseFile().getId());
		}
		return obj;
	}
	
	public void write(Stage stage) throws IOException  {
		DBObject q = reader.getStageQuery(stage.getName());
		stages.update(q, getPropertyDBObject(stage), true, false, concern);
	}

	@Override
	@Deprecated
	public void removeInactiveFiles() {
		BasicDBObject query = new BasicDBObject();
		query.put(MongoPipelineReader.ACTIVE_KEY, Stage.Mode.INACTIVE.toString());
		List<GridFSDBFile> list = pipelinefs.find(query);
		for(GridFSDBFile file : list) {
			pipelinefs.remove(file);
		}
	}
	
	@Override
	public Object save(String fileName, InputStream file) {
		GridFSInputFile inputFile = pipelinefs.createFile(file, fileName);
		inputFile.save();
		return inputFile.getId();
	}
	
	@Override
	public boolean save(Object id, String fileName, InputStream file) {
		pipelinefs.remove(new BasicDBObject(MongoDocument.MONGO_ID_KEY, id));
		
		GridFSInputFile inputFile = pipelinefs.createFile(file, fileName);
		inputFile.put("_id", id);
		inputFile.save();
		return true;
	}

	@Override
	public boolean deleteFile(Object id) {
		if(id instanceof ObjectId) {
			if(pipelinefs.find((ObjectId)id)==null) {
				return false;
			}
			pipelinefs.remove((ObjectId)id);
			return true;
		}
		else {
			logger.error("Incorrect id type, should be ObjectId but got "+id.getClass());
			return false;
		}
	}
}
