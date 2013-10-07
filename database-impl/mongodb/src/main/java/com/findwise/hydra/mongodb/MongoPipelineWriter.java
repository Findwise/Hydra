package com.findwise.hydra.mongodb;

import java.io.IOException;
import java.io.InputStream;
import java.util.Date;
import java.util.List;

import com.findwise.hydra.Pipeline;
import com.findwise.hydra.PipelineWriter;
import com.findwise.hydra.Stage;
import com.findwise.hydra.Stage.Mode;
import com.findwise.hydra.StageGroup;
import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.WriteConcern;
import com.mongodb.WriteResult;
import com.mongodb.gridfs.GridFS;
import com.mongodb.gridfs.GridFSDBFile;
import com.mongodb.gridfs.GridFSInputFile;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MongoPipelineWriter implements PipelineWriter {

	private final Logger logger = LoggerFactory.getLogger(MongoPipelineWriter.class);

	private GridFS pipelinefs;
	private DBCollection stages;
	private MongoPipelineReader reader;
	private WriteConcern concern;
	
	public MongoPipelineWriter(MongoPipelineReader reader, WriteConcern concern) {
		this.reader = reader;
		this.concern = concern;
		pipelinefs = reader.getPipelineFS();
		stages = reader.getStagesCollection();
	}
	
	@Override
	public void prepare() {}

	@Override
	public void write(Pipeline p) throws IOException {
		Pipeline old = reader.getPipeline();
		for(Stage s : old.getStages()) {
			inactivate(s);
		}
		
		for(StageGroup g : p.getStageGroups()) {
			write(g);
		}
	}
	
	public void inactivate(Stage stage) {
		DBObject q = reader.getStageQuery(stage.getName());
		stages.findAndModify(q, new BasicDBObject("$set", new BasicDBObject(MongoPipelineReader.ACTIVE_KEY, Stage.Mode.INACTIVE.toString())));
	}
	
	private DBObject getGroupDBObject(StageGroup group) {
		BasicDBObject obj = new BasicDBObject();
		
		obj.put(MongoPipelineReader.TYPE_KEY, MongoPipelineReader.GROUP_TYPE);
		obj.put(MongoPipelineReader.NAME_KEY, group.getName());
		
		BasicDBObject props = new BasicDBObject();
		if(group.isPropertiesChanged()) {
			props.put(MongoPipelineReader.PROPERTIES_DATE_SUBKEY, new Date());
		}
		else {
			props.put(MongoPipelineReader.PROPERTIES_DATE_SUBKEY, group.getPropertiesModifiedDate());
		}
		
		props.put(MongoPipelineReader.PROPERTIES_MAP_SUBKEY, group.toPropertiesMap());

		obj.put(MongoPipelineReader.PROPERTIES_KEY, props);
		
		boolean active = false;
		boolean debug = false;
		for(Stage s : group.getStages()) {
			if(s.getMode() == Mode.ACTIVE) {
				active = true;
			} else if (s.getMode() == Mode.DEBUG) {
				debug = true;
			}
		}
		if(active) {
			obj.put(MongoPipelineReader.ACTIVE_KEY, Mode.ACTIVE.toString());
		} else if(debug) {
			obj.put(MongoPipelineReader.ACTIVE_KEY, Mode.DEBUG.toString());
		} else {
			obj.put(MongoPipelineReader.ACTIVE_KEY, Mode.INACTIVE.toString());
		}
		
		
		return obj;
		
	}
	
	private DBObject getStageDBObject(Stage s, String group) {
		BasicDBObject obj = new BasicDBObject();
		obj.put(MongoPipelineReader.STAGE_KEY, s.getName());
		obj.put(MongoPipelineReader.TYPE_KEY, MongoPipelineReader.STAGE_TYPE);
		
		BasicDBObject props = new BasicDBObject();
		if(s.isPropertiesChanged()) {
			props.put(MongoPipelineReader.PROPERTIES_DATE_SUBKEY, new Date());
		}
		else {
			props.put(MongoPipelineReader.PROPERTIES_DATE_SUBKEY, s.getPropertiesModifiedDate());
		}
		
		props.put(MongoPipelineReader.PROPERTIES_MAP_SUBKEY, s.getProperties());
		
		obj.put(MongoPipelineReader.PROPERTIES_KEY, props);
		obj.put(MongoPipelineReader.ACTIVE_KEY, s.getMode().toString());
		obj.put(MongoPipelineReader.GROUP_KEY, group);
		if(s.getDatabaseFile()!=null) {
			obj.put(MongoPipelineReader.FILE_KEY, s.getDatabaseFile().getId());
		}
		return obj;
	}
	
	public void write(StageGroup stageGroup) throws IOException {
		writeProperties(stageGroup);

		for (Stage s : stageGroup.getStages()) {
			write(s, stageGroup.getName());
		}
	}
	
	private void writeProperties(StageGroup stageGroup) {
		DBObject q = reader.getGroupQuery(stageGroup.getName());
		WriteResult result = stages.update(q, getGroupDBObject(stageGroup), true, false, concern);
		if (logger.isDebugEnabled()) {
			logger.debug("Wrote properties for group '{}', operation updated '{}' objects, got message '{}'", stageGroup.getName(), result.getN(), result);
		}
	}
	
	public void write(Stage stage, String group) throws IOException  {
		DBObject q = reader.getStageQuery(stage.getName());
		WriteResult result = stages.update(q, getStageDBObject(stage, group), true, false, concern);
		if (logger.isDebugEnabled()) {
			logger.debug("Wrote stage '{}' in group '{}', operation updated '{}' objects, got message '{}'", stage.getName(), group, result.getN(), result);
		}
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
		DBObject obj = new BasicDBObject(MongoDocument.MONGO_ID_KEY, id);
		if (pipelinefs.find(obj).size()==0) {
			return false;
		}
		pipelinefs.remove(obj);
		return true;
	}
}
