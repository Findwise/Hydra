package com.findwise.hydra.mongodb;

import java.io.InputStream;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;

import com.findwise.hydra.DatabaseFile;
import com.findwise.hydra.Pipeline;
import com.findwise.hydra.PipelineReader;
import com.findwise.hydra.Stage;
import com.findwise.hydra.Stage.Mode;
import com.findwise.hydra.StageGroup;
import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.QueryBuilder;
import com.mongodb.gridfs.GridFS;
import com.mongodb.gridfs.GridFSDBFile;

public class MongoPipelineReader implements PipelineReader<MongoType> {
	private GridFS pipelinefs;

	private DBCollection stages = null;
	
	public static final String STAGE_KEY = "stage";
	public static final String TYPE_KEY = "type";
	public static final String STAGE_TYPE = "stage";
	public static final String GROUP_TYPE = "group";
	public static final String ACTIVE_KEY = "active";
	public static final String PIPELINE_KEY = "pipeline";
	public static final String FILENAME_KEY = "filename";
	public static final String UPLOAD_DATE_KEY = "uploadDate";
	public static final String PROPERTIES_KEY = "properties";
	public static final String PROPERTIES_DATE_SUBKEY = "changed";
	public static final String PROPERTIES_MAP_SUBKEY = "map";
	public static final String FILE_KEY = "file";
	public static final String GROUP_KEY = "group";
	public static final String NAME_KEY = "name";
	
	public static final String PIPELINE_FS = "configuration";
	public static final String STAGES_COLLECTION = "stages";

	public MongoPipelineReader(DB db) {
		stages = db.getCollection(STAGES_COLLECTION);
		pipelinefs = new GridFS(db, PIPELINE_FS); 
	}

	@Override
	public Pipeline getPipeline() {
		return getPipeline(Stage.Mode.ACTIVE);
	}
	
	@Override 
	public Pipeline getDebugPipeline() {
		return getPipeline(Stage.Mode.DEBUG);
	}

	private Pipeline getPipeline(Stage.Mode mode) {
		Pipeline p = new Pipeline();
		DBCursor cursor = stages.find(new BasicDBObject(ACTIVE_KEY, mode.toString()));
		
		while(cursor.hasNext()) {
			DBObject obj = cursor.next();
			if(!obj.containsField(TYPE_KEY) || STAGE_TYPE.equals(obj.get(TYPE_KEY))) {
				p.addStage(getStage(obj));
			} else {
				addGroup(p, getGroup(obj));
			}
		}
		
		return p;
	}

	private void addGroup(Pipeline p, StageGroup g) {
		if(p.hasStageGroup(g.getName())) {
			p.getStageGroup(g.getName()).setProperties(g.toPropertiesMap());
			p.getStageGroup(g.getName()).setPropertiesModifiedDate(g.getPropertiesModifiedDate());
		} else {
			p.addGroup(g);
		}
	}
	
	@SuppressWarnings("unchecked")
	private StageGroup getGroup(DBObject dbo) {
		((DBObject)dbo.get(PROPERTIES_KEY)).get(PROPERTIES_MAP_SUBKEY);
		System.out.println("***");
		System.out.println(dbo);
		StageGroup sg = new StageGroup((String)dbo.get(NAME_KEY), ((DBObject)((DBObject)dbo.get(PROPERTIES_KEY)).get(PROPERTIES_MAP_SUBKEY)).toMap());
		sg.setPropertiesModifiedDate(sg.getPropertiesModifiedDate());
		System.out.println(sg.getName());
		System.out.println("***");
		return sg;
	}
	
	@SuppressWarnings("unchecked")
	private Stage getStage(DBObject dbo) {
		Stage stage = new Stage((String)dbo.get(STAGE_KEY), getFile(dbo.get(FILE_KEY)));
		DBObject props = (DBObject) dbo.get(PROPERTIES_KEY);
		stage.setMode(Mode.valueOf((String)dbo.get(ACTIVE_KEY)));
		if(dbo.containsField(GROUP_KEY)) {
			stage.setGroupName((String)dbo.get(GROUP_KEY));
		} else {
			stage.setGroupName(stage.getName());
		}
		stage.setPropertiesModifiedDate((Date)props.get(PROPERTIES_DATE_SUBKEY));
		DBObject properties = (DBObject) props.get(PROPERTIES_MAP_SUBKEY);
		HashMap<String, Object> map = new HashMap<String, Object>();
		if(properties!=null) {
			map.putAll(properties.toMap());
		}
		stage.setProperties(map);
		
		return stage;
	}
	
	public DBObject getStageQuery(String stage) {
		return QueryBuilder.start(MongoPipelineReader.STAGE_KEY).is(stage).get();
	}
	
	public DBObject getGroupQuery(String group) {
		return QueryBuilder.start(MongoPipelineReader.NAME_KEY).is(group).and(MongoPipelineReader.TYPE_KEY).is(MongoPipelineReader.GROUP_TYPE).get();
	}
	
	public DBCollection getStagesCollection() {
		return stages;
	}
	
	public GridFS getPipelineFS() {
		return pipelinefs;
	}
	
	@Override
	public InputStream getStream(DatabaseFile df) {
		GridFSDBFile file = pipelinefs.findOne(new BasicDBObject(MongoDocument.MONGO_ID_KEY, df.getId()));
		return file==null ? null : file.getInputStream();
	}
	
	@Override
	public InputStream getStream(String fileName) {
		return getStream(getFile(fileName));
	}

	@Override
	public List<DatabaseFile> getFiles() {
		ArrayList<DatabaseFile> list = new ArrayList<DatabaseFile>();
		
		DBCursor cursor = pipelinefs.getFileList();
		while(cursor.hasNext()) {
			DBObject dbo = cursor.next();
			list.add(getFile(dbo));
		}
		
		return list;
	}
	
	@Override
	public DatabaseFile getFile(String fileName) {
		DBObject dbo = pipelinefs.findOne(new BasicDBObject(MongoPipelineReader.FILENAME_KEY, fileName));
		return getFile(dbo);
	}
	
	private DatabaseFile getFile(Object id) {
		return getFile(pipelinefs.findOne(new BasicDBObject(MongoDocument.MONGO_ID_KEY, id)));
	}
	
	private DatabaseFile getFile(DBObject dbo) {
		if(dbo == null) {
			return null;
		}
		DatabaseFile df = new DatabaseFile();
		df.setFilename((String)dbo.get("filename"));
		df.setUploadDate((Date)dbo.get("uploadDate"));
		df.setId(dbo.get(MongoDocument.MONGO_ID_KEY));
		return df;
	}
}
