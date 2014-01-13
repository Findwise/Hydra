package com.findwise.hydra;

import com.findwise.hydra.mongodb.*;
import com.mongodb.DB;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.WriteConcern;
import com.mongodb.gridfs.GridFS;
import org.junit.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;
import java.net.UnknownHostException;
import java.util.*;

import static org.hamcrest.CoreMatchers.*;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

public class FullScaleIT {
	Logger logger = LoggerFactory.getLogger(FullScaleIT.class);
	MongoConfiguration mongoConfiguration;
	MongoConnector mongoConnector;
	private Main core;

	@Before
	public void setUp() throws Exception {
		mongoConfiguration = new MongoConfiguration();
		mongoConfiguration.setNamespace("hydra-test-FullScaleIT");
		mongoConnector = new MongoConnector(mongoConfiguration);
		mongoConnector.connect();

		mongoConnector.getDB().dropDatabase();

		// Because I don't trust MongoConnector after the database has been destroyed.
		mongoConnector = new MongoConnector(mongoConfiguration);
		mongoConnector.connect();

		// Initialize core, but don't start until test wants to.
		CoreConfiguration coreConfiguration = new CoreMapConfiguration(mongoConfiguration, new MapConfiguration());
		core = new Main(coreConfiguration);
	}

	@After
	public void tearDown() throws Exception {
		core.shutdown();
	}

	// A reasonable setting for this timeout is unfortunately very dependent on the
	// performance of the machine running the test. Setting it very high to avoid
	// random failures on TravisCI
	@Test(timeout = 60000)
	public void testAPrimitivePipelineWorks() throws Exception {
		// Add libraries, using the filename as the library id. These jars should
		// be on the classpath, having been copied there by maven during the "package"
		// phase.
		uploadJar("hydra-basic-stages-jar-with-dependencies.jar");
		uploadJar("integration-test-stages-jar-with-dependencies.jar");

		// Create a StaticFieldStage and a NullOutputStage in a single linear pipeline
		Map<String, Object> fieldValueMap = new HashMap<String, Object>();
		fieldValueMap.put("testField", "Set by SetStaticFieldStage");
		HashMap<String, Object> staticStageParams = new HashMap<String, Object>();
		staticStageParams.put("fieldValueMap", fieldValueMap);
		Map<String, Object> initRequiredParams = new HashMap<String, Object>();
		initRequiredParams.put("failDocumentOnProcessException", true);
		buildPipeline(
				new StagePrototype(
						"initRequired",
						"com.findwise.hydra.stage.InitRequiredStage",
						"integration-test-stages-jar-with-dependencies.jar",
						initRequiredParams
				),
				new StagePrototype(
						"staticFieldSetter",
						"com.findwise.hydra.stage.SetStaticFieldStage",
						"hydra-basic-stages-jar-with-dependencies.jar",
						staticStageParams
				),
				new StagePrototype(
						"nullOutput",
						"com.findwise.hydra.stage.NullOutputStage",
						"integration-test-stages-jar-with-dependencies.jar"
				)
		);

		// We start the core after we've inserted the stages and libraries so
		// we don't have to wait for it to poll for updates.
		core.startup();


		// Next, we add three documents with a field "externalDocId" to let us identify them
		MongoDocumentIO mongoDocumentIO = buildMongoDocumentIO(mongoConfiguration);

		Set<String> externalDocumentIds = new HashSet<String>(Arrays.asList("1", "2", "3"));

		for (String externalDocId : externalDocumentIds) {
			MongoDocument mongoDocument = new MongoDocument();
			mongoDocument.putContentField("externalDocId", externalDocId);
			mongoDocumentIO.insert(mongoDocument);
		}

		// Now we just have to wait for all three documents to end up in the "oldDocuments" repository
		MongoTailableIterator inactiveIterator = mongoConnector.getDocumentReader().getInactiveIterator(new MongoQuery());

		Set<String> finishedDocumentIds = new HashSet<String>();
		while(!finishedDocumentIds.equals(externalDocumentIds)) {
			if(inactiveIterator.hasNext()) {
				MongoDocument finishedDocument = inactiveIterator.next();
				logger.info("Found finished document " + finishedDocument);
				// Assert that the document was successfully processed
				assertEquals(Document.Status.PROCESSED, finishedDocument.getStatus());
				// Here we assert that we indeed have passed through the staticField stage
				assertThat((String) finishedDocument.getContentField("testField"), equalTo("Set by SetStaticFieldStage"));
				finishedDocumentIds.add((String) finishedDocument.getContentField("externalDocId"));
			} else {
				// Wait for a little while before polling again.
				Thread.sleep(100);
			}
		}
	}

	private MongoDocumentIO buildMongoDocumentIO(MongoConfiguration mongoConfiguration) throws UnknownHostException {
		MongoClient mongo = new MongoClient(new MongoClientURI(mongoConfiguration.getDatabaseUrl()));
		DB db = mongo.getDB(mongoConfiguration.getNamespace());
		WriteConcern concern = mongo.getWriteConcern();
		long documentsToKeep = mongoConfiguration.getOldMaxCount();
		int oldDocsMaxSizeMB = mongoConfiguration.getOldMaxSize();
		StatusUpdater updater = new StatusUpdater(new MongoConnector(mongoConfiguration));
		GridFS documentFs = new GridFS(db);

		MongoDocumentIO io = new MongoDocumentIO(db, concern, documentsToKeep,
			oldDocsMaxSizeMB, updater, documentFs);
		io.prepare();
		return io;
	}

	/**
	 * Builds a pipeline out of an array of StageProtypes.
	 *
	 * This will set the "query" : { "touched" : ... } correctly.
	 */
	private void buildPipeline(StagePrototype... stagePrototypes) throws Exception {
		String previousStageName = null;
		for (StagePrototype stagePrototype : stagePrototypes) {
			if(previousStageName != null) {
				stagePrototype.setTouchedBy(previousStageName);
			}
			addStage(stagePrototype.getStageName(), stagePrototype.getLibraryId(), stagePrototype.getStageProperties());
			previousStageName = stagePrototype.getStageName();
		}
	}

	public void addStage(String stageName, String libraryId, Map<String, Object> stageProperties) throws Exception {
		DatabaseFile df = new DatabaseFile();
		df.setId(libraryId);
		Stage s = new Stage(stageName, df);
		s.setProperties(stageProperties);
		Pipeline pipeline = mongoConnector.getPipelineReader().getPipeline();
		StageGroup g = new StageGroup(s.getName());
		g.addStage(s);
		pipeline.addGroup(g);
		mongoConnector.getPipelineWriter().write(pipeline);
	}

	private void uploadJar(String jarFileName) {
		InputStream resourceAsStream = getClass().getResourceAsStream("/" + jarFileName);
		assert(resourceAsStream != null);
		mongoConnector.getPipelineWriter().save(jarFileName, jarFileName, resourceAsStream);
	}

	private static class StagePrototype {
		private final String stageName;
		private final String libraryId;
		private final Map<String, Object> stageProperties;

		private StagePrototype(String stageName, String className, String libraryId) {
			this(stageName, className, libraryId, new HashMap<String, Object>());
		}

		private StagePrototype(String stageName, String className, String libraryId, Map<String, Object> stageProperties) {
			this.stageName = stageName;
			this.libraryId = libraryId;
			this.stageProperties = stageProperties;
			stageProperties.put("stageClass", className);
		}

		public void setTouchedBy(String previousStageName) {
			Map<String, Object> touchedParams = new HashMap<String, Object>();
			touchedParams.put(previousStageName, true);
			Map<String, Object> queryParams = new HashMap<String, Object>();
			queryParams.put("touched", touchedParams);
			stageProperties.put("query", queryParams);
		}

		private String getStageName() {
			return stageName;
		}

		private String getLibraryId() {
			return libraryId;
		}

		private Map<String, Object> getStageProperties() {
			return stageProperties;
		}
	}
}
