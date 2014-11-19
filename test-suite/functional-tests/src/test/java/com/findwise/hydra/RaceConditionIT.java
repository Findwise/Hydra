package com.findwise.hydra;

import com.findwise.hydra.mongodb.MongoConfiguration;
import com.findwise.hydra.mongodb.MongoConnector;
import com.findwise.hydra.mongodb.MongoDocument;
import com.findwise.hydra.mongodb.MongoDocumentIO;
import com.findwise.hydra.mongodb.MongoQuery;
import com.findwise.hydra.mongodb.MongoTailableIterator;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.mongodb.DB;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.WriteConcern;
import com.mongodb.gridfs.GridFS;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;
import java.net.UnknownHostException;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

public class RaceConditionIT {

	private MongoConfiguration mongoConfiguration;
	private MongoConnector mongoConnector;
	private Main core;

	Logger logger = LoggerFactory.getLogger(RaceConditionIT.class);

	@Before
	public void setUp() throws Exception {
		mongoConfiguration = new MongoConfiguration();
		mongoConfiguration.setNamespace("hydra-test-" + getClass().getSimpleName());
		mongoConnector = new MongoConnector(mongoConfiguration);
		mongoConnector.connect();

		mongoConnector.getDB().dropDatabase();

		// Because I don't trust MongoConnector after the database has been destroyed.
		mongoConnector = new MongoConnector(mongoConfiguration);
		mongoConnector.connect();

		// Initialize core, but don't start until test wants to.
		MapConfiguration mapConfiguration = new MapConfiguration();
		mapConfiguration.setParameter(CoreConfiguration.USE_CACHE, String.valueOf(true));
		mapConfiguration.setParameter(CoreConfiguration.REST_THREAD_COUNT, String.valueOf(50));

		CoreConfiguration coreConfiguration = new CoreMapConfiguration(mongoConfiguration,
				mapConfiguration);

		core = new Main(coreConfiguration);
	}

	@After
	public void tearDown() throws Exception {
		core.shutdown();
		mongoConnector.getDB().dropDatabase();
	}

	// A reasonable setting for this timeout is unfortunately very dependent on the
	// performance of the machine running the test. Setting it very high to avoid
	// random failures on TravisCI
	@Test(timeout = 240000)
	public void testAnExtremelyParallelPipeline() throws Exception {
		// Add libraries, using the filename as the library id. These jars should
		// be on the classpath, having been copied there by maven during the "package"
		// phase.
		uploadJar("hydra-basic-stages-jar-with-dependencies.jar");
		uploadJar("integration-test-stages-jar-with-dependencies.jar");

		ParallelPipelineBuilder pipelineBuilder = new ParallelPipelineBuilder()
				.useOneStageGroupPerStage(false)
				.addStages(new StageBuilder()
								.stageName("setAction")
								.className("com.findwise.hydra.stage.SetStaticActionStage")
								.libraryId("hydra-basic-stages-jar-with-dependencies.jar")
								.stageProperties(
										Maps.newHashMap(ImmutableMap.<String, Object>of(
												"action", Document.Action.DELETE.toString())))
								.build()
				)
				.setOutputStage(new StageBuilder()
						.stageName("nullOutput")
						.className("com.findwise.hydra.stage.NullOutputStage")
						.libraryId("integration-test-stages-jar-with-dependencies.jar")
						.build())
				.stageGroupName(getClass().getName());

		int setFieldStageCount = 50;
		for (int i = 0; i < setFieldStageCount; i++) {
			Stage stage = new StageBuilder()
					.stageName("setField" + i)
					.className("com.findwise.hydra.stage.SleepyStage")
					.libraryId("integration-test-stages-jar-with-dependencies.jar")
					.stageProperties(Maps.newHashMap(ImmutableMap.<String, Object>of(
							"fieldValueMap", ImmutableMap.<String, Object>of(
									"testField" + i, String.valueOf(i)),
							"variantSleep", 1000,
							"baseSleep", 100,
							"numberOfThreads", 1)))
					.build();
			pipelineBuilder.addStages(stage);
		}

		pipelineBuilder.buildAndSave(mongoConnector);
		core.startup();

		Set<String> documents = createDocuments(100);


		MongoTailableIterator
				inactiveIterator =
				mongoConnector.getDocumentReader().getInactiveIterator(
						new MongoQuery());

		// Throw thousands of ADD documents on it, the action should always be DELETE afterwards
		int timesFailed = 0;
		while (!documents.isEmpty()) {
			if (inactiveIterator.hasNext()) {
				MongoDocument finishedDocument = inactiveIterator.next();
				logger.info("Found finished document " + finishedDocument);
				documents.remove(finishedDocument.getContentField("externalDocId"));

				assertThat(finishedDocument.getStatus(), equalTo(Document.Status.PROCESSED));
				assertThat(finishedDocument.getAction(), equalTo(Document.Action.DELETE));
				for (int i = 0; i < setFieldStageCount; i++) {
					assertTrue(finishedDocument.getTouchedBy().contains("setField" + i));
					assertTrue(finishedDocument.getFetchedBy().contains("setField" + i));
					assertTrue(finishedDocument.hasContentField("testField" + i));
					assertThat(finishedDocument.getContentField("testField" + i).toString(),
							equalTo(String.valueOf(i)));
				}
				timesFailed = 0;
			} else {
				// Wait for a little while before polling again.
				Thread.sleep(100);
				if (++timesFailed > 2) {
					break;
				}
			}
		}
		assertThat(documents.size(), equalTo(0));
	}

	private Set<String> createDocuments(int numDocs) throws UnknownHostException {
		MongoDocumentIO mongoDocumentIO = buildMongoDocumentIO(mongoConfiguration);
		Set<String> externalDocumentIds = new HashSet<String>();
		for (int i = 0; i < numDocs; i++) {
			String externalDocId = UUID.randomUUID().toString();
			MongoDocument mongoDocument = new MongoDocument();
			mongoDocument.putContentField("externalDocId", externalDocId);
			mongoDocumentIO.insert(mongoDocument);
			externalDocumentIds.add(externalDocId);
		}
		return externalDocumentIds;
	}

	private MongoDocumentIO buildMongoDocumentIO(MongoConfiguration mongoConfiguration)
			throws UnknownHostException {
		MongoClient
				mongo =
				new MongoClient(new MongoClientURI(mongoConfiguration.getDatabaseUrl()));
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

	private void uploadJar(String jarFileName) {
		InputStream resourceAsStream = getClass().getResourceAsStream("/" + jarFileName);
		assert (resourceAsStream != null);
		mongoConnector.getPipelineWriter().save(jarFileName, jarFileName, resourceAsStream);
	}
}
