package com.findwise.hydra;

import java.io.InputStream;
import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import com.mongodb.DB;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.WriteConcern;
import com.mongodb.gridfs.GridFS;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.findwise.hydra.mongodb.MongoConfiguration;
import com.findwise.hydra.mongodb.MongoConnector;
import com.findwise.hydra.mongodb.MongoDocument;
import com.findwise.hydra.mongodb.MongoDocumentIO;
import com.findwise.hydra.mongodb.MongoQuery;
import com.findwise.hydra.mongodb.MongoTailableIterator;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertThat;

@RunWith(Parameterized.class)
public class FullScaleIT {
	private final boolean useOneStageGroupPerStage;
	Logger logger = LoggerFactory.getLogger(FullScaleIT.class);

	@Parameters(name = "useOneStageGroupPerStage={0}")
	public static Iterable<Object[]> testParameters() {
		// Not the most intuitive API here.
		return Arrays.asList(
				new Object[][]{{true},{false}}
		);
	}

	public FullScaleIT(boolean useOneStageGroupPerStage) {
		this.useOneStageGroupPerStage = useOneStageGroupPerStage;
	}

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

		createPrimitivePipeline();

		// We start the core after we've inserted the stages and libraries so
		// we don't have to wait for it to poll for updates.
		core.startup();

		// Next, we add three documents with a field "externalDocId" to let us identify them
		Set<String> externalDocumentIds = createDocuments(3);

		// Now we just have to wait for all three documents to end up in the "oldDocuments" repository
		MongoTailableIterator inactiveIterator = mongoConnector.getDocumentReader().getInactiveIterator(new MongoQuery());

		Set<String> finishedDocumentIds = new HashSet<String>();
		while(!finishedDocumentIds.equals(externalDocumentIds)) {
			if(inactiveIterator.hasNext()) {
				MongoDocument finishedDocument = inactiveIterator.next();
				logger.info("Found finished document " + finishedDocument);
				// Assert that the document was successfully processed
				assertThat(finishedDocument.getStatus(), equalTo(Document.Status.PROCESSED));
				// Here we assert that we indeed have passed through the staticField stage
				assertThat((String) finishedDocument.getContentField("testField"), equalTo("Set by SetStaticFieldStage"));
				finishedDocumentIds.add((String) finishedDocument.getContentField("externalDocId"));
			} else {
				// Wait for a little while before polling again.
				Thread.sleep(100);
			}
		}
	}

	private Set<String> createDocuments(int numDocs) throws UnknownHostException {
		MongoDocumentIO mongoDocumentIO = buildMongoDocumentIO(mongoConfiguration);
		Set<String> externalDocumentIds = new HashSet<String>();
		for(int i = 0; i < numDocs; i++) {
			String externalDocId = UUID.randomUUID().toString();
			MongoDocument mongoDocument = new MongoDocument();
			mongoDocument.putContentField("externalDocId", externalDocId);
			mongoDocumentIO.insert(mongoDocument);
			externalDocumentIds.add(externalDocId);
		}
		return externalDocumentIds;
	}

	/**
	 * 	Creates a small linear pipeline
	 */
	private void createPrimitivePipeline() throws Exception {
		Map<String, Object> fieldValueMap = new HashMap<String, Object>();
		fieldValueMap.put("testField", "Set by SetStaticFieldStage");
		HashMap<String, Object> staticStageParams = new HashMap<String, Object>();
		staticStageParams.put("fieldValueMap", fieldValueMap);
		Map<String, Object> initRequiredParams = new HashMap<String, Object>();
		initRequiredParams.put("failDocumentOnProcessException", true);
		new LinearPipelineBuilder().
			addStages(
				new StageBuilder()
					.stageName("initRequired")
					.className("com.findwise.hydra.stage.InitRequiredStage")
					.libraryId("integration-test-stages-jar-with-dependencies.jar")
					.stageProperties(initRequiredParams).build(),
				new StageBuilder()
					.stageName("staticFieldSetter")
					.className("com.findwise.hydra.stage.SetStaticFieldStage")
					.libraryId("hydra-basic-stages-jar-with-dependencies.jar")
					.stageProperties(staticStageParams).build(),
				new StageBuilder()
					.stageName("nullOutput")
					.className("com.findwise.hydra.stage.NullOutputStage")
					.libraryId("integration-test-stages-jar-with-dependencies.jar").build()
			)
			.useOneStageGroupPerStage(useOneStageGroupPerStage)
			// N.B. stageGroupName is only used if useOneStageGroupPerStage is set to false
			.stageGroupName(FullScaleIT.class.getName())
			.buildAndSave(mongoConnector);
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

	private void uploadJar(String jarFileName) {
		InputStream resourceAsStream = getClass().getResourceAsStream("/" + jarFileName);
		assert(resourceAsStream != null);
		mongoConnector.getPipelineWriter().save(jarFileName, jarFileName, resourceAsStream);
	}
}
