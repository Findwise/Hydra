package com.findwise.hydra.mongodb;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.when;
import junit.framework.Assert;

import org.bson.types.ObjectId;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.runners.MockitoJUnitRunner;
import org.mockito.stubbing.Answer;

import com.findwise.hydra.StatusUpdater;
import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.MongoInternalException;
import com.mongodb.WriteResult;
import com.mongodb.gridfs.GridFS;

/**
 * Contains tests for {@link MongoDocumentIO}.
 * 
 * <p>
 * This test is a part of the MongoDB-less test suite which values mocks above running
 * MongoDB instances.
 * </p>
 * 
 * @author martin.nycander
 */
@RunWith(MockitoJUnitRunner.class)
public class MongoDocumentIOMongoLessTest {

	@Mock
	private DB db;
	@Mock
	private DBCollection documents;
	@Mock
	private DBCollection oldDocuments;
	@Mock
	private StatusUpdater updater;
	@Mock
	private GridFS gridFs;

	private MongoDocumentIO documentIO;

	@Before
	public void setUp() throws Exception {
		when(db.getCollection(eq(MongoDocumentIO.DOCUMENT_COLLECTION))).thenReturn(documents);
		when(db.getCollection(eq(MongoDocumentIO.OLD_DOCUMENT_COLLECTION))).thenReturn(oldDocuments);

		documentIO = new MongoDocumentIO(db, null, 0, 0, updater, gridFs);
	}

	@Test
	public void testMarkProcessedWhenMongoIsBroken() {
		MongoDocument document = new MongoDocument();
		document.setID(new MongoDocumentID(Mockito.mock(ObjectId.class)));
		document.putContentField("body", "Very nice body.");

		DBObject mongoObject = new BasicDBObject();
		when(documents.findAndRemove(any(DBObject.class))).thenReturn(mongoObject);

		when(oldDocuments.insert(any(DBObject.class))).thenThrow(new MongoInternalException(
				"I am suck!"));

		boolean processed = documentIO.markProcessed(document, "Test stage");

		Assert.assertFalse("Processing should not be fine", processed);
		Assert.assertTrue("The document should have logged errors", document.hasErrors());
	}

	@Test
	public void testMarkProcessedOfTooLargeDocument() {
		final MongoDocument document = new MongoDocument();
		document.setID(new MongoDocumentID(Mockito.mock(ObjectId.class)));
		final String contentField = "body";
		final String contentValue = "A very looooooooooooooooooong body!";
		document.putContentField(contentField, contentValue);

		DBObject mongoObject = new BasicDBObject();
		when(documents.findAndRemove(any(DBObject.class))).thenReturn(mongoObject);

		when(oldDocuments.insert(any(DBObject.class))).thenAnswer(new Answer<WriteResult>() {
			@Override
			public WriteResult answer(InvocationOnMock invocation) throws Throwable {
				boolean theFieldIsStillTheSame = document.getContentField(contentField)
						.equals(contentValue);

				if (theFieldIsStillTheSame) {
					throw new MongoInternalException("Document is too large!");
				} else {
					return null;
				}
			}
		});

		boolean processed = documentIO.markProcessed(document, "Test stage");

		Assert.assertTrue("Processing should have removed the body field and successfully return.",
				processed);
		Assert.assertTrue("The field 'body' should be removed.",
				document.getContentField(contentField).equals("<Removed>"));
		Assert.assertTrue("The document should have logged errors", document.hasErrors());
	}

}
