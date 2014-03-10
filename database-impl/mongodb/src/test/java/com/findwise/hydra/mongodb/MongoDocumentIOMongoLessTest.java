package com.findwise.hydra.mongodb;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.when;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

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
 * This test is a part of the MongoDB-less test suite which values mocks over running
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
	private MongoDocument document;

	@Before
	public void setUp() throws Exception {
		when(db.getCollection(eq(MongoDocumentIO.DOCUMENT_COLLECTION))).thenReturn(documents);
		when(db.getCollection(eq(MongoDocumentIO.OLD_DOCUMENT_COLLECTION))).thenReturn(oldDocuments);

		documentIO = new MongoDocumentIO(db, null, 0, 0, updater, gridFs);

		document = new MongoDocument();
		document.setID(new MongoDocumentID(Mockito.mock(ObjectId.class)));

		when(documents.findAndRemove(any(DBObject.class))).thenReturn(new BasicDBObject());
	}

	@Test
	public void testMarkProcessedWhenMongoIsBroken() {
		DBObject mongoObject = new BasicDBObject();
		when(documents.findAndRemove(any(DBObject.class))).thenReturn(mongoObject);

		// Add some content since markProcessed assumes content exists.
		document.putContentField("a", "b");

		when(oldDocuments.insert(any(DBObject.class))).thenThrow(new MongoInternalException(
				"I am suck!"));

		boolean processed = documentIO.markProcessed(document, "Test stage");

		assertFalse("Processing should not be fine", processed);
		assertTrue("The document should have logged errors", document.hasErrors());
	}

	@Test
	public void testMarkProcessedOfTooLargeDocument() {
		final String contentField = "body";
		final String contentValue = "A very looooooooooooooooooong body!";
		document.putContentField(contentField, contentValue);

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

		assertTrue("Processing should return successfully.", processed);
		assertTrue("The field 'body' should be removed.",
				document.getContentField(contentField).equals("<Removed>"));
		assertTrue("The document should have logged errors", document.hasErrors());
	}

	@Test
	public void testMarkProcessedOfTooLargeDocumentRemovesLargestField() throws Exception {
		final String shortValue = "aaa";
		final String longValue = "aaaaaaaa";
		document.putContentField("short", shortValue);
		document.putContentField("long", longValue);

		when(oldDocuments.insert(any(DBObject.class))).thenAnswer(new Answer<WriteResult>() {
			@Override
			public WriteResult answer(InvocationOnMock invocation) throws Throwable {
				boolean shortFieldIsStillTheSame = document.getContentField("short")
						.equals(shortValue);
				boolean longFieldIsStillTheSame = document.getContentField("long")
						.equals(longValue);

				if (shortFieldIsStillTheSame && longFieldIsStillTheSame) {
					throw new MongoInternalException("Document is too large!");
				} else {
					return null;
				}
			}
		});

		boolean processed = documentIO.markProcessed(document, "Test stage");

		// One field should have been removed now, and it should be the longest one.
		assertEquals(shortValue, document.getContentField("short"));
		assertEquals("<Removed>", document.getContentField("long"));
		assertTrue("Processing should return successfully.", processed);
	}

}
