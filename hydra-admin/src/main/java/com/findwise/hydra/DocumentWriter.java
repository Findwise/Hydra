package com.findwise.hydra;

import java.io.File;
import java.io.IOException;

import com.findwise.hydra.common.DocumentFile;

/**
 * This interface covers all Write and combined Read-Write actions that can be
 * applied to a document. No method in this interface can be considered
 * side-effect free.
 * 
 * @author joel.westberg
 * 
 */
public interface DocumentWriter<T extends DatabaseType> {

	/**
	 * Gets a document and timestamps the specified field name. Adds the
	 * requirement to the query that the tag field doesn't already exist, should
	 * such a requirement not already be in place.
	 * 
	 * Also requires the document not already be marked as processed.
	 * 
	 * This method enables the caller to obtain a one-time lock on a document
	 * for any given tag.
	 * 
	 * @param query
	 * @param tag
	 */
	DatabaseDocument<T> getAndTag(DatabaseQuery<T> query, String tag);

	/**
	 * Calls getAndTagRecurring(DatabaseQuery query, String tag, int
	 * intervalMillis) with a default value specified by the implementing class.
	 */
	DatabaseDocument<T> getAndTagRecurring(DatabaseQuery<T> query, String tag);

	/**
	 * Functionally the same as getAndTag(), but instead of rejecting all
	 * documents that have been tagged by the supplied tag, it will return them,
	 * should <code>intervalMillis</code> be less than the less than the time
	 * elapsed since the document was last tagged.
	 */
	DatabaseDocument<T> getAndTagRecurring(DatabaseQuery<T> query, String tag,
			int intervalMillis);

	/**
	 * Upon returning, will have updated the document with the specified ID with
	 * a metadata tag signifying that it has been released by that tag process.
	 * This ends the 2-stage process usually performed in a pipeline to allow
	 * for chaining of stages:
	 * 
	 * getAndTagDocument() - obtains a write lock for that particular stage
	 * name, avoiding race conditions if there are more than one instance of
	 * that stage
	 * 
	 * markTouched() - marks the document with the given ID as Touched. This is
	 * a queryable condition, allowing another stage to trigger on this
	 * specifically.
	 * 
	 * @param id
	 * @param tag
	 * @return
	 */
	boolean markTouched(Object id, String tag);

	/**
	 * Indicates that the document has made it through the pipeline
	 * successfully.
	 * 
	 * After this method is called on a given document, it should not show up in
	 * normal get-operations performed by this interface.
	 * 
	 * @param d
	 *            the document to mark as processed
	 * @param stage
	 *            the name of the stage that marks this document as processed
	 */
	boolean markProcessed(DatabaseDocument<T> d, String stage);

	/**
	 * Indicates that the document has been discarded without having passed
	 * through the pipeline. A reason for this might be that it was obsoleted by
	 * a newer version before reaching output.
	 * 
	 * After this method is called on a given document, it should not show up in
	 * normal get-operations performed by this interface.
	 * 
	 * @param d
	 *            the document to discard
	 * @param stage
	 *            the name of the stage that discarded this document.
	 * @return
	 */
	boolean markDiscarded(DatabaseDocument<T> d, String stage);

	/**
	 * Indicates that this document has reached an output stage which will
	 * output the document, but hasn't written it to it's output yet. A common
	 * reason for this might be to improve efficiency by doing batch-commits to
	 * a search engine by queueing the documents internally in the output stage,
	 * marking them as Pending instead of Processed until such an action can be
	 * taken.
	 * 
	 * Functionally the same as marking a document as "processed" in regard to
	 * all other workings of Hydra, with the diagnostic benefit of knowing that
	 * "pending" documents have not been sent to the search engine, in case of a
	 * crash, improper shutdown, or similar.
	 * 
	 * @param d
	 *            the document to mark as pending
	 * @param stage
	 *            the stage marking the document
	 * @return
	 */
	boolean markPending(DatabaseDocument<T> d, String stage);

	/**
	 * Inserts a new document into the database. Will fail if the document
	 * already has a non-null ID. The document's new ID will be applied to the
	 * document.
	 * 
	 * @param d
	 *            the document to insert
	 * @return false if the document already has an id, true otherwise.
	 */
	boolean insert(DatabaseDocument<T> d);

	/**
	 * @return false if update fails
	 */
	boolean update(DatabaseDocument<T> d);

	void delete(DatabaseDocument<T> d);

	/**
	 * For use in testing
	 */
	void deleteAll();

	void write(DocumentFile df) throws IOException;
	
	/**
	 * This method will be run if the Connector determines that the Database is
	 * brand new, and needs to be set up in some way.
	 */
	void prepare();

	/**
	 * If set to <code>true</code>, documents no longer live in the database
	 * will be discarded.
	 * 
	 * Implementing classes are required to support this behavior, but may
	 * choose to disregard it if called <em>after</em> the
	 * <code>prepare()</code> method.
	 * 
	 * @param discard
	 */
	void setDiscardOld(boolean discard);
}
