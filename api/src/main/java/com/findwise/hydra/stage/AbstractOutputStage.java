package com.findwise.hydra.stage;

import java.io.IOException;
import java.util.Map;

import com.findwise.hydra.local.LocalDocument;
import com.findwise.hydra.local.RemotePipeline;
import org.slf4j.LoggerFactory;
import org.slf4j.Logger;

/**
 * Base class for building output stages.
 * 
 * @author joel.westberg
 * @author anders.rask
 * @author simon.stenstrom
 */
public abstract class AbstractOutputStage extends AbstractProcessStage {
    Logger logger = LoggerFactory.getLogger(AbstractOutputStage.class);

    @Override
	public void setUp(RemotePipeline rp, Map<String, Object> properties)
			throws IllegalArgumentException, IllegalAccessException,
			IOException {
		super.setUp(rp, properties);
	}

	public void process(LocalDocument document) throws ProcessException {
		logger.debug("Processing document: " + document.getID());
		output(document);
	}

	/**
	 * 
	 * @param document
	 */
	public abstract void output(LocalDocument document);

	protected boolean accept(LocalDocument document) throws IOException {
		return getRemotePipeline().markProcessed(document);
	}

	protected boolean pending(LocalDocument document) throws IOException {
		return getRemotePipeline().markPending(document);
	}

	protected boolean fail(LocalDocument document) throws IOException {
		return getRemotePipeline().markFailed(document);
	}

	protected boolean fail(LocalDocument document, Throwable throwable)
			throws IOException {
		return getRemotePipeline().markFailed(document, throwable);
	}

	@Override
	protected boolean persist() {
		/*
		 * TODO: Overridden and intentionally left blank. This structure should
		 * be refactored. Accept/reject/pending should be used instead for all
		 * OutputStages...
		 */
		return true;
	}
}
