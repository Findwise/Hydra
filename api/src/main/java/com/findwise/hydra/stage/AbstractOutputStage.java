package com.findwise.hydra.stage;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import com.findwise.hydra.common.Logger;
import com.findwise.hydra.local.LocalDocument;
import com.findwise.hydra.local.LocalQuery;
import com.findwise.hydra.local.RemotePipeline;
import com.findwise.hydra.stage.rules.FieldOutputRule;
import com.findwise.hydra.stage.rules.OutputRule;

/**
 * Base class for building output stages.
 * 
 * @author joel.westberg
 * @author anders.rask
 * @author simon.stenstrom
 */
public abstract class AbstractOutputStage extends AbstractProcessStage {

	private List<OutputRule> rules;


	@Override
	public void setUp(RemotePipeline rp, Map<String, Object> properties) throws IllegalArgumentException, IllegalAccessException, IOException {
		super.setUp(rp, properties);
		rules = new ArrayList<OutputRule>();
	}

	public List<OutputRule> getRules() {
		return rules;
	}

	public void addRule(OutputRule rule) {
		rules.add(rule);
	}

	public LocalQuery makeQueryFromRules() {
		LocalQuery q = new LocalQuery();
		for (OutputRule rule : rules) {
			if (rule instanceof FieldOutputRule) {
				q.requireContentFieldExists(((FieldOutputRule) rule).getField());
			}
		}
		return q;
	}

	public void process(LocalDocument document) throws ProcessException {
		Logger.debug("Processing document: " + document.getID());
		boolean rejected = false;
		for (OutputRule rule : rules) {
			if (!rule.verify(document)) {
				try {
					reject();
				} catch (IOException e) {
					throw new ProcessException(e);
				} 
				rejected = true;
			}
		}
		if (!rejected) {
			output(document);
		} else {
			Logger.debug("Document was rejected.");
		}
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
	
	private boolean reject() throws IOException {
		return getRemotePipeline().releaseLastDocument();
	}
	
	@Override
	protected void persist() {
		/*
		 * TODO: Overridden and intentionally left blank. This structure should be refactored. 
		 * Accept/reject/pending should be used instead for all OutputStages...
		 */
	}
}
