package com.findwise.hydra.stage;

import com.findwise.hydra.JsonException;
import com.findwise.hydra.local.LocalDocument;
import com.findwise.hydra.local.LocalQuery;
import com.findwise.hydra.local.RemotePipeline;
import com.google.common.util.concurrent.AbstractExecutionThreadService;
import org.apache.http.ParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.BlockingQueue;

public class StageService extends AbstractExecutionThreadService {
	private static Logger logger = LoggerFactory.getLogger(StageService.class);

	private final String stageName;
	private final ProcessStageRunner stageRunner;
	private final LocalQuery query;
	private final RemotePipeline remotePipeline;

	public static final int DEFAULT_HOLD_INTERVAL = 2000;
	private long holdInterval = DEFAULT_HOLD_INTERVAL;

	public StageService(String stageName, ProcessStageRunner stageRunner, LocalQuery query, RemotePipeline remotePipeline) {
		this.stageName = stageName;
		this.stageRunner = stageRunner;
		this.query = query;
		this.remotePipeline = remotePipeline;
	}

	@Override
	protected String serviceName() {
		return stageName;
	}

	/**
	 * Fetches a document to be processed from the RemotePipeline
	 *
	 * @return A document to be processed
	 * @throws org.apache.http.ParseException
	 * @throws java.io.IOException
	 * @throws com.findwise.hydra.JsonException
	 */
	protected LocalDocument fetch() throws ParseException, IOException,
			JsonException {
		return remotePipeline.getDocument(query);
	}

	@Override
	public void run() throws Exception {
		while (isRunning()) {
			LocalDocument doc = fetch();
			if (doc == null) {
				Thread.sleep(holdInterval);
			} else {
				stageRunner.performProcessing(doc);
			}
		}
	}

	@Override
	protected void shutDown() throws Exception {
		stageRunner.shutdownProcessing();
	}
}
