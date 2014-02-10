package com.findwise.hydra.stage;

import com.findwise.hydra.JsonException;
import com.findwise.hydra.local.RemotePipeline;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;

public class StageServiceFactory {
	public static List<StageService> createStageServices(String stageName, String hostName, int port, boolean usePerformanceLogging, AbstractProcessStage overrideStage) throws IOException, InitFailedException, RequiredArgumentMissingException, ClassNotFoundException, InstantiationException, JsonException, IllegalAccessException {
		RemotePipeline remotePipeline = new RemotePipeline(hostName, port, stageName, usePerformanceLogging);
		AbstractProcessStage stage = (overrideStage != null) ? overrideStage : remotePipeline.getStageInstance();
		ProcessStageRunner stageRunner = new ProcessStageRunner(stageName, stage, remotePipeline);
		List<StageService> stageServices = new ArrayList<StageService>();
		for(int i = 0; i < stage.getNumberOfThreads(); i++) {
			stageServices.add(new StageService(stageName, stageRunner, stage.getQuery(), remotePipeline));
		}
		return stageServices;
	}
}
