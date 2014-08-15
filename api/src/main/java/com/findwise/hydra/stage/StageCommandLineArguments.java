package com.findwise.hydra.stage;

import com.findwise.hydra.local.HttpEndpointConstants;
import com.findwise.hydra.local.RemotePipeline;

public class StageCommandLineArguments {
	private static final int STAGE_NAME_PARAM = 0;
	private static final int PIPELINE_HOST_PARAM = 1;
	private static final int PIPELINE_PORT_PARAM = 2;
	private static final int PERFORMANCE_LOG_PARAM = 3;
	private static final int LOG_PORT_PARAM = 4;
	private static final int OVERRIDE_PROPERTIES_PARAM = 5;

	private final String stageName;
	private final String host;
	private final int port;
	private final boolean performanceLogging;
	private final int logPort;

	/* @Nullable */
	private final AbstractProcessStage stage;

	private StageCommandLineArguments(String stageName, String host, int port, boolean performanceLogging, int logPort, AbstractProcessStage stage) {
		this.stageName = stageName;
		this.host = host;
		this.port = port;
		this.performanceLogging = performanceLogging;
		this.logPort = logPort;
		this.stage = stage;
	}

	public static StageCommandLineArguments parse(String[] args) throws Exception {
		if(args.length == 0) {
			throw new RequiredArgumentMissingException("Missing stage(group) name");
		}

		String stageName = args[STAGE_NAME_PARAM];
		String host = (args.length>PIPELINE_HOST_PARAM) ? args[PIPELINE_HOST_PARAM] : HttpEndpointConstants.DEFAULT_HOST;
		int port = (args.length>PIPELINE_PORT_PARAM) ? Integer.parseInt(args[PIPELINE_PORT_PARAM]) : HttpEndpointConstants.DEFAULT_PORT;
		boolean usePerformanceLogging = (args.length>PERFORMANCE_LOG_PARAM) ? Boolean.parseBoolean(args[PERFORMANCE_LOG_PARAM]) : false;
		int logPort = (args.length>LOG_PORT_PARAM) ? Integer.parseInt(args[LOG_PORT_PARAM]) : RemotePipeline.DEFAULT_LOG_PORT;
		AbstractProcessStage stageOverrideProperties = (args.length>OVERRIDE_PROPERTIES_PARAM) ? AbstractProcessStageMapper.fromJsonString(args[OVERRIDE_PROPERTIES_PARAM]) : null;

		return new StageCommandLineArguments(stageName, host, port, usePerformanceLogging, logPort, stageOverrideProperties);
	}

	public String getStageName() {
		return stageName;
	}

	// A bit of a hack here. StageStarter and GroupStarter takes the same command line
	// arguments, but in the case of GroupStarter, the first argument is the name of the stage group.
	public String getStageGroupName() {
		return stageName;
	}

	public String getHost() {
		return host;
	}

	public int getPort() {
		return port;
	}

	public boolean isPerformanceLogging() {
		return performanceLogging;
	}

	public int getLogPort() {
		return logPort;
	}

	public AbstractProcessStage getStage() {
		return stage;
	}
}
