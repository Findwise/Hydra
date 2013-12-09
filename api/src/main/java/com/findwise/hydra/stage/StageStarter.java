package com.findwise.hydra.stage;

import com.findwise.hydra.Logging;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.Service;
import com.google.common.util.concurrent.ServiceManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class StageStarter {
	private static Logger logger = LoggerFactory.getLogger(StageStarter.class);

	public static void main(String args[]) throws Exception {
		try {
			StageCommandLineArguments commandLineArguments = StageCommandLineArguments.parse(args);

			String host = commandLineArguments.getHost();
			String stageName = commandLineArguments.getStageName();
			int port = commandLineArguments.getPort();
			boolean performanceLogging = commandLineArguments.isPerformanceLogging();
			AbstractProcessStage stage = commandLineArguments.getStage();

			// TODO: Why don't we want to log remotely if args.length == 1 ?
			if( args.length > 1) {
				Logging.setup(host, commandLineArguments.getLogPort());
			}

			List<StageService> stageServices = StageServiceFactory.createStageServices(stageName, host, port, performanceLogging, stage);
			startServices(new ServiceManager(stageServices));
		} catch(Exception e) {
			logger.error("Caught exception while starting stage(s).", e);
			System.exit(1);
		}
	}

	static void startServices(final ServiceManager manager) {
		manager.addListener(new ServiceManager.Listener() {
			public void stopped() {}
			public void healthy() {}
			public void failure(Service service) {
				logger.error("Failure in " + service.toString() + ". Shutting down.", service.failureCause());
				System.exit(1);
			}
		}, MoreExecutors.sameThreadExecutor());

		Runtime.getRuntime().addShutdownHook(new Thread() {
			public void run() {
				// Give the services 5 seconds to stop to ensure that we are responsive to shutdown
				// requests.
				try {
					manager.stopAsync().awaitStopped(5, TimeUnit.SECONDS);
				} catch (TimeoutException timeout) {
					// stopping timed out
				}
			}
		});

		manager.startAsync();  // start all the services asynchronously
	}
}
