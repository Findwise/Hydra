package com.findwise.hydra;

import java.io.IOException;
import java.lang.Thread.UncaughtExceptionHandler;
import java.util.concurrent.TimeUnit;

import org.apache.commons.configuration.ConfigurationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.net.SimpleSocketServer;

import com.findwise.hydra.mongodb.MongoConnector;
import com.findwise.hydra.mongodb.MongoType;
import com.findwise.hydra.net.HttpRESTHandler;
import com.findwise.hydra.net.RESTServer;

public final class Main {

	private Main() {
	}

	private static Logger logger = LoggerFactory.getLogger(Main.class);
	private static SimpleSocketServer simpleSocketServer = null;
	private static RESTServer server = null;

	private static boolean shuttingDown = false;

	private static UncaughtExceptionHandler uncaughtExceptionHandler = new UncaughtExceptionHandler() {
		@Override
		public void uncaughtException(Thread t, Throwable e) {
			if (!shuttingDown) {
				logger.error("Got an uncaught exception. Shutting down Hydra", e);
				shutdown();
			} else {
				logger.error("Got exception while shutting down", e);
			}
		}
	};
	
	public static void main(String[] args) {
		if (args.length > 1) {
			logger.error("Some parameters on command line were ignored.");
		}

		Thread.setDefaultUncaughtExceptionHandler(uncaughtExceptionHandler);
		
		CoreConfiguration conf;
		if (args.length > 0) {
			conf = getConfiguration(args[0]);
		} else {
			conf = getConfiguration(null);
		}

        startup(conf);
	}

	private static void startup(CoreConfiguration conf) {
		simpleSocketServer = new SimpleSocketServer((LoggerContext) LoggerFactory.getILoggerFactory(), conf.getLoggingPort());
		simpleSocketServer.start();

		DatabaseConnector<MongoType> backing = new MongoConnector(conf);
		try {
			backing.connect();
		} catch (IOException e) {
			logger.error("Unable to start", e);
			return;
		}

		Cache<MongoType> cache;
		if (conf.isCacheEnabled()) {
			cache = new MemoryCache<MongoType>();
		} else {
			cache = new NoopCache<MongoType>();
		}

		CachingDocumentNIO<MongoType> caching = new CachingDocumentNIO<MongoType>(
				backing, 
				cache, 
				conf.isCacheEnabled(), 
				conf.getCacheTimeout());

		NodeMaster<MongoType> nm = new NodeMaster<MongoType>(
				conf, 
				caching,
				new Pipeline());

		server = new RESTServer(conf,
				new HttpRESTHandler<MongoType>(
						nm.getDocumentIO(),
						backing.getPipelineReader(), 
						null,
						conf.isPerformanceLogging()));

		if (!server.blockingStart()) {
			if (server.hasError()) {
				logger.error("Failed to start REST server: ", server.getError());
			} else {
				logger.error("Failed to start REST server");
			}
			
			shutdown();
		}

		try {
			nm.blockingStart();
		} catch (IOException e) {
			logger.error("Unable to start nodemaster... Shutting down.");
			shutdown();
		}
	}

	private static void shutdown() {
		logger.info("Got shutdown request...");
		shuttingDown = true;
		long killDelay = TimeUnit.SECONDS.toMillis(30);
		killUnlessShutdownWithin(killDelay);

		if (simpleSocketServer != null) {
			try {
				simpleSocketServer.close();
			} catch (Exception e) {
				logger.debug("Caught exception while shuttin down simpleSocketSserver. Was is not started?", e);
			}
		} else {
			logger.trace("simpleSocketServer was null");
		}

		if (server != null) {
			try {
				server.shutdown();
				return;
			} catch (Exception e) {
				logger.debug("Caught exception while shuttin down the server. Was it not started?", e);
				System.exit(1);
			}
		} else {
			logger.trace("server was null");
		}		
	}

	private static void killUnlessShutdownWithin(long killDelay) {

		if (killDelay < 0) {
			return;
		}
		
		class HydraKiller extends Thread {

			private final long killDelay;

			public HydraKiller(long killDelay) {
				this.killDelay = killDelay;
			}

			@Override
			public void run() {
				try {
					logger.debug("Hydra will be killed in " + killDelay + "ms unless it is shut down gracefully untill then");
					Thread.sleep(killDelay);
					logger.info("Failed to shutdown hydra gracefully withing configured shutdown timout. Killing Hydra now");
					System.exit(1);
				} catch (Throwable e) {
					logger.error("Caught exception in HydraKiller thread. Killing Hydra right away!", e);
					System.exit(1);
				}
			}
		}
		
		HydraKiller killerThread = new HydraKiller(killDelay);
		killerThread.setDaemon(true);
		killerThread.start();
	}

	
	public static boolean isShuttingDown() {
		return shuttingDown;
	}
	
	protected static CoreConfiguration getConfiguration(String fileName) {
		try {
			if (fileName != null) {
				return new FileConfiguration(fileName);
			} else {
				return new FileConfiguration();
			}
		} catch (ConfigurationException e) {
			logger.error("Unable to read configuration", e);
			return null;
		}
	}

}
