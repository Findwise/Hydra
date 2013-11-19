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

public final class Main implements ShutdownHandler {

	private static final long KILL_DELAY = TimeUnit.SECONDS.toMillis(30);

	private Main() {
	}

	private Logger logger = LoggerFactory.getLogger(Main.class);
	private SimpleSocketServer simpleSocketServer = null;
	private RESTServer server = null;

	private volatile boolean shuttingDown = false;
	
	public static void main(String[] args) {

		Main main = new Main();
		
		if (args.length > 1) {
			main.logger.error("Some parameters on command line were ignored.");
		}
		
		CoreConfiguration conf;
		if (args.length > 0) {
			conf = main.getConfiguration(args[0]);
		} else {
			conf = main.getConfiguration(null);
		}

		main.startup(conf);
	}

	private void startup(CoreConfiguration conf) {
		ShuttingDownOnUncaughtException uncaughtExceptionHandler = new ShuttingDownOnUncaughtException(this);
		Thread.setDefaultUncaughtExceptionHandler(uncaughtExceptionHandler);
		simpleSocketServer = new SimpleSocketServer((LoggerContext) LoggerFactory.getILoggerFactory(), conf.getLoggingPort());
		simpleSocketServer.start();

		logger.info("Hydra Core creating connector, {}='{}', {}='{}'",
				DatabaseConfiguration.DATABASE_URL_PARAM, conf.getDatabaseUrl(),
				DatabaseConfiguration.DATABASE_NAMESPACE, conf.getNamespace());
		
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
				new Pipeline(), 
				this);

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

	public void shutdown() {
		logger.info("Got shutdown request...");
		shuttingDown = true;
		killUnlessShutdownWithin(KILL_DELAY);

		if (simpleSocketServer != null) {
			try {
				simpleSocketServer.close();
			} catch (Exception e) {
				logger.debug("Caught exception while shutting down simpleSocketServer. Was it not started?", e);
			}
		} else {
			logger.trace("simpleSocketServer was null");
		}

		if (server != null) {
			try {
				server.shutdown();
				return;
			} catch (Exception e) {
				logger.debug("Caught exception while shutting down the server. Was it not started?", e);
				System.exit(1);
			}
		} else {
			logger.trace("server was null");
		}
	}

	public boolean isShuttingDown() {
		return shuttingDown;
	}

	private void killUnlessShutdownWithin(long killDelay) {
		
		if (killDelay < 0) {
			return;
		}
		HydraKiller killerThread = new HydraKiller(killDelay);
		killerThread.setDaemon(true);
		killerThread.start();
	}
	
	protected CoreConfiguration getConfiguration(String fileName) {
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

	private class ShuttingDownOnUncaughtException implements UncaughtExceptionHandler {

		private final ShutdownHandler shutdownHandler;

		public ShuttingDownOnUncaughtException(ShutdownHandler shutdownHandler) {
			this.shutdownHandler = shutdownHandler;
		}

		@Override
		public void uncaughtException(Thread t, Throwable e) {
			if (!shutdownHandler.isShuttingDown()) {
				logger.error("Got an uncaught exception. Shutting down Hydra", e);
				shutdownHandler.shutdown();
			} else {
				logger.error("Got exception while shutting down", e);
			}
		}
		
	}

	private class HydraKiller extends Thread {

		Logger logger = LoggerFactory.getLogger(HydraKiller.class);
		private final long killDelay;

		public HydraKiller(long killDelay) {
			this.killDelay = killDelay;
		}

		@Override
		public void run() {
			try {
				logger.debug("Hydra will be killed in " + killDelay + "ms unless it is shut down gracefully before then");
				Thread.sleep(killDelay);
				logger.info("Failed to shutdown hydra gracefully within configured shutdown timeout. Killing Hydra now");
				System.exit(1);
			} catch (Throwable e) {
				logger.error("Caught exception in HydraKiller thread. Killing Hydra right away!", e);
				System.exit(1);
			}
		}
	}
}
