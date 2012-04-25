package com.findwise.hydra;

import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.findwise.hydra.net.RESTServer;
import com.google.inject.Guice;
import com.google.inject.Injector;

public final class Main {

	private Main() {
	}

	private static Logger logger = LoggerFactory.getLogger(Main.class);

	public static void main(String[] args) {
		if (args.length > 0) {
			logger.error("Some parameters were provided on CommandLine. Ignored.");
		}

		Injector i = Guice.createInjector(new CoreModule());

		RESTServer server = i.getInstance(RESTServer.class);

		if (!server.blockingStart()) {
			if (server.hasError()) {
				logger.error("Failed to start REST server: ", server.getError());
			}
			else {
				logger.error("Failed to start REST server");
			}
			try {
				server.shutdown();
			}
			catch (IOException e2) {
				logger.error("IOException caught while shutting down REST server thread", e2);
				System.exit(1);
			}
			return;
		}

		NodeMaster nm = i.getInstance(NodeMaster.class);

		try {
			nm.blockingStart();
		}
		catch (IOException e) {
			logger.error("Unable to start nodemaster... Shutting down.");
			try {
				server.shutdown();
			}
			catch (IOException e2) {
				logger.error("IOException caught while shutting down", e2);
				System.exit(1);
			}
		}
	}

}
