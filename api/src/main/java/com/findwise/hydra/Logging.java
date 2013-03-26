package com.findwise.hydra;

import java.net.InetAddress;
import java.net.UnknownHostException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.encoder.PatternLayoutEncoder;
import ch.qos.logback.classic.net.SocketAppender;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.ConsoleAppender;

/**
 * Class responsible for setting up and controlling logging.
 * 
 * @author petter.remen 
 */
public class Logging {
	private static LoggerContext lc = (LoggerContext) LoggerFactory
			.getILoggerFactory();

	private static boolean remoteLoggerActive = false;

	synchronized public static void setup(String host, int port)
			throws UnknownHostException {
		if (remoteLoggerActive) {
			return;
		}
		remoteLoggerActive = true;

		lc = (LoggerContext) LoggerFactory.getILoggerFactory();

		SocketAppender socketAppender = new SocketAppender(
				InetAddress.getByName(host), port);
		socketAppender.setContext(lc);
		socketAppender.start();

		ch.qos.logback.classic.Logger rootLogger = getRootLogger();
		/*
		 * Default configuration will contain a stdout logger. This will remove
		 * that.
		 */
		rootLogger.detachAndStopAllAppenders();
		rootLogger.addAppender(socketAppender);

		setInternalLoggingLevel(Level.WARN);
		setGlobalLoggingLevel(Level.TRACE);
	}

	/**
	 * Adds a new ConsoleAppender to the ROOT logger.
	 */
	synchronized public static void addConsoleAppender() {
		addConsoleAppender(getRootLogger());
	}

	/**
	 * Adds a ConsoleAppender to the supplied logger.
	 * 
	 * @param logger
	 */
	synchronized public static void addConsoleAppender(
			ch.qos.logback.classic.Logger logger) {
		PatternLayoutEncoder encoder = new PatternLayoutEncoder();
		encoder.setContext(logger.getLoggerContext());
		encoder.setPattern("%d{HH:mm:ss.SSS} [%-5level] %msg %n");
		encoder.start();

		ConsoleAppender<ILoggingEvent> c = new ConsoleAppender<ILoggingEvent>();
		c.setContext(logger.getLoggerContext());
		c.setOutputStream(System.err);
		c.setEncoder(encoder);
		c.start();

		logger.addAppender(c);
	}

	public static Level getInternalLogLevel() {
		return getInternalLogger().getLevel();
	}

	public static void setInternalLoggingLevel(Level loggingLevel) {
		getInternalLogger().setLevel(loggingLevel);
	}

	public static Level getGlobalLogLevel() {
		return getRootLogger().getLevel();
	}

	public static void setGlobalLoggingLevel(Level loggingLevel) {
		getRootLogger().setLevel(loggingLevel);
	}

	private static ch.qos.logback.classic.Logger getInternalLogger() {
		return lc.getLogger("internal");
	}

	private static ch.qos.logback.classic.Logger getRootLogger() {
		return lc.getLogger(Logger.ROOT_LOGGER_NAME);
	}
}
