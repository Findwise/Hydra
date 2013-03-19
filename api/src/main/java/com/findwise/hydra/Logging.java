package com.findwise.hydra;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.net.SocketAppender;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.net.UnknownHostException;

/**
 * User: petter.remen
 * Date: 2013-03-19
 * Time: 16:16
 */

public class Logging {
    private static LoggerContext lc = (LoggerContext) LoggerFactory.getILoggerFactory();

    private static boolean remoteLoggerActive = false;
    synchronized public static void setup(String host, int port) throws UnknownHostException {
        if(remoteLoggerActive) {return;}
        remoteLoggerActive = true;

        lc = (LoggerContext) LoggerFactory.getILoggerFactory();

        SocketAppender socketAppender = new SocketAppender(InetAddress.getByName(host), port);
        socketAppender.setContext(lc);
        socketAppender.start();

        ch.qos.logback.classic.Logger rootLogger = (ch.qos.logback.classic.Logger)getRootLogger();
        /* Default configuration will contain a stdout logger. This will remove that. */
        rootLogger.detachAndStopAllAppenders();
        rootLogger.addAppender(socketAppender);

        setInternalLoggingLevel(Level.WARN);
        setGlobalLoggingLevel(Level.TRACE);
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
