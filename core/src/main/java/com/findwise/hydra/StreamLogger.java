package com.findwise.hydra;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * A thread that reads from an inputstream and logs to the logger.
 * 
 */
public class StreamLogger extends Thread {
    private Logger logger = LoggerFactory.getLogger(StreamLogger.class);

    private final BufferedReader streamReader;
    private final String stageName;


    public StreamLogger(String stageName, InputStream inputStream) {
        this.stageName = stageName;
        this.streamReader = new BufferedReader(new InputStreamReader(inputStream));
        setDaemon(true);
	}

    public void run() {
        while(true) {
            try {
                String s = this.streamReader.readLine();
                if(s == null) {
                    /* End of stream reached */
                    return;
                }
                logger.info(String.format("Received message from stage %s: %s", stageName, s));
            } catch (IOException e) {
                logger.error("Error while reading from stream. Closing.", e);
                try {
                    streamReader.close();
                } catch (IOException e1) {
                    logger.error("Got error while closing stream", e);
                }
                return;
            }
        }
    }
}
