package com.findwise.hydra;

import com.findwise.hydra.DocumentLogger.Format;

public class Log {
	public static void main(String[] args) throws Exception {
		DocumentLogger dl = new DocumentLogger(Utility.getConnectorInstance(), Format.MULTI);
		dl.start();
	}
}
