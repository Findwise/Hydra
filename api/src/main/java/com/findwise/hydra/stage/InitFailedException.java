package com.findwise.hydra.stage;

public class InitFailedException extends Exception {

	private static final long serialVersionUID = 201311251624L;

	public InitFailedException() {
		super();
	}

	public InitFailedException(String message) {
		super(message);
	}

	public InitFailedException(String message, Throwable cause) {
		super(message, cause);
	}

	public InitFailedException(Throwable cause) {
		super(cause);
	}
}
