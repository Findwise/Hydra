package com.findwise.hydra.output;

import java.util.Date;

import com.findwise.hydra.Document;
import com.findwise.hydra.local.LocalDocument;
import com.findwise.hydra.stage.AbstractOutputStage;
import com.findwise.hydra.stage.RequiredArgumentMissingException;

/**
 * Reference implementation of an outputstage. Simply writes any document to stdout.
 * 
 * @author joel.westberg
 */
public class SystemOutputStage extends AbstractOutputStage {
	
	@Override
	public void init() throws RequiredArgumentMissingException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void output(LocalDocument d) {
		System.out.println((Document)d);
		try {
			accept(d);
		} catch (Exception e) {
		}
	}
	
	long lastWrite = 0;
	long someLimit = 1000;

	protected void notifyInternal() {
		long now = new Date().getTime();
		if ((now-lastWrite) > someLimit ) {
			System.out.println("do interesting stuff");
		}
	}
}
