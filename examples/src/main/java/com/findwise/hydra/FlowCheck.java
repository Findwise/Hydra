package com.findwise.hydra;

import java.io.IOException;

import org.apache.http.HttpException;

import com.findwise.hydra.JsonException;
import com.findwise.hydra.local.RemotePipeline;

public class FlowCheck {
	
	public FlowCheck() {
	}
	
	public void start() throws IOException, JsonException, HttpException {
		postDocuments(1000);
	}
	
	public void postDocuments(int numberToPost) throws JsonException, IOException, HttpException {

		RemotePipeline rp = new RemotePipeline("insertStage");
		for(int i=0; i<numberToPost; i++) {
			rp.saveFull(LocalDocumentFactory.getRandomStringDocument("in", "id", "x", "y", "z", "a", "b", "c", "d", "e", "f"));
		}
	}
	
	public static void main(String[] args) throws Exception {
		FlowCheck fc = new FlowCheck();
		fc.start();
	}
}
