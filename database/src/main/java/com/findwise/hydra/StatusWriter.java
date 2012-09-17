package com.findwise.hydra;


public interface StatusWriter<T extends DatabaseType> {

	void increment(int processed, int failed, int discarded);

	void save(PipelineStatus<T> status);

}
