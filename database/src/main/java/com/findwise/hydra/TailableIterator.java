package com.findwise.hydra;

import java.util.Iterator;

public interface TailableIterator<T extends DatabaseType> extends Iterator<DatabaseDocument<T>> {
	void interrupt();
}
