package com.findwise.hydra.admin;

import com.findwise.hydra.DatabaseFile;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;

import java.util.Date;
import java.util.Map;

public class LibraryCache {

	private final Cache<String, Library> cache;

	public LibraryCache() {
		this.cache = CacheBuilder.newBuilder().build();
	}

	public boolean isValidFor(DatabaseFile databaseFile) {
		Library library = cache.getIfPresent(databaseFile.getId());
		if (library != null) {
			Date cached = library.getDatabaseFile().getUploadDate();
			Date read = databaseFile.getUploadDate();
			if (!read.after(cached)) {
				return true;
			} else {
				cache.invalidate(databaseFile.getId());
			}
		}
		return false;
	}

	public Map<String, StageInformation> getStages(DatabaseFile databaseFile) {
		Library library = cache.getIfPresent(databaseFile.getId());
		if (library != null) {
			return library.getStages();
		} else {
			return null;
		}
	}

	public void put(DatabaseFile databaseFile, Map<String, StageInformation> stages) {
		cache.put(databaseFile.getId().toString(), new Library(databaseFile, stages));
	}

	private class Library {

		private final DatabaseFile databaseFile;
		private final Map<String, StageInformation> stages;

		private Library(DatabaseFile databaseFile, Map<String, StageInformation> stages) {
			this.databaseFile = databaseFile;
			this.stages = stages;
		}

		public DatabaseFile getDatabaseFile() {
			return databaseFile;
		}

		public Map<String, StageInformation> getStages() {
			return stages;
		}
	}
}
