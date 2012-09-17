package com.findwise.hydra.memorydb;

import com.findwise.hydra.PipelineStatus;
import com.findwise.hydra.StatusReader;
import com.findwise.hydra.StatusWriter;

public class MemoryStatusIO implements StatusReader<MemoryType>, StatusWriter<MemoryType> {

	
	@Override
	public void increment(int processed, int failed, int discarded) {
		
	}

	@Override
	public MemoryPipelineStatus getStatus() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void save(PipelineStatus<MemoryType> status) {
		// TODO Auto-generated method stub
		
	}

	public static class MemoryPipelineStatus implements PipelineStatus<MemoryType> {

		@Override
		public void setDiscardOldDocuments(boolean discardOld) {
			// TODO Auto-generated method stub
			
		}

		@Override
		public boolean isDiscardingOldDocuments() {
			// TODO Auto-generated method stub
			return false;
		}

		@Override
		public void setDiscardedToKeep(long numberToKeep) {
			// TODO Auto-generated method stub
			
		}

		@Override
		public long getNumberToKeep() {
			// TODO Auto-generated method stub
			return 0;
		}

		@Override
		public void setDiscardedMaxSize(int maxSize) {
			// TODO Auto-generated method stub
			
		}

		@Override
		public int getDiscardedMaxSize() {
			// TODO Auto-generated method stub
			return 0;
		}

		@Override
		public int getDiscardedCount() {
			// TODO Auto-generated method stub
			return 0;
		}

		@Override
		public void setDiscardedCount(int i) {
			// TODO Auto-generated method stub
			
		}

		@Override
		public int getProcessedCount() {
			// TODO Auto-generated method stub
			return 0;
		}

		@Override
		public void setProcessedCount(int i) {
			// TODO Auto-generated method stub
			
		}

		@Override
		public int getFailedCount() {
			// TODO Auto-generated method stub
			return 0;
		}

		@Override
		public void setFailedCount(int i) {
			// TODO Auto-generated method stub
			
		}
		
	}

	@Override
	public boolean hasStatus() {
		// TODO Auto-generated method stub
		return false;
	}
}
