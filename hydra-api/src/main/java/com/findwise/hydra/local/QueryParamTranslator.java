package com.findwise.hydra.local;

import java.util.List;


public interface QueryParamTranslator {
	
	public LocalQuery createQueryFromString(String s);
	public LocalQuery createQueryFromList(List<String> properties);
	public String extractQueryOptions(LocalQuery q);
	
}
