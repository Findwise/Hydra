package com.findwise.hydra.admin;

import java.util.HashMap;
import java.util.Map;
import java.util.NoSuchElementException;

import com.findwise.hydra.admin.StageScanner.ParameterMeta;

public class StageInformation extends HashMap<String, Object>{
	private static final long serialVersionUID = 6044907153063140805L;

	/**
	 * @param clazz
	 * @throws NoSuchElementException if the passed Class is not annotated with com.findwise.hydra.stage.Stage
	 */
	public StageInformation(Class<?> clazz) throws NoSuchElementException {
		super();
		
		if(!StageScanner.hasStageAnnotation(clazz)) {
			throw new NoSuchElementException("No Stage-annotation found on the specified class "+clazz.getCanonicalName());
		}
		
		put("class", clazz);
		put("description", StageScanner.getStageDescription(clazz));
		
		HashMap<String, Object> map = new HashMap<String, Object>();
		
		for(ParameterMeta p : StageScanner.getParameters(clazz)) {
			map.put(p.getName(), parameterMap(p));
		}
		put("parameters", map);
	}
	
	private Map<String, Object> parameterMap(ParameterMeta p) {
		HashMap<String, Object> map = new HashMap<String, Object>();
		
		map.put("description", p.getDescription());
		map.put("required", p.isRequired());
		map.put("type", p.getType());
		return map;
	}
}
