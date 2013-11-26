package com.findwise.hydra.stage;

import java.util.Map;

import com.findwise.hydra.local.LocalDocument;

public abstract class AbstractMappingProcessStage extends AbstractProcessStage {
	@Parameter(required = true, description = "Map from String to String, e.g. {inField:outField}. "
		+ "A map such as {key:value} will cause this stage to perform it's action on the value of "
		+ "the field 'key' and store it into the field 'value'")
	private Map<String, String> map;
	
	@Override
	public final void process(LocalDocument doc) throws ProcessException {
		for(Map.Entry<String, String> e : map.entrySet()) {
			if(doc.hasContentField(e.getKey())) {
				processField(doc, e.getKey(), e.getValue());
			}
		}
	}
	

	@Override
	public final void init() throws RequiredArgumentMissingException, InitFailedException {
		if(map==null || map.size()==0)  {
			throw new RequiredArgumentMissingException("Required argument 'map' is missing or zero-size");
		}
		stageInit();
	}

	public abstract void stageInit() throws RequiredArgumentMissingException, InitFailedException;

	public abstract void processField(LocalDocument doc, String fromField, String toField) throws ProcessException;
}
