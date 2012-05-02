package com.findwise.hydra;

import org.apache.commons.lang.StringUtils;

import com.findwise.hydra.common.Logger;
import com.findwise.hydra.local.LocalDocument;
import com.findwise.hydra.local.RemotePipeline;

public class StdinInput {
	
	/**
	 * Usage: key:value is added here;key2:second value here;key3:Third value here [...] 
	 * @param args
	 */
	public static void main(String[] args) {
		RemotePipeline rp1 = new RemotePipeline("127.0.0.1", 12001, "StdinInputNode");
		LocalDocument ld = new LocalDocument();
		
		for (String tuple : StringUtils.join(args, " ").split(";")) {
			String[] keyValue = tuple.split(":");
			if (keyValue.length != 2) {
				Logger.error("Wrong input format. Format is 'key:value;key2:value2 [...]'");
				Logger.error("Your data was not added");
				return;
			}
			ld.putContentField(keyValue[0], keyValue[1]);
		}
		
		try {
			if (rp1.saveFull(ld)) {
				Logger.info("Document added");
			}
		} catch (Exception e) {
			Logger.error("Failed to write document", e);
		}
	}

	
}
