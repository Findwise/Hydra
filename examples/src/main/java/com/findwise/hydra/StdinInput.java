package com.findwise.hydra;

import com.findwise.hydra.local.HttpRemotePipeline;
import org.apache.commons.lang.StringUtils;

import com.findwise.hydra.local.LocalDocument;
import com.findwise.hydra.local.RemotePipeline;
import org.slf4j.LoggerFactory;
import org.slf4j.Logger;

public class StdinInput {
    private static Logger logger = LoggerFactory.getLogger(StdinInput.class);

    /**
	 * Usage: key:value is added here;key2:second value here;key3:Third value here [...] 
	 * @param args
	 */
	public static void main(String[] args) {
		RemotePipeline rp1 = new HttpRemotePipeline("127.0.0.1", 12001, "StdinInputNode");
		LocalDocument ld = new LocalDocument();
		
		for (String tuple : StringUtils.join(args, " ").split(";")) {
			String[] keyValue = tuple.split(":");
			if (keyValue.length != 2) {
				logger.error("Wrong input format. Format is 'key:value;key2:value2 [...]'");
				logger.error("Your data was not added");
				return;
			}
			ld.putContentField(keyValue[0], keyValue[1]);
		}
		
		try {
			if (rp1.saveFull(ld)) {
				logger.info("Document added");
			}
		} catch (Exception e) {
			logger.error("Failed to write document", e);
		}
	}

	
}
