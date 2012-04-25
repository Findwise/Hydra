package com.findwise.hydra.stage;

import java.security.NoSuchAlgorithmException;

import com.findwise.hydra.local.LocalDocument;
import com.findwise.tools.Hasher;

@Stage(description = "Hashes the value of a field and saves it in another named field. Hash is stored as a hex string.")
public class HashStage extends AbstractMappingProcessStage {

	@Parameter(name = "algorithm", description = "The name of the algorithm to use. Must be a registered java.security.Provider, see http://docs.oracle.com/javase/6/docs/technotes/guides/security/crypto/CryptoSpec.html#AppA")
	private String algorithm = "MD5";
	
	private Hasher hasher;

	@Override
	public void stageInit() throws RequiredArgumentMissingException {
		try {
			hasher = new Hasher(algorithm);
		} catch (NoSuchAlgorithmException e) {
			throw new RequiredArgumentMissingException("Specified algorithm does not exist", e);
		}
	}
	
	@Override
	public void processField(LocalDocument doc, String fromField, String toField)
			throws ProcessException {
		doc.putContentField(toField, hasher.hashString(doc.getContentField(fromField).toString()));
	}
}
