package com.findwise.hydra.stage;

import com.findwise.hydra.local.LocalDocument;
import com.findwise.tools.Hasher;
import java.security.NoSuchAlgorithmException;
import java.util.List;

/**
 *
 * @author poserdonut
 */
@Stage(description = "Hashes the value of the given fields and " +
		"saves it in another named field. Hash is stored as a hex string.")
public class ConcatenatingHashStage extends AbstractProcessStage {
    
    @Parameter(name = "algorithm", description = "The name of the algorithm to use. " +
    		"Must be a registered java.security.Provider, see " +
    		"http://docs.oracle.com/javase/6/docs/technotes/guides/security/crypto/CryptoSpec.html#AppA")
    private String algorithm = "MD5";
    
    @Parameter(name = "fields", required = true,
    		description= "The fields that you want to concatenate and run the hash on")
    List<String> fields;
    
    @Parameter(name = "output", required = true,
    		description = "The field you want to output the hash value to")
    String output;
    
    private Hasher hasher;
    
     @Override
    public void init() throws RequiredArgumentMissingException {
        try {
            hasher = new Hasher(algorithm);
        } catch (NoSuchAlgorithmException e) {
            throw new RequiredArgumentMissingException("Specified algorithm does not exist", e);
        }
        
         if (this.output == null || this.output.length() == 0) {
             throw new RequiredArgumentMissingException("No output field configured");
         }
        
         if (this.fields == null || this.fields.isEmpty()) {
             throw new RequiredArgumentMissingException("No fields to concatenate configured");
         }
       
    }

    @Override
    public void process(LocalDocument doc) {
        StringBuilder sb = new StringBuilder();
        
        for(String field: fields) {
            if(doc.hasContentField(field))
                sb.append(doc.getContentField(field));
        }
        
        if(!sb.toString().isEmpty())
            doc.putContentField(output, hasher.hashString(sb.toString()));
    }
    
}
