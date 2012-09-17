package com.findwise.hydra.cmd;

/**
 * @author johan.sjoberg
 */
public class JarFile {

    private final String filename;
    private final String pipelinename;
    private final String id;

    public JarFile(String fileName, String pipelineName, String uniqueJarId) {
        this.filename = fileName;
        this.pipelinename = pipelineName;
        this.id = uniqueJarId;
    }

    /**
     * @return the jar filename
     */
    public String getFilename() {
        return filename;
    }

    /**
     * @return the unique id of the jar which can be used to reference the jar
     * file in pipeline stages
     */
    public String getId() {
        return id;
    }

    /**
     * @return name of the pipeline to store the jar file in
     */
    public String getPipelinename() {
        return pipelinename;
    }
}
