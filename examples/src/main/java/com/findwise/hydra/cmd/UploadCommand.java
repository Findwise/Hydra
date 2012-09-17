package com.findwise.hydra.cmd;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import java.util.List;

/**
 * @author johan.sjoberg
 */
@Parameters(commandDescription="Tool for uploading a jar-file or a pipeline configuration to mongodb")
public class UploadCommand {

    @Parameter(names = {"-h", "--host", "--hostname"}, description = "MongoDB hostname")
    private String host = "127.0.0.1";

    @Parameter(names = {"-c", "--config"}, description = "File containing the pipeline configuration")
    private String config;

    @Parameter(names = {"-f", "--jarfile"}, arity = 3, description = "Jar file to upload followed by the pipeline name and a jar file id")
    private List<String> jarFile;

    @Parameter(names = {"-s", "--stage"}, description = "The name of the stage in the pipeline to upload. Used in combination with '-c' to only upload a subset of a pipeline")
    private List<String> stage;

    @Parameter(names = {"--help"}, description = "Prints this help text", hidden = true)
    private Boolean help = false;

    /**
     * Test if the command is valid. 
     * 
     * @param cmd command to test
     * @return true if valid, otherwise false. 
     */
    public boolean isValid() {
        if (jarFile != null && jarFile.size() == 3) {
            for (String s : jarFile) {
                if (s == null) {
                    return false;
                }
            }
            return true;
        } else if (config != null) {
            return true;
        }
        return false;
    }

    public Boolean isHelp() {
        return help;
    }

    public String getHost() {
        return host;
    }

    public JarFile getJarFile() {
        return jarFile != null 
                ? new JarFile(jarFile.get(0), jarFile.get(1), jarFile.get(2)) 
                : null;
    }

    public String getConfig() {
        return config;
    }

    public List<String> getStageNames() {
        return stage;
    }

    @Override
    public String toString() {
        return "UploadCommand{" + "host=" + host + ", config=" + config + ", jarFile=" + jarFile + ", stage=" + stage + ", help=" + help + '}';
    }
}