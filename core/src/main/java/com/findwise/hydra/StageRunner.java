package com.findwise.hydra;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.exec.CommandLine;
import org.apache.commons.exec.DefaultExecuteResultHandler;
import org.apache.commons.exec.DefaultExecutor;
import org.apache.commons.exec.ExecuteException;
import org.apache.commons.exec.Executor;
import org.apache.commons.exec.ProcessDestroyer;
import org.apache.commons.exec.PumpStreamHandler;
import org.apache.commons.exec.launcher.CommandLauncher;
import org.apache.commons.exec.launcher.CommandLauncherFactory;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StageRunner extends Thread {

    private StageGroup stageGroup;
    private Logger logger = LoggerFactory.getLogger(getClass());
    private StageDestroyer stageDestroyer;
    private boolean prepared = false;
    private int timesToRetry = -1;
    private int timesStarted;
    private int pipelinePort;
    private boolean loggingEnabled = true;
    private List<File> files = null;
    private String jvmParameters = null;
    private String startupArgsString = null;
    private String classPathString = null;
    private String java = "java";
    private boolean hasQueried = false;
    private File targetDirectory;
    private File baseDirectory;
    
    private boolean wasKilled = false;;

    public synchronized void setHasQueried() {
        hasQueried = true;
    }

    public synchronized boolean hasQueried() {
        return hasQueried;
    }

    public StageRunner(StageGroup stageGroup, File baseDirectory, int pipelinePort) {
        this.stageGroup = stageGroup;
        this.baseDirectory = baseDirectory;
        this.targetDirectory = new File(baseDirectory, stageGroup.getName());
        this.pipelinePort = pipelinePort;
        timesStarted = 0;
    }
    
    /**
     * This method must be called prior to a call to start()
     * @throws IOException
     */
    public void prepare() throws IOException {
        files = new ArrayList<File>();
        
        
    	if((!baseDirectory.isDirectory() && !baseDirectory.mkdir()) || 
    			(!targetDirectory.isDirectory() && !targetDirectory.mkdir())) {
    		throw new IOException("Unable to write files, target ("+targetDirectory.getAbsolutePath()+") is not a directory");
    	}
    	
    	for(DatabaseFile df : stageGroup.getDatabaseFiles()) {
    		File f = new File(targetDirectory, df.getFilename());
    		files.add(f);
    		IOUtils.copy(df.getInputStream(), new FileOutputStream(f));
    	}

        stageDestroyer = new StageDestroyer();
        
    	setParameters(stageGroup.toPropertiesMap());
        if(stageGroup.getStages().size()==1) {
        	//If there is only a single stage in this group, it's configuration takes precedent
        	setParameters(stageGroup.getStages().iterator().next().getProperties());
        };
        
        prepared = true;
    }

    public final void setParameters(Map<String, Object> conf) {
        if (conf.containsKey(StageGroup.JVM_PARAMETERS_KEY) && conf.get(StageGroup.JVM_PARAMETERS_KEY)!=null) {
            jvmParameters = (String) conf.get(StageGroup.JVM_PARAMETERS_KEY);
        } else {
            jvmParameters = null;
        }

        if (conf.containsKey(StageGroup.JAVA_LOCATION_KEY) && conf.get(StageGroup.JAVA_LOCATION_KEY)!=null) {
            java = (String) conf.get(StageGroup.JAVA_LOCATION_KEY);
        } else {
            java = "java";
        }
        if (conf.containsKey(StageGroup.RETRIES_KEY) && conf.get(StageGroup.RETRIES_KEY)!=null) {
            timesToRetry = (Integer) conf.get(StageGroup.RETRIES_KEY);
        } else {
            timesToRetry = -1;
        }
        if (conf.containsKey(StageGroup.LOGGING_KEY) && conf.get(StageGroup.LOGGING_KEY)!=null) {
            loggingEnabled = (Boolean) conf.get(StageGroup.LOGGING_KEY);
        } else {
            loggingEnabled = true;
        }
    }

    public void run() {
    	if(!prepared) {
    		logger.error("The StageRunner was not prepared prior to being started. Aborting!");
    		return;
    	}

        do {
            logger.info("Starting stage group " + stageGroup.getName()
                    + ". Times started so far: " + timesStarted);
            timesStarted++;
            boolean cleanShutdown = runGroup();
            if (cleanShutdown) {
                return;
            }
            if (!hasQueried()) {
                logger.error("The stage group " + stageGroup.getName() + " did not start. It will not be restarted until configuration changes.");
                return;
            }
        } while (timesToRetry == -1 || timesToRetry >= timesStarted);

        logger.error("Stage group " + stageGroup.getName()
                + " has failed and cannot be restarted. ");
    }

    public void printJavaVersion() {
        CommandLine cmdLine = new CommandLine("java");
        cmdLine.addArgument("-version");
        DefaultExecuteResultHandler resultHandler = new DefaultExecuteResultHandler();
        Executor executor = new DefaultExecutor();
        try {
            executor.execute(cmdLine, resultHandler);
        } catch (ExecuteException e) {
            throw new RuntimeException(e);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Launches this stage and waits for process termination.
     * 
     * @return true if the stage was killed by a call to the destroy()-method. false otherwise.
     */
    private boolean runGroup() {
		CommandLine cmdLine = new CommandLine(java);
        cmdLine.addArgument(jvmParameters, false);
        if(classPathString==null) {
        	classPathString = targetDirectory.getAbsolutePath()+File.separator+"*";
        } else {
        	classPathString = classPathString+":"+targetDirectory.getAbsolutePath()+File.separator+"*";
        }
        
        cmdLine.addArgument("-cp");
        cmdLine.addArgument("${classpath}");
        cmdLine.addArgument("-jar");
        cmdLine.addArgument("${file}");
        cmdLine.addArgument(stageGroup.getName());
        cmdLine.addArgument("localhost");
        cmdLine.addArgument("" + pipelinePort);
        cmdLine.addArgument(startupArgsString);
        
        HashMap<String, Object> map = new HashMap<String, Object>();
        map.put("file", files.get(0)); //Any of the files should do as a starting point
        map.put("classpath", classPathString);
        
        cmdLine.setSubstitutionMap(map);
        logger.info("Launching with command " + cmdLine.toString());
        
        CommandLauncher cl = CommandLauncherFactory.createVMLauncher();
        
        int exitValue = 0;
        
        try {
            Process p = cl.exec(cmdLine, null);

            if (loggingEnabled) {
                PumpStreamHandler streams = new PumpStreamHandler(new StreamLogger(stageGroup.getName()));
                streams.setProcessInputStream(p.getOutputStream());
                streams.setProcessErrorStream(p.getErrorStream());
                streams.setProcessOutputStream(p.getInputStream());
                streams.start();
            }
            stageDestroyer.add(p);
            
            exitValue = p.waitFor();

        } catch (InterruptedException e) {
			throw new IllegalStateException("Caught Interrupt while waiting for process exit", e);
		} catch (IOException e) {
			logger.error("Caught IOException while running command", e);
			return false;
		}
		
		if(!wasKilled) {
	        logger.error("Stage group " + stageGroup.getName()
	                + " terminated unexpectedly with exit value " + exitValue);
			return false;
		}
        return true;
    }

    /**
     * Destroys the JVM running this stage and removes it's working files. 
     * Should a JVM shutdown fail, it will throw an IllegalStateException.
     */
    public void destroy() {
        logger.debug("Attempting to destroy JVM running stage group "
                + stageGroup.getName());
        boolean success = stageDestroyer.killAll();
        if (success) {
            logger.debug("... destruction successful");
        } else {
            logger.error("JVM running stage group " + stageGroup.getName()
                    + " was not killed");
            throw new IllegalStateException("Orphaned process for "
                    + stageGroup.getName());
        }
        
        removeFiles();
        
        wasKilled = true;
    }
    
    private void removeFiles() {
    	long start = System.currentTimeMillis();
    	IOException ex = null;
    	do {
    		try {
        		FileUtils.deleteDirectory(targetDirectory);
        		return;
        	} catch(IOException e) {
        		ex = e;
        		try {
					Thread.sleep(50);
				} catch (InterruptedException e1) {
					logger.error("Interrupted while waiting on delete");
					Thread.currentThread().interrupt();
					return;
				}
        	}
		} while (start + 5000 > System.currentTimeMillis());
		logger.error("Unable to delete the directory "
				+ targetDirectory.getAbsolutePath()
				+ ", containing Stage Group " + stageGroup.getName(), ex);
	}
    
    public StageGroup getStageGroup() {
    	return stageGroup;
    }
    
    /**
     * Manages the destruction of any Stages launched in this wrapper.
     * Automatically binds to the Runtime to shut down along with the master
     * JVM.
     * 
     * @author joel.westberg
     * 
     */
    static class StageDestroyer extends Thread implements ProcessDestroyer {

        private final List<Process> processes = new ArrayList<Process>();

        @Override
        public boolean add(Process p) {
            /**
             * Register this destroyer to Runtime in order to avoid orphaned
             * processes if the main JVM dies
             */
            Runtime.getRuntime().addShutdownHook(this);
            processes.add(p);
            return true;
        }

        @Override
        public boolean remove(Process p) {
            return processes.remove(p);
        }

        @Override
        public int size() {
            return processes.size();
        }

        /**
         * Invoked by the runtime ShutdownHook on JVM exit
         */
        public void run() {
            killAll();
        }

        public boolean killAll() {
            boolean success = true;
            synchronized (processes) {
                for (Process process : processes) {
                    try {
                        process.destroy();
                    } catch (RuntimeException t) {
                        success = false;
                    }
                }
            }
            return success;
        }
    }

	public void setStageDestroyer(StageDestroyer sd) {
		stageDestroyer = sd;
	}
}
