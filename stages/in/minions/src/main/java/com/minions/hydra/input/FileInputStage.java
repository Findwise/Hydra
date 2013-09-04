package com.minions.hydra.input;

import com.findwise.hydra.common.JsonException;
import com.findwise.hydra.common.Logger;
import com.findwise.hydra.local.LocalDocument;
import com.findwise.hydra.stage.AbstractInputStage;
import com.findwise.hydra.stage.Parameter;
import com.findwise.hydra.stage.RequiredArgumentMissingException;
import com.findwise.hydra.stage.Stage;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.logging.Level;

/**
 * FolderPath : folder that this stage should monitor for files
 * 
 * MonitorDelay : Delay between monitors
 * 
 * @author frederick.ceder
 * 
 */
@Stage
public class FileInputStage extends AbstractInputStage {

	//private HttpInputServer server;
	
	@Parameter(name = "MonitorPath", description = "Path of the folder to monitor")
	public String MonitorPath;
        @Parameter(name = "ProcessPath", description = "Path of the folder to put a file for processing")
	public String ProcessPath;
	@Parameter(name = "MonitorDelayValue", description = "Delay time for monitor between monitors")
	public int MonitorDelayValue;        
        
	private File InputFolder;

	@Override
	public void init() throws RequiredArgumentMissingException 
    {
		Logger.info("FolderMonitor starting.");
		
        InputFolder = new File(MonitorPath);
        boolean folderExists = InputFolder.exists();
        if(!folderExists) folderExists = InputFolder.mkdirs();
        if(!folderExists) 
        {
            Logger.error("Unknown input directory. Check settings file.");
            throw new RequiredArgumentMissingException("Unknown input directory. Check settings file.");
        }
        Logger.debug("Found inputfolder.");
        
        File processFolder = new File(ProcessPath);
        folderExists = processFolder.exists();
        if(!folderExists) folderExists = processFolder.mkdirs();
        if(!folderExists)
        {
            Logger.error("Unknown process directory. Check settings file.");
            throw new RequiredArgumentMissingException("Unknown process directory. Check settings file.");
        }
        Logger.debug("Found processfolder.");
                
                
                
		boolean Monitoring = true;
		while(Monitoring)
		{
			try
			{
				File[] files = InputFolder.listFiles();
				if(files.length > 0)
				{
                    Logger.debug("Found " + files.length + " files.");
					for(File f : files)
					{
                        File processFile = processFile = copyToProcessFolder(f);
                                                
						if(processFile != null)
                        {
                            sendToRemotePipeline(processFile);
                            Logger.debug("Sent one file " + f.getName() + " to pipeline.");
                            if(f.delete())
                                Logger.debug("Removed " + f.getPath());
                            else Logger.debug("Couldn't remove " + f.getPath());
                        }
                        else Logger.error("Couldn't copy file. Check write permissions.");
					}
				}
				Thread.sleep(MonitorDelayValue);
			} catch(InterruptedException ex) {
				Logger.debug("MonitorThread interrupted.");
				Monitoring = false;
			} catch(JsonException e) {
                Logger.error("MonitorThread: " + e.getMessage());
            } catch (IOException e) {
				Logger.error("MonitorThread: " + e.getMessage());
				e.printStackTrace();
			} 
		}
	}
	
    /**
     * Creates a localDocument from the file instance and sends it to the
     * remote pipeline
     * @param f
     * @throws IOException
     * @throws JsonException 
     */
	private void sendToRemotePipeline(File f) throws IOException, JsonException 
	{
        LocalDocument doc = new LocalDocument();
        String[] buffer = f.getName().split("\\.");
        
        if(f.getPath().contains("."))
        {
            Logger.debug("Found period in " + f.getName() + " extension is " + buffer[buffer.length -1]);
            doc.putContentField("Content-Type", buffer[buffer.length -1]);
        }
        else
        {
            Logger.debug("Unknown extension, defaulting to file.");
            doc.putContentField("Content-Type", "file");
        }
        doc.putContentField("Date", new SimpleDateFormat().format(new Date()));
        doc.putContentField("Path", f.getAbsolutePath());
        getRemotePipeline().saveFull(doc);
	}

    /**
     * Copies a file to the ProcessPath folder.
     * 
     * Then the new file instance for the processpath is returned.
     * 
     * @param inputFile
     * @return
     * @throws FileNotFoundException
     * @throws IOException 
     */
    private File copyToProcessFolder(File inputFile) throws FileNotFoundException, IOException  {        
        FileOutputStream out = null;
        File processFile = new File(ProcessPath + inputFile.getName());
        FileInputStream in = new FileInputStream(inputFile);
        
        byte[] buffer = new byte[(int)inputFile.length()];
        in.read(buffer);
        in.close();
        try {
            processFile.createNewFile();
            out = new FileOutputStream(processFile);
            out.write(buffer);
        } catch (IOException ex) {
            return null;
        } finally {
            try {
                out.close();
            } catch (IOException ex) {
                java.util.logging.Logger.getLogger(FileInputStage.class.getName()).log(Level.SEVERE, null, ex);
            }
        }
        return processFile;          
    }
}
