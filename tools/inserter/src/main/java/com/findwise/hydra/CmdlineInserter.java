package com.findwise.hydra;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

import com.findwise.hydra.mongodb.MongoConfiguration;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.OptionGroup;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.UnrecognizedOptionException;
import org.bson.types.ObjectId;

import com.findwise.hydra.mongodb.MongoConnector;

public class CmdlineInserter {
	
	@SuppressWarnings("static-access")
	private static Option getPipelineOption() {
		return OptionBuilder.withArgName("pipeline").hasArg()
				.withDescription("The name of the pipeline you are modifying").withLongOpt("pipeline")
				.create("p");
	}
	
	@SuppressWarnings("static-access")
	private static Option getHelpOption() {
		return OptionBuilder.withLongOpt("help").withDescription("Print this help message").create("h");
	}
	
	@SuppressWarnings("static-access")
	private static Option getLibraryOption() {
		return OptionBuilder.withLongOpt("library").withDescription("add a library").create("l");
	}
	
	@SuppressWarnings("static-access")
	private static Option getStageOption() {
		return OptionBuilder.withLongOpt("stage").withDescription("add a stage").create("s");
	}
	
	@SuppressWarnings("static-access")
	private static Option getUserOptions() {
		return OptionBuilder.withLongOpt("user").hasArg().withDescription("the database user name").create();
	}
	
	@SuppressWarnings("static-access")
	private static Option getPasswordOptions() {
		return OptionBuilder.withLongOpt("password").hasArg().withDescription("the database user password").create();
	}
	
	@SuppressWarnings("static-access")
	private static Option getMongoOption() {
		return OptionBuilder.withLongOpt("mongo").hasArg().withDescription("the MongoDB uri of format mongodb://host:port,host:port").create("m");
	}
	
	private static Options getOptions() {
		Options options = new Options();
		options.addOption(getUserOptions());
		options.addOption(getPasswordOptions());
		options.addOption(getHelpOption());
		options.addOption(getMongoOption());
		options.addOption("a", "add", false, "performs an add. Use '-h -a' for more information");
		options.addOption("r", "remove", false, "performs a remove Use '-h -r' for more information");
		
		return options;
	}
	
	private static Options getAddOptions() {
		
		Options options = new Options();
		options.addOption(getUserOptions());
		options.addOption(getPasswordOptions());
		options.addOption(getHelpOption());
		options.addOption(getMongoOption());
		OptionGroup og = new OptionGroup();
		og.addOption(getLibraryOption());
		og.addOption(getStageOption());
		og.setRequired(true);
		options.addOptionGroup(og);
		options.addOption(getPipelineOption());

		return options;
	}
	
	@SuppressWarnings("static-access")
	private static Options getAddLibraryOptions() {
		Options options = new Options();
		options.addOption(getLibraryOption());
		options.addOption(getHelpOption());
		options.addOption(getMongoOption());
		options.addOption(getUserOptions());
		options.addOption(getPasswordOptions());
		options.addOption(OptionBuilder.withArgName("library id").hasArg()
				.withDescription("The ID of the library you wish to add. Reusing an id will overwrite.").withLongOpt("id")
				.create("i"));
		options.addOption(getPipelineOption());
		return options;
	}
		
	@SuppressWarnings("static-access")
	private static Options getAddStageOptions() {
		Options options = new Options();
		options.addOption("n", "name", true, "The name of the stage being modified");
		options.addOption("d", "debug", false, "add the stage in 'DEBUG' mode. if not specified, it is added as 'ACTIVE'");
		options.addOption(getHelpOption());
		options.addOption(OptionBuilder.withArgName("library id").hasArg()
				.withDescription("The ID of the library your stage is in").withLongOpt("id")
				.create("i"));
		options.addOption(getStageOption());
		options.addOption(getPipelineOption());
		options.addOption(getMongoOption());
		options.addOption(getUserOptions());
		options.addOption(getPasswordOptions());
		return options;
	}
	
	private static Options getRemoveOptions() {
		Options options = new Options();
		options.addOption(getHelpOption());
		OptionGroup og = new OptionGroup();
		og.addOption(getLibraryOption());
		og.addOption(getStageOption());
		og.setRequired(true);
		options.addOptionGroup(og);
		options.addOption(getPipelineOption());
		options.addOption(getMongoOption());
		options.addOption(getUserOptions());
		options.addOption(getPasswordOptions());

		return options;
	}
	
	@SuppressWarnings("static-access")
	private static Options getRemoveLibraryOptions() {
		Options options = new Options();
		options.addOption(getLibraryOption());
		options.addOption(getHelpOption());
		options.addOption(getMongoOption());
		options.addOption(getUserOptions());
		options.addOption(getPasswordOptions());
		options.addOption(OptionBuilder.withArgName("library id").hasArg()
				.withDescription("The ID of the library you wish to remove").withLongOpt("id")
				.create("i"));
		options.addOption(getPipelineOption());
		options.addOption("c", "cascade", false, "also remove all stages referencing this library");
		return options;
	}
	
	private static Options getRemoveStageOptions() {
		Options options = new Options();
		options.addOption("n", "name", true, "The name of the stage being modified");
		options.addOption(getHelpOption());
		options.addOption(getStageOption());
		options.addOption(getPipelineOption());
		options.addOption(getMongoOption());
		options.addOption(getUserOptions());
		options.addOption(getPasswordOptions());
		return options;
	}
	
	
	@SuppressWarnings("unchecked")
	private static Options getAllOptions() {
		HashSet<Option> ops = new HashSet<Option>();
		ops.addAll(getOptions().getOptions());
		ops.addAll(getAddOptions().getOptions());
		ops.addAll(getAddStageOptions().getOptions());
		ops.addAll(getAddLibraryOptions().getOptions());
		ops.addAll(getRemoveOptions().getOptions());
		ops.addAll(getRemoveLibraryOptions().getOptions());
		ops.addAll(getRemoveStageOptions().getOptions());
		Options os = new Options();
		
		for(Option o : ops) {
			os.addOption(o);
		}
		return os;
	}

	public static void main(String[] args) throws Exception {

		Options options = getAllOptions();
		CommandLineParser parser = new GnuParser();
		CommandLine cmd;
		try {
			cmd = parser.parse(options, args, false);
		} catch(UnrecognizedOptionException e) {
			System.out.println(e.getMessage());
			printUsage(parser.parse(options, args, true));
			return;
		}
		if(cmd.hasOption("h")) {
			printUsage(cmd);
			return;
		}

		if(!cmd.hasOption("p")) {
			System.out.println("No pipeline specified\n");
			printUsage(cmd);
			return;
		}

		MongoConfiguration conf = new MongoConfiguration();

		conf.setNamespace(cmd.getOptionValue("p"));

		if(cmd.hasOption("m")) {
			conf.setDatabaseUrl(cmd.getOptionValue("m"));
		}
		if(cmd.hasOption("user")) {
			conf.setDatabaseUser(cmd.getOptionValue("user"));
		}
		if(cmd.hasOption("password")) {
			conf.setDatabasePassword(cmd.getOptionValue("password"));
		}
		
		MongoConnector mdc = new MongoConnector(conf);

		mdc.connect();
		

		if (cmd.hasOption("a")) {
			add(mdc, cmd);
		}
		else if(cmd.hasOption("r")) {
			remove(mdc, cmd);
		} else {
			printUsage(cmd);
		}
	}
	
	public static void add(MongoConnector mdc, CommandLine cmd) throws URISyntaxException, IOException {
		if (cmd.hasOption("l")) {
			addLibrary(mdc, cmd);
		} 
		if (cmd.hasOption("s")) {
			addStage(mdc, cmd);
		}
		if(!cmd.hasOption("l") && !cmd.hasOption("s")) {
			System.out.println("No type specified (library or stage)");
			printUsage(cmd);
			return;
		}
	}
	
	public static void remove(MongoConnector mdc, CommandLine cmd) throws URISyntaxException, IOException {
		if (cmd.hasOption("l")) {
			removeLibrary(mdc, cmd);
		} 
		if (cmd.hasOption("s")) {
			removeStage(mdc, cmd);
		}
		if(!cmd.hasOption("l") && !cmd.hasOption("s")) {
			System.out.println("No type specified (library or stage)");
			printUsage(cmd);
			return;
		}
	}
	
	public static void removeLibrary(MongoConnector mdc, CommandLine cmd) throws IOException {
		if(!cmd.hasOption("i")) {
			System.out.println("No library id specified\n");
			printUsage(cmd);
			return;
		}
		
		
		if(cmd.hasOption("c")) {
			Pipeline pipeline = mdc.getPipelineReader().getPipeline();

			List<Stage> stagesToDelete = new ArrayList<Stage>();
			for(Stage s : pipeline.getStages()) {
				if(s.getDatabaseFile().getId().equals(cmd.getOptionValue("i"))) {
					stagesToDelete.add(s);
				}
			}
			
			for(Stage s : stagesToDelete) {
				for(StageGroup g : pipeline.getStageGroups()) {
					if(g.hasStage(s.getName())) {
						g.removeStage(s.getName());
					}
				}
			}
			
			mdc.getPipelineWriter().write(pipeline);
			System.out.println("Removed "+stagesToDelete.size()+" stages from the pipeline");
		}
		
		boolean res = mdc.getPipelineWriter().deleteFile(cmd.getOptionValue("i"));
		if(res) {
			System.out.println("Removed library file with id "+cmd.getOptionValue("i"));
		}
		else {
			System.out.println("No library file with the specified id '"+cmd.getOptionValue("i")+"' exists");
		}
		
	}
	
	public static void removeStage(MongoConnector mdc, CommandLine cmd) throws IOException {
		if(!cmd.hasOption("n")) {
			System.out.println("No stage name specified\n");
			printUsage(cmd);
			return;
		}
		String name = cmd.getOptionValue("n");
		
		Pipeline pipeline = mdc.getPipelineReader().getPipeline();

		if(pipeline.getStage(name) == null) {
			System.out.println("Specified stage '"+name+"' did not exist\n");
			return;
		}
		boolean found = false;
		for(StageGroup g : pipeline.getStageGroups()) {
			if(g.hasStage(name)) {
				g.removeStage(name);
				found = true;
				break;
			}
		}
		if(found) {
			mdc.getPipelineWriter().write(pipeline);
			System.out.println("Successfully removed stage '"+name+"'");
		} else {
			System.out.println("Unable to delete '"+name+"'. Stage did not exist.");
		}
	}
	
	private static void addStage(MongoConnector mdc, CommandLine cmd) throws IOException {
		if(!cmd.hasOption("n")) {
			System.out.println("No stage name specified\n");
			printUsage(cmd);
			return;
		}
		
		if(!cmd.hasOption("i")) {
			System.out.println("No library id specified\n");
			printUsage(cmd);
			return;
		}
		
		String name = cmd.getOptionValue("n");
		String libraryId = cmd.getOptionValue("i");
		
		boolean debug = cmd.hasOption("d");
		
		String filename;
		if(cmd.hasOption("l")) {
			if(cmd.getArgs().length<2) {
				System.out.println("Library file specified, but no stage property file was found\n");
				printUsage(cmd);
				return;
			}
			filename = cmd.getArgs()[1];
		} else if(cmd.getArgs().length>0){
			filename = cmd.getArgs()[0];
		} else {
			filename = name+".properties";
		}
		Map<String, Object> map = readPropertiesFile(filename);
		
		DatabaseFile df = new DatabaseFile();
		try {
			df.setId(new ObjectId(libraryId));
		} catch (Exception e) {
			df.setId(libraryId);
		}
		Stage s = new Stage(name, df);
		s.setProperties(map);
		if(debug) {
			s.setMode(Stage.Mode.DEBUG);
		} else {
			s.setMode(Stage.Mode.ACTIVE);
		}
		Pipeline pipeline = mdc.getPipelineReader()
				.getPipeline();
		StageGroup g = new StageGroup(s.getName());
		g.addStage(s);
		pipeline.addGroup(g);
		mdc.getPipelineWriter().write(pipeline);
		System.out.println("Added stage " + name
				+ " to the pipeline.");
	}
		
	private static void addLibrary(MongoConnector mdc, CommandLine cmd) throws FileNotFoundException, URISyntaxException {
		Object outId;
		if(cmd.getArgs().length<1) {
			System.out.println("No file specified\n");
			printUsage(cmd);
			return;
		}
		if (!cmd.hasOption("i")) {
			outId = addFile(mdc, cmd.getArgs()[0]);
		} else {
			outId = addFile(mdc, cmd.getArgs()[0], cmd.getOptionValue("i"));
		}
		if (outId != null) {
			System.out.println("Added stage library with id: " + outId);
		}
	}

	private static void printUsage(CommandLine cmd) {
		HelpFormatter hf = new HelpFormatter();

		String options = "";

		Options ops = getOptions();
		Option[] entered = cmd.getOptions();
		for (Option o : entered) {
			if (!o.getOpt().equals("h")) {
				options += "-" + o.getOpt() + " ";
				if (o.getValue() != null) {
					options += o.getValue() + " ";
				}
			}
		}
		if (cmd.hasOption("a")) {
			if (cmd.hasOption("s")) {
				ops = getAddStageOptions();
			} else if (cmd.hasOption("l")) {
				ops = getAddLibraryOptions();
			} else {
				ops = getAddOptions();
			}
		} else if (cmd.hasOption("r")) {
			if(cmd.hasOption("s")) {
				ops = getRemoveStageOptions();
			} else if (cmd.hasOption("l")) {
				ops = getRemoveLibraryOptions();
			} else {
				ops = getRemoveOptions();
			}
			return;
		}
		hf.printHelp("" + options + "[FILE [FILE]]\n", ops);
	}
	

	private static Map<String, Object> readPropertiesFile(String filename) {
		String json = "";
		try {
			json = readFileAsString(filename);
		} catch (IOException e) {
			System.err.println("Property file " + filename
					+ " could not be read");
			System.exit(-1);
		}
		try {
			return SerializationUtils.fromJson(json);
		} catch (JsonException e) {
			System.err.println("Property file " + filename
					+ " is not well formed json");
			System.exit(-1);
		}
		return null;
	}

	private static String readFileAsString(String filePath)
			throws java.io.IOException {
		StringBuffer fileData = new StringBuffer(1000);
		BufferedReader reader = new BufferedReader(new FileReader(filePath));
		char[] buf = new char[1024];
		int numRead = 0;
		while ((numRead = reader.read(buf)) != -1) {
			String readData = String.valueOf(buf, 0, numRead);
			fileData.append(readData);
			buf = new char[1024];
		}
		reader.close();
		return fileData.toString();
	}

	public static Object addFile(MongoConnector dbc, String jar)
			throws FileNotFoundException, URISyntaxException {
		return addFile(dbc, jar, null);
	}

	public static Object addFile(MongoConnector dbc, String jar, String id)
			throws FileNotFoundException, URISyntaxException {
		URL path = ClassLoader.getSystemResource(jar);
		File f;
		if (path == null) {
			f = new File(jar);
			if (!f.exists()) {
				System.out.println("Unable to locate file " + jar);
				return null;
			}
		} else {
			f = new File(path.toURI());
		}
		if (id == null) {
			return dbc.getPipelineWriter().save(f.getName(),
					new FileInputStream(f));
		}
		dbc.getPipelineWriter().save(id, f.getName(), new FileInputStream(f));
		return id;
	}
}
