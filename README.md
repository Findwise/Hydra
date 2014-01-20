Hydra Processing Framework
==========================

Overview: [findwise.github.com/Hydra](http://findwise.github.com/Hydra/)

Mailing list/group: [hydra-processing google group](https://groups.google.com/forum/#!forum/hydra-processing)

Current snapshot: [![Build Status](https://secure.travis-ci.org/Findwise/Hydra.png?branch=master)](https://travis-ci.org/Findwise/Hydra)

This readme uses Google Analytics for basic visit statistics thanks to ga-beacon: [![Analytics](https://ga-beacon.appspot.com/UA-45204268-3/Hydra/readme)](https://github.com/igrigorik/ga-beacon)

What you'll need
----------------

* [MongoDB](http://www.mongodb.org/downloads) 

You'll need a database as the central node for Hydra. Currently, the only supported database is MongoDB. Simply install and start MongoDB.

To get output from Hydra, the following systems are supported:

* [Solr](http://lucene.apache.org/solr/)

* [elasticsearch](http://www.elasticsearch.org/)

Check [stages/out](https://github.com/Findwise/Hydra/tree/master/stages/out) to see the implemented outputs.
You can easily write your own output, as well!

Starting Hydra
--------------

You have two alternatives, either build Hydra yourself to get the very latest features, or download the latest released (and stable).
### Getting and starting core
#### Download Hydra

You can find the latest released build on [the download page](http://findwise.github.io/Hydra/#downloads). The main executable is called Hydra Core.

#### Building Hydra 

If you want to get involved in development of the framework, or simply want to try out the latest new features, you will need to build it yourself. Building Hydra, however, is very simple. 

There are a few pre-requisites: Hydra is built with Maven, and as such you will need to install and set up [maven](http://maven.apache.org). You will also need to have a MongoDB instance running for some of the integration tests to pass.

1. Clone the repository.
2. In the root directory, run `mvn clean install`
3. To start, run `java -jar hydra-core.jar` from inside the `distribution/bin` directory

Using Hydra
-----------

Once you have a running pipeline system, you can now start adding some some stages to it. For the most basic pipeline, you'll want to load at least two stage libraries into Hydra: `basic-stages` and `solr-out` (if you are connecting Hydra to send documents to Solr).

You'll find the projects to build these two (using Maven) in `stages/processing/basic` and `stages/output/solr`.

There are a few different methods for setting up pipelines in Hydra:

* Script your pipeline setup with the `database-impl/mongodb` package
* Set up the Admin Service webapp and configure the pipeline using its REST interface (see the readme file in the `admin-service` package)
* Use the `CmdlineInserter`-class that you can find in the `tools` project under the Hydra root

Using the CmdlineInserter, if you run `mvn clean install` on that project, you will get a runnable jar that you can use (`java -jar hydra-inserter.jar`) Below, we'll assume that's the method you are using. 

### Inserting a library into Hydra
When inserting the library, you will need to provide the name of the jar and an ID that uniquely identifies this library. Should you give an ID that already exists, the old library will be overwritten and *any pipeline stages being run from it will be restarted.*

Run the CmdlineInserter class (or the jar) with the following arguments:
	`-a -p pipeline -l -i {my-library-id} {my-jar-with-dependencies.jar}` 

### Configuring a stage in Hydra

Now that you have a library inserted, you can add your stage by referencing the library id. 

#### The configuration
In order to configure a stage, you'll need to know what stage it is you want to configure. A configuration for a SetStaticField-stage might look like this:

```

	{
		stageClass: "com.findwise.hydra.stage.SetStaticFieldStage",
		query: {"touched" : {"extractTitles" : true}, "exists" : {"source" : true} },
		fieldValueMap: {"source": ["value1", "value2"] }
	}

```

* __stageClass__: *Required*. Must be the full name of the stage class to be configured. 
* __query__: A serialized version of the query, that all documents this stage receives must match. In this example, all documents received by this stage will have already been processed by a stage called _extractTitles_ and they all have a field called _source_. See below for more information about the query syntax.
* __fieldValueMap__: A parameter expected by this stage. Parameters can be a number of different types and are used for stage-specific configuration. This parameter is a map from field names to objects (JSON arrays are converted to lists).

Save the configuration in a file somewhere on disk, e.g. {mystage.properties}, for ease of use. 

The possible configuration parameteras for each stage can be found using the `admin-service` web application. Try the `/libraries` endpoint to see all the stages of all your inserted libraries and their configurable parameters.

##### Query syntax
The `query` in the configuration is used to decide what documents the stage should make changes to. A document is picked up by the stage only if it matched *all* clauses in the query. The available options are:

* { "touched" : { "stageId" : true/false } } - Matches if a documents has (or has not if value is `false`) been processed by the stage with id `stageId`
* { "exists" : { "fieldName" : true/false } } - Matches if the document has (or has not if value is `false`) a field called `fieldName`
* { "equals" : { "fieldName" : fieldValue} } - Matches if the document has a field called `fieldName` with the value `fieldValue` (where `fieldValue` may be any object)
* { "notEquals" : { "fieldName" : fieldValue} } - Matches whenever equlas is not matching
* { "action" : "ADD"/"UPDATE"/"DELETE" } - Matches if the action of the document is set to `ADD`, `UPDATE` or `DELETE`

#### Inserting the configuration

Run the CmdlineInserter class (or the jar) with the following arguments:
	`-a -p pipeline -s -i {my-library-id} -n {my-stage-name} {mystage.properties}` 
	
That's it. You now have a pipeline configured with a SetStaticField-stage. If your Hydra Core was running while you configured this, you'll notice that it picked up the change and launched the stage with the properties you provided. Any subsequent changes to the configuration will also make Hydra Core restart the stage.

Next, you'll probably want to add an output stage, and then start pushing some documents in!

#### Inserting documents

There are a couple of way to import your documents into Hydra. The easiest way would be to post a document to the hydra admin-service (see documentation in the `admin-service` package). Adding documents this way is however not recommended in a production environment. Instead, create your own input connector using the `hydra-mongodb` package in `database-impl`. 

Basically, an input connector pushes data directly to mongodb by creating an instance of `MongoDocumentIO` and calling its insert method:

```

	...
	MongoDocumentIO io = new MongoDocumentIO(db, concern, documentsToKeep, oldDocsMaxSizeMB, updater, documentFs);
	io.prepare();

	MongoDocument document = new MongoDocument(jsonDocument);
	io.insert(document);
	...

```

A `StdinInput`connector can be found in the `stages/debugging` package, and can be used as a reference implementation.

## Setting up a demo pipeline

To set up a pipeline, we want to add *stage libraries* containing stages, *stage configuration* to instruct Hydra how the stages should be run, and a *document* to process.

Start the mongo deamon (mongod), in your `mongodb/bin` folder.

Get hold of the following jars, either by building Hydra or downloading from https://github.com/Findwise/Hydra/releases

* Hydra Core: `hydra-core.jar`
* Hydra Inserter (CmdLineInserter): `hydra-inserter.jar`
* Stage library - Basic: `basic-jar-with-dependencies.jar`
* Stage library - Debugging: `debugging-jar-with-dependencies.jar`

Place the jars in a folder and enter it.

Insert the libraries to hydra:

* Basic stages as library "basic": `java -jar hydra-inserter.jar --add --pipeline pipeline --library --id basic basic-jar-with-dependencies.jar`
* Debugging stages as library "debug": `java -jar hydra-inserter.jar --add --pipeline pipeline --library --id debug debugging-jar-with-dependencies.jar`

You've now added the stage libraries `basic` and `debug` to the pipeline `pipeline`. The IDs given are used when setting up your stages, to tell Hydra where it should look for the stage class. They can be anything you want.

Create configuration files:

* Create a file called `setTitleStage.json` containing

```

	{
		stageClass: "com.findwise.hydra.stage.SetStaticFieldStage",
		fieldValueMap: {
			"title" : "This is my title" 
		}
	}

```

* Create a file called `stdOutStage.json` containing

```

	{
		stageClass: "com.findwise.hydra.debugging.StdoutOutput",
		query : { 
			"touched" : { 
				"setTitleStage" : true 
			} 
		}
	}

```

Add the stages:

* `java -jar hydra-inserter.jar --add --pipeline pipeline --stage --id basic --name setTitleStage setTitleStage.json` 
* `java -jar hydra-inserter.jar --add --pipeline pipeline --stage --id debug --name stdOutStage stdOutStage.json` 

You have now added the stages `setTitleStage` and `stdOutStage` to the pipeline `pipeline` using the stage libraries `basic` and `debug`, respectively.

Start hydra by running `java -jar hydra-core.jar`. You will now see a lot of logging, and should see the two stages starting up. Wait until Hydra is logging `No updates found".

Everything is now up and running. To add a document for processing, just type

```

	java -cp hydra-debugging-jar-with-dependencies.jar com.findwise.hydra.debugging.StdinInput 
	"{\"contents\":{\"text\":\"This is my text\"}}"

```

Hydra will print out the processed document, that should contain a title as well as the imported text. Try modifying the configuration, inserting it again, then insert a new document and see what happens!


## Debugging

You can run your stages from the command line (or you IDE) if you need to debug them. 

java -cp `{libraryJarWithDependencies}` com.findwise.hydra.stage.AbstractStage `{fieldName}` `{hydraHost}` `{hydraRestPort}` `{performaceLoggingEnabled}` `{loggingPort}` `{configuration}`

i.e.
```
	java -cp basic-jar-with-dependincies.jar com.findwise.hydra.stage.AbstractStage staticField localhost 12001 false 12002 "{ stageClass: \"com.findwise.hydra.stage.SetStaticFieldStage\", fieldValueMap: {\"source\": \"my source\" } }"
```


## Scaling and Distribution

You can easily start a stage with several threads on your machine. This can for example be done when you have one stage that is the bottleneck of your processing. Set the `numberOfThreads` parameter to the stage configuration and push the configuration to hydra and it will restart the stage with more threads.

```

	{
		stageClass: "com.findwise.hydra.stage.SetStaticFieldStage",
		fieldValueMap: {
			"title" : "This is my title" 
		},
		numberOfThreads: 3
	}

```

If you need to distribute Hydra, to make it run on several servers, the only thing you need to do is set up a second hydra-core using the same mongodb instance as your previously running hydra instance. The new core will automatically download the stages and start them. Each Hydra core instance will hold its own cache, meaning that a document that will probably not be passed around between the servers.

Configuring the mongodb location is done in the `resource.properties` file, that is located in the `distribution/bin` folder.

If you are running heavy processing in your stages, the cache may time out forcing a new call to mongodb. To prevent this from happening, increase the cache timeout in `resource.properties`. 
