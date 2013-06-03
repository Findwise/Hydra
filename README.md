Hydra Processing Framework
==========================

Overview: [findwise.github.com/Hydra](http://findwise.github.com/Hydra/)

Mailing list/group: [hydra-processing google group](https://groups.google.com/forum/#!forum/hydra-processing)

Current snapshot: [![Build Status](https://secure.travis-ci.org/Findwise/Hydra.png?branch=master)](https://travis-ci.org/Findwise/Hydra)

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
3. To start, run `java -jar hydra-core.jar` from inside the `bin` directory

Using Hydra
-----------

Once you have a running pipeline system, you can now start adding some some stages to it. For the most basic pipeline, you'll want to load at least two stage libraries into Hydra: `basic-stages` and `solr-out` (if you are connecting Hydra to send documents to Solr).

You'll find the projects to build these two (using Maven) in `stages/processing/basic` and `stages/output/solr`.

There are a few different methods for setting up pipelines in Hydra:

* Script your pipeline setup with the `database-impl/mongo` package
* Set up the Admin Service webapp and configure the pipeline using its REST interface
* Use the `CmdlineInserter`-class that you can find in the `examples` project under the Hydra root

Using the CmdlineInserter, if you run `mvn clean install` on that project, you will get a runnable jar that you can use (`java -jar inserter-jar-with-dependencies.jar`) Below, we'll assume that's the method you are using. 

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
* __query__: A serialized version of the query, that all documents this stage receives must match. In this example, all documents received by this stage will have already been processed by a stage called _extractTitles_ and they all have a field called _source_.
* __fieldValueMap__: The input parameters specific for this stage. In this case, it expects a map from field names to any object.

Save the configuration in a file somewhere on disk, e.g. {mystage.properties}, for ease of use. 

#### Inserting the configuration

Run the CmdlineInserter class (or the jar) with the following arguments:
	`-a -p pipeline -s -i {my-library-id} -n {my-stage-name} {mystage.properties}` 
	
That's it. You now have a pipeline configured with a SetStaticField-stage. If your Hydra Core was running while you configured this, you'll notice that it picked up the change and launched the stage with the properties you provided. Any subsequent changes to the configuration will also make Hydra Core restart the stage.

Next, you'll probably want to add an output stage, and then start pushing some documents in!
