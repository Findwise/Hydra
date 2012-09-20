Hydra Admin Service
==========================

Available endpoints
----------------

* **/** - Method: GET

	Lists some general statistics about the Hydra node.
* **/library** - Method: GET 

	Lists libraries currently available in the database. For each library, a list of available stages and their parameters are presented. Here's an example of how the returned JSON might look:
	
	```
	solr-out : {
		uploaded: 1347989234697,
		filename: "hydra-solr-out-stage-jar-with-dependencies.jar",
		stages: {
			com.findwise.hydra.output.solr.SolrOutputStage: {
				description: "Writes documents to Solr",
				class: "com.findwise.hydra.output.solr.SolrOutputStage",
				parameters: {
					fieldMappings: {
						description: "A map specifying which fields in the Hydra document becomes which fields in Solr. The value of an entry must be one of either String or List<String>.",
						type: "Map",
						required: false
					},
					commitWithin: {
						description: "",
						type: "int",
						required: false
					},
					solrDeployPath: {
						description: "The URL of the Solr to which this stage will post data",
						type: "String",
						required: false
					},
					query: {
						description: "The Query that this stage will recieve documents matching",
						type: "LocalQuery",
						required: false
					},
					idField: {
						description: "",
						type: "String",
						required: false
					},
					failDocumentOnProcessException: {
						description: "If set, indicates that the document being processed should be FAILED if a ProcessException is thrown by the stage. If not set, the error will only be persisted and the document written back to Hydra.",
						type: "boolean",
						required: false
					},
					sendAll: {
						description: "If set, fieldMappings will be ignored and all fields will be sent to Solr.",
						type: "boolean",
						required: false
					}
				}
			}
		}
	}
	```
	
* **/library/{library-id}** - Method: GET

	Same as **/library** but only for the specified library id. 
	
* **/library/{library-id}** - Method: POST
	
	Request format: *multipart/form-data*
	
	Request content: Field named *file* containing library jar file.

	Posts a jar file into Hydra. If post is successful, the returned response is 202 ACCEPTED and the textual response of a GET for this newly inserted library. 
	
	An example of how to successfully conform to these parameters would be using this form:
	
	```
	<form method="post" action="http://<host:port>/hydra/library/YOUR-LIBRARY-ID-HERE" enctype="multipart/form-data">
        <input type="file" name="file"/>
        <input type="submit"/>
    </form>
	```