Hydra Admin Service
==========================

Available endpoints
----------------

* **/** - Method: GET

	Lists some general statistics about the Hydra node.
	
* **/libraries** - Method: GET 

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
	
* **/libraries/{library-id}** - Method: GET

	Same as **/library** but only for the specified library id. 
	
* **/libraries/{library-id}** - Method: POST
	
	Request format: *multipart/form-data*
	
	Request content: Field named *file* containing library jar file.

	Posts a jar file into Hydra. If post is successful, the returned response is 202 ACCEPTED and the textual response of a GET for this newly inserted library. 
	
	An example of how to successfully conform to these parameters would be by using this form:
	
	```
	<form method="post" action="http://<host:port>/hydra/library/<desired-library-id>" enctype="multipart/form-data">
        <input type="file" name="file"/>
        <input type="submit"/>
    </form>
	```
	
* **/libraries/{library-id}/stages/{stage-name}** - Method: POST
	
	Request format: *application/json*
	
	Request content: Stage configuration in json format

	Adds a stage or replaces a stage in the pipeline with the attached configuration as raw body.
	
* **/stages** - Method: GET
	
	Lists all active stages in the pipeline. Sample response:
	```
	{
	    "stages": [
	        {
	            "name": "languageDetection",
	            "databaseFile": {
	                "filename": "language-detection-stage-jar-with-dependencies.jar",
	                "uploadDate": 1350482288206,
	                "id": "language-detection",
	                "inputStream": null
	            },
	            "mode": "ACTIVE",
	            "properties": {
	                "languages": [
	                    "en",
	                    "sv",
	                    "da",
	                    "no",
	                    "de",
	                    "lv",
	                    "lt",
	                    "uk"
	                ],
	                "query": {
	                    "touched": {
	                        "cleanContent": true
	                    }
	                },
	                "map": {
	                    "DRECONTENT": "language_code"
	                },
	                "defaultLanguage": "GeneralUTF8",
	                "threshold": 500,
	                "stageClass": "com.findwise.hydra.stage.languageDetection.LanguageDetectionStage"
	            },
	            "propertiesModifiedDate": 1351525890276,
	            "propertiesChanged": true
	        },
	        {
	            "name": "cleanParentInfo",
	            "databaseFile": {
	                "filename": "hydra-basic-stages-jar-with-dependencies.jar",
	                "uploadDate": 1350482283857,
	                "id": "basic",
	                "inputStream": null
	            },
	            "mode": "ACTIVE",
	            "properties": {
	                "regexConfigs": [
	                    {
	                        "outField": "PARENT_TITLE",
	                        "regex": ">(.*)<",
	                        "inField": "PARENT_TITLE",
	                        "substitute": "$1"
	                    },
	                    {
	                        "outField": "PARENT_LINK",
	                        "regex": "href=\"(.*?)\"",
	                        "inField": "PARENT_LINK",
	                        "substitute": "$1"
	                    }
	                ],
	                "query": {
	                    "touched": {
	                        "extractParentInfo": true
	                    }
	                },
	                "stageClass": "com.findwise.hydra.stage.RegexStage"
	            },
	            "propertiesModifiedDate": 1351525890276,
	            "propertiesChanged": true
	        }
	    ]
	}
	```
	
  
* **/stages/{stage-name}** - Method: GET

	Same as **/stages** but only for the specified stage name. 
	

* **/documents** - Method: GET
	
	Lists the (first 10) documents matching a query (or any documents if the query is ommitted). 
	The number of documents to be returned and what document to start counting from, can be configured with request parameters
	
	Available parameters: 
	
		- q: A json query to be matched by the counted documents (*default: {}*)
		
		- limit: The number of documents to return (*default: 10*)
		
		- skip: The number of documents to skip (used for example if you which to do pagination on returned documents) (*default: 0*)
	
	Sample request: http://localhost:8080/hydra/documents?q={fetched:{rename:true}}&limit=10&skip=0
	
	Sample response: 
	```
	{
	    "documents": [
	        {
	            "actionTouched": false,
	            "touchedContent": [],
	            "touchedMetadata": [],
	            "partialObject": false,
	            "id": {
	                "time": 1350460786000,
	                "new": false,
	                "machine": -880203394,
	                "timeSecond": 1350460786,
	                "inc": 1616527124
	            },
	            "status": "PROCESSING",
	            "action": "DELETE",
	            "metadataMap": {
	                "fetched": {
	                    "addContent": 1350460788405,
	                    "renameDeleteReference": 1350460787082
	                },
	                "touched": {
	                    "addContent": 1350460788422,
	                    "renameDeleteReference": 1350460787097
	                }
	            },
	            "contentFields": [
	                "content",
	                "url"
	            ],
	            "contentsMap": {
	                "content": "content",
	                "url":"http://asdf.com",
	            },
	            "idkey": "_id"
	        }
	    ]
	}
	```
	
* **/documents/count** - Method: GET
	
	Returns the number of document matching a query (or the total number of documents if the query is ommitted)

	Available parameters: 
	
		- q: A json query to be matched by the counted documents (*default: {}*)
	
	Sample request: http://localhost:8080/hydra/documents/count?q={fetched:{rename:true}}
	
	Sample response: 
	
	```
	{
		numberOfDocuments: 13
	}
	```
	
	
* **/documents/edit** - Method: POST
	
	Changes the metadata of a document matching a query. Currently only supports deleting the a stages from the fetched and/or touched array  
	
	Available parameters: 
	
		- q: A json query to be matched by the counted documents (*default: {}*)
		
		- limit: The maximum number of documents to update (*default: 1*)
	
	Request content: A json configuration describing the change. Format: {"deletes":{fetched:["staticField"]},touched:["staticField"]}
	
	Sample resopnse:
	
	```
	{
	    "numberOfChangedDocuments": 5,
	    "changedDocuments": [...] (a list of documents as described above)
	}
	```