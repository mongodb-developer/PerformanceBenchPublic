# Notice: Repository Deprecation
This repository is deprecated and no longer actively maintained. It contains outdated code examples or practices that do not align with current MongoDB best practices. While the repository remains accessible for reference purposes, we strongly discourage its use in production environments.
Users should be aware that this repository will not receive any further updates, bug fixes, or security patches. This code may expose you to security vulnerabilities, compatibility issues with current MongoDB versions, and potential performance problems. Any implementation based on this repository is at the user's own risk.
For up-to-date resources, please refer to the [MongoDB Developer Center](https://mongodb.com/developer).


# Performance Bench

### A framework for comparing the relative performance of different database schemas

PerformanceBench is a simple Java framework designed to allow developers to assess the relative performance 
of different database design patterns. Although designed primarily with MongoDB in mind, the framework could
be used with any database.

PerformanceBench defines its functionality in terms of **_"models"_** (the design patterns being assessed), and 
_**"measures"**_ (the operations to be measured against each model). As an example, a developer may wish to assess
the relative performance of a design based on having data spread across multiple collections and accessed
using $lookup aggregations, versus one based on embedding related documents within each other. In this
scenario, the models might be referred to as "multi-table" and "hierarchical", with the "measures" for each
being CRUD operations - "Create", "Read", "Update" and "Delete".

PerformanceBench is configured via a JSON format configuration file which contains an array of documents -
one for each model being tested. Each model document in the configuration file is expected to contain a set
of mandatory fields that are common for all models being tested via performance bench. Additionally, each
document can contain a set of custom fields specific to the model and measures being tested.

Models and their associated measures are tested via Java classes implementing the SchemaTest interface 
defined by PerformanceBench. The output of each implemented class is array of BSON documented which 
PerformanceBench writes to a specified MongoDB collection for later analysis. The format of the output 
documents is at the discretion of the implementing developer and should be based around supporting the 
expected analysis of the results. PerformanceBench itself does not impose any requirements on the format of
the output documents.

## The SchemaTest Interface

The SchemaTest interface defines five methods:

`public void initialize(JSONObject args);`

In the initialize method, implementing classes should carry out any steps necessary prior to measures being
executed. This could - for example - include establishing and verifying connection to the database, building 
or preparing a test data set, and / or removing the results of prior execution runs. The JSON configuration
document for the model should be passed as argument 'args'.

`public String name();`

The name method should return a string name for the implementing class. Class implementers can set the 
returned value to anything that makes sense for their use-case.

`public void warmup(JSONObject args);`

The warmup method is called by PerformanceBench prior to any iterations of any measure being executed. It is
designed to allow model class implementers to attempt to create an environment that accurately reflects the 
expected state of the database in real-life. This could, for example, mean seeding the cache with an 
appropriate working set of data. The JSON configuration document for the model should be passed as argument 
'args'.

`public Document[] executeMeasure(int opsToTest, String measure, JSONObject args);`

The executeMeasure method allows PerformanceBench to instruct a model implementing class to execute a defined
number of iterations of a specified measure. Typically, the method implementation will contain a case 
statement redirecting execution to the code for each defined measure, however there is no requirement to 
implement in that way. Model class developers are free to implement this in any way that makes sense for 
their specific use-case.
The return from this method should be an array of BSON Document objects containing the results of each test
iteration. Implementers are free to include whatever fields are necessary in these documents to support the 
metrics their use case requires. PerformanceBench does not impose any expectations or restrictions on the 
shape of these documents. The returned documents are stored by PerformanceBench in a database and collection
specified by the JSON configuration document for the model.
The JSON configuration document for the model should be passed as argument 'args'. The name of the measure
to be executed should be passed in parameter 'measure'. These names are defined in the JSON configuration 
documents. The number of iterations of the measure to execute is passed in parameter 'opsToTest'.

`public void cleanup(JSONObject args);`

The cleanup method is called by PerformanceBench after all iterations of all measures have been executed by
the implementing class and is designed primarily to allow test data to be deleted or reset ahead of future
test executions. However, the method can also be used to execute any other post test-run functionality 
necessary for a given use case. This may, for example, include calculating average / mean / percentile 
execution times for a test run, or for cleanly disconnecting from a database. The JSON configuration 
document for the model should be passed as argument 'args'.

## JSON Configuration File Format

When PerformanceBench is run, it reads the JSON configuration file and executes the measures for each model
defined in the file in the order in which they are defined in the file. The configuration options allow
developers to define a number of concurrent threads in which each measure for each model should be executed
allowing multi-user environments to be modelled. Additionally, the number of iterations of each measure to
be carried out by each thread can also be defined. If the configuration for a given model specifies 4 threads
should each carry out 1000 iterations, and there are 4 measures defined for the model, a total of 16,000 
measure executions will be carried out for the model.

An example JSON configuration file is listed below:
```
{
  "models": [
    {
      "namespace": "com.mongodb.devrel.pods.performancebench.models.apimonitor_lookup",
      "className": "APIMonitorLookupTest",
      "measures": ["USEPIPELINE"],
      "threads": 2,
      "iterations": 500,
      "resultsuri": "mongodb+srv://myuser:mypass@my_atlas_instance.mongodb.net/?retryWrites=true&w=majority",
      "resultsCollectionName": "apimonitor_results",
      "resultsDBName": "performancebenchresults",
      "custom": {
        "uri": "mongodb+srv://myuser:mypass@my_atlas_instance.mongodb.net/?retryWrites=true&w=majority",
        "apiCollectionName": "APIDetails",
        "metricsCollectionName": "APIMetrics",
        "precomputeCollectionName": "APIPreCalc",
        "dbname": "APIMonitor",
        "regions": ["UK", "TK", "HK", "IN" ],
        "baseDate": "2022-11-01T00:00:00.000Z",
        "clusterTier": "M40",
        "rebuildData": false,
        "apiCount": 1000
      }
    },
    {
      "namespace": "com.mongodb.devrel.pods.performancebench.models.apimonitor_regionquery",
      "className": "APIMonitorRegionQueryTest",
      "measures": ["QUERYSYNC","QUERYASYNC"],
      "threads": 2,
      "iterations": 500,
      "resultsuri": "mongodb+srv://myuser:mypass@my_atlas_instance.mongodb.net/?retryWrites=true&w=majority",
      "resultsCollectionName": "apimonitor_results",
      "resultsDBName": "performancebenchresults",
      "custom": {
        "uri": "mongodb+srv://myuser:mypass@my_atlas_instance.mongodb.net/?retryWrites=true&w=majority",
        "apiCollectionName": "APIDetails",
        "metricsCollectionName": "APIMetrics",
        "precomputeCollectionName": "APIPreCalc",
        "dbname": "APIMonitor",
        "regions": ["UK", "TK", "HK", "IN" ],
        "baseDate": "2022-11-01T00:00:00.000Z",
        "clusterTier": "M40",
        "rebuildData": false,
        "apiCount": 1000
      }
    }
  ]
}
```

The **_'models'_** array is compulsory, and within each document in the array, the '**_namespace', 'className', 'measures',
'threads', 'iterations', 'resultsuri', 'resultsCollectionName', 'resultsDBName'_** and **_'custom'_** fields are
required:

**'namespace'** defines the namespace of the class implementing SchemaTest interface for this model.

**'className'** is the name of the class implementing the SchemaTest interface for this model.

**'measures'** is an array of string values defining the name of the measures to be executed for this model. 
PerformanceBench instructs the model implementation to execute the measures in the order in which they are listed
in the array.

**'threads'** defines the number of concurrent threads PerformanceBench will spawn to execute iterations of each
defiend measure. This allows multi-user environments to be simulated by the model classes.

**'iterations'** defines the number of iterations of each measure each thread should execute.

**'resultsuri'** defines the connection string for the MongoDB instance to which PerformanceBench will write 
results documents for this model.

**'resultsDBName'** defines the MongoDB database name to which PerformanceBench will write results documents for 
this model.

**'resultsCollectionName'** defines the MongoDB collection name to which PerformanceBench will write results 
documents for this model.

**'custom'** is a sub-document containing configuration values specific to the model implementation. The contents
of this document is at the discretion of model class implementing developers and - other than expecting it
to exist, PerformanceBench does not impose any expectations or requirements on shape of this document.

## Executing PerformanceBench

The name and path to the configuration file is passed to PerformanceBench as a command line parameter with the
'-c' flag, eg:

`java -jar performancebench.jar -c apimonitor_config.json`

The source was developed in IntelliJ IDEA 2022.2.3 using OpenJDK Runtime Environment Temurin-17.0.3+7 (build 17.0.3+7).
The compiled application was also run on Amazon Linux using OpenJDK 17.0.5 (2022-10-18 LTS - Corretto).

## APIMonitor Sample Models

The code in this repository includes example SchemaTest classes for a hypothetical API monitoring application. 
Within this application, a monitoring document is produced by observability software every 15 minutes for each
API every 15 minutes and includes the total number of calls to the API for the period, the number that were
successful and the number that failed. Each API is assigned to a geographic region (UK, Tokyo, Hong Kong, and
India). The implemented model classes are designed to compare alternative schema design options for querying 
the data to summarise the total number of calls and the overall success a and failure rates for all APIs in a
given region for a given time period. 
The implemented classes include an option to build, or rebuild, a sample data set for a given number of APIs.

## DISCLAIMER

PerformanceBench is **NOT** supported or endorsed by MongoDB. Whilst there are no restrictions on its download
or use, anyone doing so does so at their own risk.

