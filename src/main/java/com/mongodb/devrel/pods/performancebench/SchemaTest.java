/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 * Click nbfs://nbhost/SystemFileSystem/Templates/Classes/Interface.java to edit this template
 */
package com.mongodb.devrel.pods.performancebench;

/**
 *
 * @author graeme
 *
 * Interface describing the methods to implimented by each model class.
 *
 */
import org.json.simple.JSONObject;
import org.bson.Document;

public abstract interface SchemaTest {

    /**
     *
     * @param args - a JSON Object listing parameters specific to the measure(s) being executed.
     *
     *             In the initialize method, implementing classes should carry out any steps necessary
     *             prior to tests being executed. This could - for example - include establishing
     *             and verifying connection to the database, building or preparing a test data set,
     *             and / or removing the results of prior execution runs.
     *
     */
    public void initialize(JSONObject args);

    /**
     *
     * @param opsToTest - the number of iterations of the measure to be performed
     * @param measure - the name of the measure to be executed.
     * @param args - a JSON Object listing parameters specific to the measure(s) being executed.
     * @return an array of BSON documents containing the results of each measure iteration.
     *          These will be saved to a collection specified in the application config JSON file.
     *
     *          Implementing classes can define one or more 'measures' to be executed. These could, for
     *          example, define a separate set of steps to measure each of Create, Read, Update and
     *          Delete operations against a data model.
     *          Typically, the method implementation will contain a case statement redirecting execution
     *          to the code for each defined measure, however there is no requirement to implement in
     *          that way. Model class developers are free to implement this in any way that makes sense
     *          for their specific use-case.
     *          It is expected that the specified measure will be executed x times where x is equal to
     *          the 'opsToTest' parameter.
     *          The args parameter contains the custom parameters defined for the implementing class in
     *          the application JSON configuration file. Model class developers are free to add whatever
     *          parameters are necessary for their classes.
     *          The return from this method should be an array of BSON Document objects containing the
     *          results of each test iteration. Implementers are free to include whatever fields are
     *          necessary in these documents to support the metrics their use case requires.
     *          PerformanceBench does not impose any expectations or restrictions on the shape of
     *          these documents. The returned documents are stored in the database and collection
     *          specified by the "resultsCollectionName" and "resultsDBName" fields in the application
     *          configuration JSON file.
     */
    public Document[] executeMeasure(int opsToTest, String measure, JSONObject args);

    /**
     *
     * @return returns a string name for the implementing class. Class implementors can set the returned
     *          value to anything that makes sense for their use-case.
     */
    public String name();

    /**
     *
     *  @param args - a JSON Object listing parameters specific to the measure(s) being executed.
     *
     *  The cleanup method is called by PerformanceBench after all iterations of all measures have been
     *  executed by the implementing class and is designed primarily to allow test data to be deleted or
     *  reset ahead of future test executions. However, the method can also be used to execute any other
     *  post test-run functionality necessary for a given use case. This may, for example, include
     *  calculating average / mean / percentile execution times for a test run
     *
     */
    public void cleanup(JSONObject args);

    /**
     *
     *  @param args - a JSON Object listing parameters specific to the measure(s) being executed.
     *
     *  The warmup method is called by PerformanceBench prior to any iterations of any measure being
     *  executed. It is designed to allow model class implementers to attempt to create an environment
     *  that accurately reflects the expected state of the database in real-life by,
     *  for example, seeding the cache with an appropriate working set of data,
     */
    public void warmup(JSONObject args);
}

