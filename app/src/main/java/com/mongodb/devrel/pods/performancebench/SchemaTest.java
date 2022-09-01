/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 * Click nbfs://nbhost/SystemFileSystem/Templates/Classes/Interface.java to edit this template
 */
package com.mongodb.devrel.pods.performancebench;

/**
 *
 * @author graeme
 */
import org.json.simple.JSONObject;

public abstract interface SchemaTest {
            
    public void initialize(JSONObject args);
        
    public double[] executeMeasure(int opsToTest, String subtest, JSONObject args, boolean warmup);
	
	public String name();

	public void cleanup();

	public void warmup();
}
