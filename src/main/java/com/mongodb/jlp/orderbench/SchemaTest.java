package com.mongodb.jlp.orderbench;

import java.util.List;

import org.bson.Document;

public interface SchemaTest {
	void prepareTestData();

	String name();

	void cleanup();

	void warmup();

	List<Document> getOrderById(int custid, int orderid);

	public int addNewShipment(int custid, int orderid, int shipmentid, int itemsinshipment, int warehouseid);

	public int updateSingleItem(int custid, int orderid, int itemid);

	public int updateMultiItem(int custid, int orderid, int itemid);
}
