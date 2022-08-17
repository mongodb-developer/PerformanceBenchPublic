/* 
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 * Click nbfs://nbhost/SystemFileSystem/Templates/Other/SQLTemplate.sql to edit this template
 */
/**
 * Author:  graeme
 * Created: Aug 12, 2022
 */
DROP SCHEMA IF EXISTS orderbench CASCADE;
CREATE SCHEMA IF NOT EXISTS orderbench
    AUTHORIZATION postgres;

CREATE EXTENSION pg_prewarm SCHEMA orderbench;

DROP TABLE IF EXISTS orderbench.customers;

CREATE TABLE IF NOT EXISTS orderbench.customers
(
    _id character varying COLLATE pg_catalog."default" NOT NULL,
    email character varying COLLATE pg_catalog."default",
    type character varying COLLATE pg_catalog."default",
    data character varying COLLATE pg_catalog."default",
    "customerId" integer,
    CONSTRAINT customers_pkey PRIMARY KEY (_id)
)

TABLESPACE pg_default;

ALTER TABLE IF EXISTS orderbench.customers
    OWNER to postgres;

DROP TABLE IF EXISTS orderbench.invoices;

DROP INDEX IF EXISTS orderbench.customers_id;

CREATE INDEX IF NOT EXISTS customers_id
    ON orderbench.customers USING btree
    (_id COLLATE pg_catalog."default" ASC NULLS LAST)
    TABLESPACE pg_default;

CREATE TABLE IF NOT EXISTS orderbench.invoices
(
    _id character varying COLLATE pg_catalog."default" NOT NULL,
    date timestamp without time zone,
    type character varying COLLATE pg_catalog."default",
    "customerId" integer,
    "orderId" integer,
    "invoiceId" integer,
    amount numeric,
    CONSTRAINT invoices_pkey PRIMARY KEY (_id)
)

TABLESPACE pg_default;

ALTER TABLE IF EXISTS orderbench.invoices
    OWNER to postgres;

DROP INDEX IF EXISTS orderbench.invoices_id;

CREATE INDEX IF NOT EXISTS invoices_id
    ON orderbench.invoices USING btree
    (_id COLLATE pg_catalog."default" ASC NULLS LAST)
    TABLESPACE pg_default;

DROP TABLE IF EXISTS orderbench.orderitems;

CREATE TABLE IF NOT EXISTS orderbench.orderitems
(
    _id character varying COLLATE pg_catalog."default" NOT NULL,
    date timestamp without time zone,
    type character varying COLLATE pg_catalog."default",
    details character varying COLLATE pg_catalog."default",
    data character varying COLLATE pg_catalog."default",
    qty integer,
    "customerId" integer,
    "itemId" integer,
    "orderId" integer,
    "productId" integer,
    price integer,
    CONSTRAINT orderitems_pkey PRIMARY KEY (_id)
)

TABLESPACE pg_default;

ALTER TABLE IF EXISTS orderbench.orderitems
    OWNER to postgres;

DROP INDEX IF EXISTS orderbench.orderitems_id;

CREATE INDEX IF NOT EXISTS orderitems_id
    ON orderbench.orderitems USING btree
    (_id COLLATE pg_catalog."default" ASC NULLS LAST)
    TABLESPACE pg_default;


DROP TABLE IF EXISTS orderbench.orders;

CREATE TABLE IF NOT EXISTS orderbench.orders
(
    _id character varying COLLATE pg_catalog."default" NOT NULL,
    date timestamp without time zone,
    type character varying COLLATE pg_catalog."default",
    description character varying COLLATE pg_catalog."default",
    amount integer,
    "customerId" integer,
    "orderId" integer,
    lastupdate time without time zone,
    CONSTRAINT orders_pkey PRIMARY KEY (_id)
)

TABLESPACE pg_default;

ALTER TABLE IF EXISTS orderbench.orders
    OWNER to postgres;

DROP INDEX IF EXISTS orderbench.orders_id;

CREATE INDEX IF NOT EXISTS orders_id
    ON orderbench.orders USING btree
    (_id COLLATE pg_catalog."default" ASC NULLS LAST)
    TABLESPACE pg_default;

DROP TABLE IF EXISTS orderbench.products;

CREATE TABLE IF NOT EXISTS orderbench.products
(
    _id character varying COLLATE pg_catalog."default" NOT NULL,
    type character varying COLLATE pg_catalog."default",
    name character varying COLLATE pg_catalog."default",
    description character varying COLLATE pg_catalog."default",
    qty integer,
    "productId" integer,
    "warehouseId" integer,
    CONSTRAINT products_pkey PRIMARY KEY (_id)
)

TABLESPACE pg_default;

ALTER TABLE IF EXISTS orderbench.products
    OWNER to postgres;

DROP INDEX IF EXISTS orderbench.products_id;

CREATE INDEX IF NOT EXISTS products_id
    ON orderbench.products USING btree
    (_id COLLATE pg_catalog."default" ASC NULLS LAST)
    TABLESPACE pg_default;

DROP TABLE IF EXISTS orderbench.warehouses;

CREATE TABLE IF NOT EXISTS orderbench.warehouses
(
    _id character varying COLLATE pg_catalog."default" NOT NULL,
    "Country" character varying COLLATE pg_catalog."default",
    "County" character varying COLLATE pg_catalog."default",
    "City" character varying COLLATE pg_catalog."default",
    "Street" character varying COLLATE pg_catalog."default",
    "Number" integer,
    "ZipCode" integer,
    "warehouseId" integer,
    CONSTRAINT warehouses_pkey PRIMARY KEY (_id)
)

TABLESPACE pg_default;

ALTER TABLE IF EXISTS orderbench.warehouses
    OWNER to postgres;

DROP INDEX IF EXISTS orderbench.warehouses_id;

CREATE INDEX IF NOT EXISTS warehouses_id
    ON orderbench.warehouses USING btree
    (_id COLLATE pg_catalog."default" ASC NULLS LAST)
    TABLESPACE pg_default;

DROP TABLE IF EXISTS orderbench.shipments;

CREATE TABLE IF NOT EXISTS orderbench.shipments
(
    _id character varying COLLATE pg_catalog."default" NOT NULL,
    "customerId" integer,
    "orderId" integer,
    "shipmentId" integer,
    "type" character varying COLLATE pg_catalog."default",
    date timestamp without time zone,
    "Country" character varying COLLATE pg_catalog."default",
    "County" character varying COLLATE pg_catalog."default",
    "City" character varying COLLATE pg_catalog."default",
    "Street" character varying COLLATE pg_catalog."default",
    "Number" integer,
    "ZipCode" integer,
    "method" character varying COLLATE pg_catalog."default",
    CONSTRAINT shipments_pkey PRIMARY KEY (_id)
)

TABLESPACE pg_default;

ALTER TABLE IF EXISTS orderbench.shipments
    OWNER to postgres;

DROP INDEX IF EXISTS orderbench.shipments_id;

CREATE INDEX IF NOT EXISTS shipments_id
    ON orderbench.shipments USING btree
    (_id COLLATE pg_catalog."default" ASC NULLS LAST)
    TABLESPACE pg_default;

DROP TABLE IF EXISTS orderbench.shipmentitems;

CREATE TABLE IF NOT EXISTS orderbench.shipmentitems
(
    _id character varying COLLATE pg_catalog."default" NOT NULL,
    "customerId" integer,
    "orderId" integer,
    "shipmentId" integer,
    "shipmentItemId" integer,
    "type" character varying COLLATE pg_catalog."default",
    CONSTRAINT shipmentitems_pkey PRIMARY KEY (_id)
)

TABLESPACE pg_default;

ALTER TABLE IF EXISTS orderbench.shipmentitems
    OWNER to postgres;

DROP INDEX IF EXISTS orderbench.shipmentitems_id;

CREATE INDEX IF NOT EXISTS shipmentitems_id
    ON orderbench.shipmentitems USING btree
    (_id COLLATE pg_catalog."default" ASC NULLS LAST)
    TABLESPACE pg_default;