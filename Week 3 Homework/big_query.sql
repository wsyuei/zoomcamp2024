
-- CREATE AN EXTERNAL TABLE REFERRING TO GCS BUCKET
CREATE OR REPLACE EXTERNAL TABLE `mindful-future-412612.ny_taxi_dataset.external_green_tripdata_2022`
OPTIONS (
  format = 'PARQUET',
  uris = ['gs://mage_zoomcamp_bucket_01/green_taxi_2022_data.parquet']
);

-- CREATE A NON-PARTITIONED TABLE FROM EXTERNAL TABLE
CREATE OR REPLACE TABLE ny_taxi_dataset.green_trips_non_partitioned AS 
(SELECT * FROM `mindful-future-412612.ny_taxi_dataset.external_green_tripdata_2022`);


--CREATE A PARTITIONED TABLE FROM EXTERNAL TABLE
CREATE OR REPLACE TABLE ny_taxi_dataset.green_trips_partitioned PARTITION BY (lpep_pickup_date) 
AS (SELECT * FROM `mindful-future-412612.ny_taxi_dataset.external_green_tripdata_2022`);

-- CREATE MATERILZED TABLE 
CREATE MATERIALIZED VIEW `mindful-future-412612.ny_taxi_dataset.mv_green_tripdata_2022` AS
SELECT *
FROM `mindful-future-412612.ny_taxi_dataset.green_trips_partitioned`;

-- count distinct PULocationID value from external table and materialzed table
SELECT count(distinct(PULocationID)) FROM  `mindful-future-412612.ny_taxi_dataset.external_green_tripdata_2022`;
SELECT count(distinct(PULocationID)) FROM `mindful-future-412612.ny_taxi_dataset.mv_green_tripdata_2022` ;

-- count VendorID value WITH 0 fare amount
SELECT count(VendorID) FROM `mindful-future-412612.ny_taxi_dataset.external_green_tripdata_2022` WHERE fare_amount = 0.0;


-- Impact of partition
SELECT DISTINCT(PULocationID)
FROM `mindful-future-412612.ny_taxi_dataset.green_trips_non_partitioned` 
WHERE lpep_pickup_date BETWEEN '2022-06-01' AND '2022-06-30';

-- Impact of non partition
SELECT DISTINCT(PULocationID)
FROM `mindful-future-412612.ny_taxi_dataset.green_trips_partitioned` 
WHERE lpep_pickup_date BETWEEN '2022-06-01' AND '2022-06-30';
