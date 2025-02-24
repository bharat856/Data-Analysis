CREATE OR REPLACE TABLE `data-analysis-project-bharat.uber_data_analysis_database.analyticsTable` AS (
SELECT 
    f.trip_id,
    d.tpep_pickup_datetime,
    d.tpep_dropoff_datetime,
    p.passenger_count,
    t.trip_distance,
    r.RatecodeID,
    pick.pickup_latitude,
    pick.pickup_longitude,
    drop.dropoff_latitude,
    drop.dropoff_longitude,
    pay.payment_type,
    f.fare_amount,
    f.extra,
    f.mta_tax,
    f.tip_amount,
    f.tolls_amount,
    f.improvement_surcharge,
    f.total_amount
FROM 

    `uber_data_analysis_database.fact_table` f
JOIN `uber_data_analysis_database.datetime_dim` d  ON f.datetime_dim_id = d.datetime_dim_id
JOIN `uber_data_analysis_database.passenger_count_dim` p  ON p.passenger_count_dim_id = f.passenger_count_dim_id  
JOIN `uber_data_analysis_database.trip_distance_dim` t  ON t.trip_distance_dim_id = f.trip_distance_dim_id  
JOIN `uber_data_analysis_database.rate_code_dim` r ON r.rate_code_dim_id = f.rate_code_dim_id  
JOIN `uber_data_analysis_database.pickup_location_dim` pick ON pick.pickup_location_dim_id = f.pickup_location_dim_id
JOIN `uber_data_analysis_database.dropoff_location_dim` drop ON drop.dropoff_location_dim_id = f.dropoff_location_dim_id
JOIN `uber_data_analysis_database.payment_type_dim` pay ON pay.payment_type_dim_id = f.payment_type_dim_id
);