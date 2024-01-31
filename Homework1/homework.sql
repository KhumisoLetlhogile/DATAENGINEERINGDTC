SELECT COUNT(*)
FROM green_taxi_data
WHERE DATE(lpep_pickup_datetime) = DATE '2019-09-18'
AND DATE(lpep_dropoff_datetime) = DATE '2019-09-18';

SELECT DATE(lpep_pickup_datetime)
FROM green_taxi_data
ORDER BY trip_distance DESC
LIMIT 1;

SELECT  zn."Borough",SUM(tx.total_amount)
FROM green_taxi_data tx
JOIN zones_data zn
ON tx."PULocationID" = zn."LocationID"
WHERE  DATE(lpep_pickup_datetime) = DATE '2019-09-18'
	AND "Borough" <> 'Unknown'
GROUP BY zn."Borough"
HAVING SUM(tx.total_amount)> 50000;


SELECT DOzn."Zone", MAX(tip_amount)
FROM green_taxi_data tx
JOIN zones_data PUzn
ON tx."PULocationID" =  PUzn."LocationID"
JOIN zones_data DOzn 
ON tx."DOLocationID" =  DOzn."LocationID"
WHERE PUzn."Zone" = 'Astoria'
GROUP BY DOzn."Zone"
ORDER BY  MAX(tip_amount) DESC
LIMIT 1;