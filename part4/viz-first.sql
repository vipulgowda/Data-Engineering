WITH CombinedRecords AS (
    -- Select breadcrumb data
    SELECT
        'Breadcrumb' AS record_type,
        b.tstamp AS event_time,
        t.route_id, 
        t.trip_id, 
        t.vehicle_id,
        b.latitude, 
        b.longitude, 
        b.speed,
        NULL AS train_mileage,  -- Placeholder for stop data
        NULL AS estimated_load  -- Placeholder for stop data
    FROM 
        trip t
    JOIN 
        breadcrumb b ON t.trip_id = b.trip_id

    UNION ALL

    -- Select stop data
    SELECT
        'Stop' AS record_type,
        s.stop_time AS event_time,
        t.route_id, 
        t.trip_id, 
        t.vehicle_id,
        NULL AS latitude,  -- Initially null, to be updated
        NULL AS longitude, -- Initially null, to be updated
        0 AS speed,     -- Placeholder for breadcrumb data
        s.train_mileage, 
        s.estimated_load
    FROM 
        trip t
    JOIN 
        stop s ON t.trip_id = s.trip_id
),
UpdatedRecords AS (
    SELECT
        record_type,
        event_time,
        route_id,
        trip_id,
        vehicle_id,
        COALESCE(latitude, LAG(latitude) OVER (PARTITION BY trip_id ORDER BY event_time)) AS latitude,
        COALESCE(longitude, LAG(longitude) OVER (PARTITION BY trip_id ORDER BY event_time)) AS longitude,
        speed,
        train_mileage,
        estimated_load
    FROM
        CombinedRecords
),
VehicleTripCounts AS (
    -- Step 1: Count the number of trips for each vehicle
    SELECT 
        vehicle_id,
        COUNT(DISTINCT trip_id) AS trip_count
    FROM 
        UpdatedRecords
    where route_id > 1
    GROUP BY 
        vehicle_id
    ORDER BY 
        trip_count DESC
    LIMIT 1  -- Limit to the vehicle with the most trips
),
VehicleTravelData AS (     -- Step 3: Get the travel locations for the top vehicle trips
    SELECT 
        b.vehicle_id,
        b.trip_id,
        b.route_id,
        b.latitude,
        b.longitude,
        b.estimated_load,
        b.train_mileage,
        b.speed,
        b.event_time as tstamp
    FROM UpdatedRecords b WHERE b.trip_id IN (
    SELECT 
        t.trip_id
    FROM 
        UpdatedRecords t
    JOIN 
        VehicleTripCounts vtc ON t.vehicle_id = vtc.vehicle_id
    WHERE t.route_id > 1
        )
)
SELECT -- Final Selection: Get vehicle travel data ordered by vehicle, trip, and timestamp
    longitude,
    latitude,
    speed, -- Convert speed of 0 to NULL
    vehicle_id,
    trip_id,
    route_id,
    tstamp,
    estimated_load,
    train_mileage
FROM
    VehicleTravelData
WHERE
    latitude IS NOT NULL -- Exclude rows with NULL latitude
    AND longitude IS NOT NULL -- Exclude rows with NULL longitude
ORDER BY
    tstamp, trip_id; -- Order by timestamp and trip_id

