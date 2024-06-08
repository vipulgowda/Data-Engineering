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

TripAverageLoad AS (
    -- Compute the average estimated load for each trip
    SELECT
        trip_id,
        route_id,
        vehicle_id,
        AVG(estimated_load) AS average_estimated_load
    FROM
        UpdatedRecords
    WHERE
        estimated_load IS NOT NULL -- Only consider rows where estimated_load is available
    GROUP BY
        trip_id, route_id, vehicle_id
)

-- Final Selection: Filter trips with average estimated load greater than 10

Select 
    longitude,
    latitude,
    speed,
    vehicle_id,
    trip_id,
    route_id,
    event_time,
    estimated_load,
    train_mileage
from UpdatedRecords WHERE trip_id IN (
SELECT
    DISTINCT trip_id
FROM
    TripAverageLoad
WHERE
    average_estimated_load > 23
) AND latitude IS NOT NULL -- Exclude rows with NULL latitude
  AND longitude IS NOT NULL -- Exclude rows with NULL longitude
ORDER BY
    event_time, trip_id, vehicle_id;
