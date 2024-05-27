create table Stop (
        vehicle_number integer,
        trip_id integer,
        stop_time timestamp,
        leave_time timestamp,
        arrive_time timestamp,
        route_id integer,
        direction  tripdir_type,
        service_key service_type,
        maximum_speed float,
        train_mileage float, 
        dwell integer,
        location_id integer,
        lift integer,
        ons integer,
        offs integer,
        estimated_load integer,
        FOREIGN KEY (trip_id) REFERENCES Trip
);