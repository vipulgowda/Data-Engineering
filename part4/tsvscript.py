#!/usr/bin/python3
import csv, json
from geojson import Feature, FeatureCollection, Point
features = []
 #vehicle_trip-data.tsv
 #vehicle_load-data.tsv

with open('vehicle_trip-data.tsv', newline='') as csvfile:
    reader = csv.reader(csvfile, delimiter='\t')
    data = csvfile.readlines()

    for line in data[1:len(data)-1]:
        line.strip()
        row = line.split("\\t")
        # skip the rows where speed is missing
        x = row[0]
        y = row[1]
        speed = float(row[2])
        rounded_speed = round(speed, 2)  # Round to 2 decimal places
            
            # Handle other fields, checking for empty values
        vehicle_id = int(row[3].strip()) if row[3].strip() else None
        trip_id = row[4].strip() if row[4].strip() else None
        route_id = int(row[5].strip()) if row[5].strip() else None
        timestamp = row[6].strip() if row[6].strip() else None
        estimated_load = int(row[7].strip()) if row[7].strip() else None
        train_mileage = float(row[8].strip()) if row[8].strip() else None
        if speed is None or speed == "":
            continue
        try:
            latitude, longitude = map(float, (y, x))
            features.append(
                Feature(
                    geometry = Point((longitude,latitude)),
                    properties = {
                        'speed': (int(rounded_speed)),
                        'vehicle_id': (vehicle_id),
                        'route_id': (route_id),
                        'estimated_load': (estimated_load),
                        'train_mileage': (train_mileage),
                        'timestamp':  timestamp,
                    }
                )
            )
        except ValueError as ve:
            print(f"ValueError: {ve} in row: {row}")
            continue
        except IndexError as ie:
            print(f"IndexError: {ie} in row: {row}")
            continue

collection = FeatureCollection(features)
with open("new-vehicle_trip-data.geojson", "w") as f:
    f.write('%s' % collection)