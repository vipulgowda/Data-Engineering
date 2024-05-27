from io import StringIO
import psycopg2
from concurrent.futures import TimeoutError
from google.cloud import pubsub_v1
from datetime import datetime, timedelta
import json
import pandas as pd

project_id = "dataeng-gowda-vipulp"
subscription_id = "trimet-stop-topic-sub"

DBname = "postgres"
DBuser = "postgres"
DBpwd = "hello"

subscriber = pubsub_v1.SubscriberClient()

subscription_path = subscriber.subscription_path(project_id, subscription_id)
json_list = []

def callback(message: pubsub_v1.subscriber.message.Message) -> None:
    json_message = json.loads(message.data.decode('utf-8'))
    json_list.append(json_message)
    message.ack()

streaming_pull_future = subscriber.subscribe(
    subscription_path, callback=callback)

print(f"Listening for messages on {subscription_path}..\n")

with subscriber:
    try:
        streaming_pull_future.result(timeout=5.0)
    except TimeoutError:
        streaming_pull_future.cancel()
        streaming_pull_future.result()


#############################################
#                                           #
#                 Milesone 2                #
#                                           #
#############################################
df = pd.DataFrame(json_list)

# Get the length of the DataFrame
length = len(df)

if length > 0:
    # Define function to create timestamp
    def create_timestamp(row, time_type):
        opd_date = datetime.strptime(row['timestamp'], '%d%b%Y:%H:%M:%S')
        converted_time = timedelta(seconds=row[time_type])
        timestamp = opd_date + converted_time
        return pd.Timestamp(timestamp)
    
    # Apply the function to create the TIMESTAMP column
    df['stop_time'] = df.apply(create_timestamp, axis=1,args=('stop_time',))
    df['leave_time'] = df.apply(create_timestamp, axis=1,args=('leave_time',))
    df['arrive_time'] = df.apply(create_timestamp, axis=1,args=('arrive_time',))
    
    df["vehicle_id"] = df["vehicle_number"]
    df["route_id"] = df["route_number"]
    
    df["direction"] = df['direction'].map({1: 'Out', 0: 'Back'})
    df['service_key'] = df['service_key'].map({'U': 'Sunday', 'S': 'Saturday', 'W': 'Weekday'})
    
    df = df[['vehicle_number', 'trip_id', 'stop_time','leave_time', 'arrive_time', 'route_id','direction', 'service_key',  "maximum_speed",  "train_mileage", "dwell", "location_id", "door", "lift", "ons", "offs","estimated_load"]]
    
    # Adjust display options to show all columns and their content
    pd.set_option('display.max_columns', None)
    pd.set_option('display.expand_frame_repr', False)  # Prevent line wrapping of DataFrame

    print(df.head())
    # Establish a connection to the database
    conn = psycopg2.connect(
        host="localhost",
        database=DBname,
        user=DBuser,
        password=DBpwd
    )
    
    update_query = "UPDATE trip SET route_id="

    def copy_from_stop(conn, df):
        """
        Here we are going save the dataframe in memory 
        and use copy_from() to copy it to the table
        """
        # save dataframe to an in memory buffer
        buffer = StringIO()
        df.to_csv(buffer, index=False, header=False)
        buffer.seek(0)

        cursor = conn.cursor()
        try:
            cursor.copy_from(buffer, 'stop', sep=",")
            conn.commit()
        except (Exception, psycopg2.DatabaseError) as error:
            print("Error: %s" % error)
            conn.rollback()
            cursor.close()
            return 1
        print("Loading of Stop table completed")
        cursor.close()

    def update_trip(conn, df):
        """
        Here we are going save the dataframe in memory 
        and use copy_from() to copy it to the table
        """
        # save dataframe to an in memory buffer
        buffer = StringIO()

        df.to_csv(buffer, index=False, header=False)
        buffer.seek(0)

        cursor = conn.cursor()
        try:
            for index, row in df.iterrows():
                trip_id = row['trip_id']
                route_id = row['route_id']
                direction = row['direction']
                # Execute the update query
                cursor.execute("""
                    UPDATE trip
                    SET route_id = %s, direction = %s
                    WHERE trip_id = %s
                """, (route_id, direction, trip_id))
                conn.commit()
        except (Exception, psycopg2.DatabaseError) as error:
            print("Error: %s" % error)
            conn.rollback()
            cursor.close()
            return 1
        print("Update of Trip table completed")
        cursor.close()

    copy_from_stop(conn, df)
    update_trip(conn, df)



"""
create table Trip (
        trip_id integer,
        route_id integer,
        vehicle_id integer,
        service_key service_type,
        direction tripdir_type,
        PRIMARY KEY (trip_id)
);

create table BreadCrumb (
        tstamp timestamp,
        latitude float,
        longitude float,
        speed float,
        trip_id integer,
        FOREIGN KEY (trip_id) REFERENCES Trip
);

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
"""