from concurrent.futures import TimeoutError
from google.cloud import pubsub_v1
from datetime import datetime
import os
import json

project_id="dataeng-gowda-vipulp"
subscription_id="trimet-sub-a"

current_date = datetime.now().strftime("%m%d%Y")
current_folder = 'trimet-sub-a'

def create_recv_folder_if_not_exists(folder_name):
    if not os.path.exists(folder_name):
        os.makedirs(folder_name)
        print(f"Folder '{folder_name}' created successfully.")

current_folder_path = "./home/vipulp/dataeng-project/" + current_folder
file_name = f'{current_date}.json'
file_path = current_folder_path + "/"  + file_name

create_recv_folder_if_not_exists(current_folder_path)

subscriber=pubsub_v1.SubscriberClient()

subscription_path = subscriber.subscription_path(project_id, subscription_id)

def callback(message: pubsub_v1.subscriber.message.Message) -> None:
    print(f"Received {message}")
    json_message = json.dumps(message.data.decode('utf-8'))
    with open(file_path, 'a') as file:
        file.write(json_message + '\n')
    message.ack()

streaming_pull_future = subscriber.subscribe(subscription_path, callback=callback)

print(f"Listening for messages on {subscription_path}..\n")

with subscriber:
    try:
        streaming_pull_future.result(timeout=50.0)
    except TimeoutError:
        streaming_pull_future.cancel()
        streaming_pull_future.result()