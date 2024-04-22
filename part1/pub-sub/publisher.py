import time
import os
import json
from datetime import datetime
from google.cloud import pubsub_v1
# TODO(developer)
project_id = "dataeng-gowda-vipulp"
topic_id = "trimet-topics"

publisher = pubsub_v1.PublisherClient()
# The `topic_path` method creates a fully qualified identifier
# in the form `projects/{project_id}/topics/{topic_id}`
topic_path = publisher.topic_path(project_id, topic_id)


def read_files_in_folder(folder):
    try:
      # Iterate through all files in the folder
      for filename in os.listdir(folder):
        file_path = os.path.join(folder, filename)
        # Check if the file is a regular file
        if os.path.isfile(file_path):
          print(f'Records in file: {filename}')
          # Read and print all records in the file
          with open(file_path, 'r') as file:
              json_data = json.load(file)
              for record in json_data:
              # Data must be a bytestring
                data = json.dumps(record)
                data = data.encode("utf-8")
                # When you publish a message, the client returns a future.
                future = publisher.publish(topic_path, data)
                print(future.result())
                print(f"Published messages to {topic_path}.")
    except FileNotFoundError:
        print(f'Folder {folder} does not exist.')
    except PermissionError:
        print(f'Permission denied when accessing folder {folder}.')
    except Exception as e:
        print(f'An error occurred: {e}')

# Get today's date in the format MMDDYYYY
current_date = datetime.now().strftime("%m%d%Y")

# Path to the folder named after today's date
folder_path = f'./breadcrumb_data/{current_date}/'

# Check if the folder exists
if os.path.exists(folder_path):
    read_files_in_folder(folder_path)
else:
    print(f'Folder for {current_date} does not exist.')

