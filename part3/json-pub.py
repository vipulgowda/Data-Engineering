import asyncio
import aiohttp
from datetime import datetime
from google.cloud import pubsub_v1
import pandas as pd
from bs4 import BeautifulSoup
import re
from io import StringIO
import json

# project_id = "dataeng-gowda-vipulp"
# topic_id = "trimet-stop-topic"

# publisher = pubsub_v1.PublisherClient()
# topic_path = publisher.topic_path(project_id, topic_id)


# Path to the JSON file
json_file_path = './record.json'

def extract_h2_tags(html_content):
    soup = BeautifulSoup(html_content, 'html.parser')
    h2_tags = soup.find_all('h2')
    return [int(re.search(r'\b\d+\b', tag.get_text()).group(0)) for tag in h2_tags]

def extract_date(html_content):
    soup = BeautifulSoup(html_content, 'html.parser')
    date_tag = str(soup.find('h1'))
    date_str = re.search(r'\d{4}-\d{2}-\d{2}', date_tag)
    # Parse the date
    date_obj = datetime.strptime(date_str.group(), '%Y-%m-%d')
    # Format the date
    formatted_date = date_obj.strftime('%d%b%Y:%H:%M:%S').upper()
    return formatted_date


def html_to_dataframes(html_content):
    dataframes = pd.read_html(StringIO(html_content))
    return dataframes


async def convert_data_to_json(resp):
    html_content = await resp.text()
     
    h2_texts = extract_h2_tags(html_content)
    date_format = extract_date(html_content)
    dataframes = html_to_dataframes(html_content)

    for idx, df in enumerate(dataframes):
        df["trip_id"] = h2_texts[idx]
        df["timestamp"] = date_format
    concatenated_df = pd.concat(dataframes, ignore_index=True)
    json_data = concatenated_df.to_json(orient='records')
    return json_data


def main():
  # Read the JSON file
  with open(json_file_path, 'r') as file:
      data = json.load(file)
      print(data)
      future=""
      # Ensure data is a list
      if isinstance(data, list):
      # Iterate through each record (dictionary) in the list
        for record in data:
          # Process each record (for example, print the details)
            json_data = json.dumps(record)
            #Define the file path where you want to save the JSON json_data
            json_data = json_data.encode("utf-8")
            # future = publisher.publish(topic_path, json_data)
      else:
        print("The JSON file does not contain a list of records.")
  # future.result()

main()
