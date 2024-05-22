import asyncio
import aiohttp
from datetime import datetime
from google.cloud import pubsub_v1
import pandas as pd
from bs4 import BeautifulSoup
import re
from io import StringIO
import json

project_id = "dataeng-gowda-vipulp"
topic_id = "trimet-stop-topic"

publisher = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path(project_id, topic_id)


def extract_h2_tags(html_content):
    soup = BeautifulSoup(html_content, 'html.parser')
    h2_tags = soup.find_all('h2')
    return [int(re.search(r'\b\d+\b', tag.get_text()).group(0)) for tag in h2_tags]


def html_to_dataframes(html_content):
    dataframes = pd.read_html(StringIO(html_content))
    return dataframes


async def convert_data_to_json(resp):
    html_content = await resp.text()
     
    h2_texts = extract_h2_tags(html_content)

    dataframes = html_to_dataframes(html_content)

    for idx, df in enumerate(dataframes):
        df["trip_id"] = h2_texts[idx]
    concatenated_df = pd.concat(dataframes, ignore_index=True)
    json_data = concatenated_df.to_json(orient='records')
    return json_data


async def main():
    async with aiohttp.ClientSession() as session:
        try:
            with open("ids.txt", "r", encoding='utf-8') as busIds:
                print(f'Running the script on {datetime.now()}')
                record_count = 0
                future = ""
                # existing_data = []
                for bus_id in busIds:
                    bus_url = f'https://busdata.cs.pdx.edu/api/getStopEvents?vehicle_num={bus_id}'
                    async with session.get(bus_url) as resp:
                        if resp.status == 404:
                            print(f'Skipping this id: {bus_id}')
                            pass
                        else:
                            print(f'Content found for {bus_id}')
                            # Check if the content type is text/html
                            stop_records = await convert_data_to_json(resp)
                            stop_records = json.loads(stop_records)
                            for record in stop_records:
                                record_count += 1
                                data = json.dumps(record)
                                #Define the file path where you want to save the JSON data
                                data = data.encode("utf-8")
                                # existing_data.append(record)
                                future = publisher.publish(topic_path, data)
                                future.result()
            # file_path = 'record.json'
            # # Open the file in write mode and save the JSON string
            # with open(file_path, 'w') as file:
            #     json.dump(existing_data, file, indent=4)
            #     # file.write(existing_data)
            #     print(f'JSON data has been written to {file_path}')
            print(record_count)
        except Exception as e:
            print(f"An error occurred: {e}")

asyncio.run(main())
