from google.cloud import pubsub_v1
from datetime import datetime
import asyncio
import aiohttp
import json

project_id="dataeng-gowda-vipulp"
topic_id="trimet-topic"

publisher = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path(project_id, topic_id)

async def main():
  async with aiohttp.ClientSession() as session:
    try:
      with open("ids.txt", "r", encoding='utf-8') as busIds:
        print(f'Running the script on {datetime.now()}')
        record_count=0
        future = ""
        for bus_id in busIds:
          bus_url = f'https://busdata.cs.pdx.edu/api/getBreadCrumbs?vehicle_id={bus_id}'
          async with session.get(bus_url) as resp:
            if resp.status == 404:
              print(f'Skipping this id: {bus_id}')
            else:
              print(f'Content found for {bus_id}')
              breadcrumb_records = await resp.json()
              for record in breadcrumb_records:
                record_count += 1
                data = json.dumps(record)
                data = data.encode("utf-8")
                future = publisher.publish(topic_path, data)
            print(record_count)
            future.result()
    except Exception as e:
      print(f"An error occurred: {e}")
      
asyncio.run(main())