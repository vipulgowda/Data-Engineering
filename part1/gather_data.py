import asyncio
import aiohttp
from datetime import datetime
import os

async def main():
    async with aiohttp.ClientSession() as session:
        try:
            with open("ids.txt", "r", encoding='utf-8') as busIds:
                print(f'Running the script on {datetime.now()}')
                for bus_id in busIds:
                    bus_url = f'https://busdata.cs.pdx.edu/api/getBreadCrumbs?vehicle_id={bus_id}'
                    async with session.get(bus_url) as resp:
                        if resp.status == 404:
                            print(f'Skipping this id: {bus_id}')
                            pass
                        else:
                            print(f'Content found for {bus_id}')
                            current_date = datetime.now().strftime("%m%d%Y")
                            current_folder = f'breadcrumb_data/{current_date}/'
                            os.makedirs(current_folder, exist_ok=True)  # Create folder if it doesn't exist
                            file_name = f'{current_folder}' + resp.headers.get("Content-Disposition", "").split("=")[1]
                            resp_text = await resp.text()
                            with open(file_name, "w", encoding='utf-8') as file:
                                file.write(resp_text)
        except Exception as e:
            print(f"An error occurred: {e}")

asyncio.run(main())