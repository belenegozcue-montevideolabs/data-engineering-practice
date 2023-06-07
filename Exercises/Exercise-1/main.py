import os
import requests, zipfile, io
import aiohttp
import asyncio
from datetime import datetime

download_uris = [
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2018_Q4.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q1.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q2.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q3.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q4.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2020_Q1.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2220_Q1.zip",
]

def createFolder(directory: str):
    """
    Creates a downloads folder on the given directory.
    """
    try:
        if not os.path.exists(directory):
            os.makedirs(directory)
    except OSError:
        print('Error: Creating directory. ' +  directory)

def main():
    try:
        for url in download_uris:
            req = requests.get(url)

            filename = url.split('/')[-1].split('.')[0] 
            z = zipfile.ZipFile(io.BytesIO(req.content))
            z.extract(f"{filename}.csv", path="/Users/belenegozcue/Desktop/data-engineering-practice/Exercises/Exercise-1/downloads/")
            print('Csv downloaded!')
    except zipfile.BadZipFile as e:
        print('Error occured while processing ZIP file: ', e, filename)

# WAY TO DO IT ASYNCHRONOURS USING aiohttp

# async def download_and_extract(url):
#     async with aiohttp.ClientSession() as session:
#         try:
#             async with session.get(url, ssl=False) as response:
#                 filename = url.split('/')[-1].split('.')[0]
#                 content = await response.read()

#                 with zipfile.ZipFile(io.BytesIO(content)) as z:
#                     z.extract(f"{filename}.csv", path="/Users/belenegozcue/Desktop/data-engineering-practice/Exercises/Exercise-1/downloads/")
#                 print(f"{filename}.csv downloaded!")
#         except zipfile.BadZipFile as e:
#             print('Error occured while processing ZIP file: ', e, filename)
                

# async def download():
#     tasks = []
#     for url in download_uris:
#         tasks.append(asyncio.ensure_future(download_and_extract(url)))

#     await asyncio.gather(*tasks)
#     print('Already downloaded all of the files!')


if __name__ == "__main__":
    now = datetime.now()
    print(now)
    createFolder('/Users/belenegozcue/Desktop/data-engineering-practice/Exercises/Exercise-1/downloads/')
    # asyncio.run(download())
    main()
    end = datetime.now()
    print(end - now)
