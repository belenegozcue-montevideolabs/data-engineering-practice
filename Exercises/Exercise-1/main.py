from functools import partial
import os
import requests, zipfile, io
import aiohttp
import asyncio
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor

download_uris = [
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2018_Q4.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q1.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q2.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q3.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q4.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2020_Q1.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2220_Q1.zip",
]

def extract_download_csv(url, path):
    try:
        req = requests.get(url)

        filename = url.split('/')[-1].split('.')[0] 
        z = zipfile.ZipFile(io.BytesIO(req.content))
        z.extract(f"{filename}.csv", path=path)
        print('Csv downloaded!')
    except zipfile.BadZipFile as e:
        print('Error occured while processing ZIP file: ', e, filename)

def createFolder(directory: str):
    """
    Creates a downloads folder on the given directory.
    """
    try:
        if not os.path.exists(directory):
            os.makedirs(directory)
    except OSError:
        print('Error: Creating directory. ' +  directory)

# NOT ASYNCHRONOUS

def main():
    print('You are now running MAIN')
    for url in download_uris:
        extract_download_csv(url, "/Users/belenegozcue/Desktop/data-engineering-practice/Exercises/Exercise-1/downloads/")


# WAY TO DO IT ASYNCHRONOURS USING aiohttp

async def download_and_extract_async(url):
    async with aiohttp.ClientSession() as session:
        try:
            async with session.get(url, ssl=False) as response:
                filename = url.split('/')[-1].split('.')[0]
                content = await response.read()

                with zipfile.ZipFile(io.BytesIO(content)) as z:
                    z.extract(f"{filename}.csv", path="/Users/belenegozcue/Desktop/data-engineering-practice/Exercises/Exercise-1/downloads/")
                print(f"{filename}.csv downloaded!")
        except zipfile.BadZipFile as e:
            print('Error occured while processing ZIP file: ', e, filename)
                

async def download_async():
    tasks = []
    for url in download_uris:
        tasks.append(asyncio.ensure_future(download_and_extract_async(url)))

    await asyncio.gather(*tasks)
    print('Already downloaded all of the files!')

# ThreadPoolExecutor

def thread_pool_executor():
    print('started runnning TPE')
    start = datetime.now() 

    with ThreadPoolExecutor(max_workers=15) as executor:
        # Partial function helps pass certain parameters to function
        pre_function = partial(extract_download_csv, path="/Users/belenegozcue/Desktop/data-engineering-practice/Exercises/Exercise-1/downloads/" )
        executor.map(pre_function, download_uris)

    end = datetime.now()
    print("TIEMPO", end-start)

"""
  Tiempos:
  not async: 3:38
  async: 1:24
  thread_pool_executor: 1:30
"""
if __name__ == "__main__":
    now = datetime.now()
    # print(now)
    createFolder('/Users/belenegozcue/Desktop/data-engineering-practice/Exercises/Exercise-1/downloads/')
    thread_pool_executor()
    #asyncio.run(download_async())
    #main()
    end = datetime.now()
    print(end - now)

