import requests
import pandas as pd
from bs4 import BeautifulSoup
import io

url = "https://www.ncei.noaa.gov/data/local-climatological-data/access/2021/"

def get_html(url: str):
    """
    Returns the HTML content from a URL. 
    """
    try:
        response = requests.get(url, allow_redirects=True)
        response.raise_for_status()
        
        return response.text
    except requests.exceptions.RequestException as e:
        print("Error getting response from url:", e)

def parse_html_bs4(html):
    """
    Parses HTML content and finds table in it.
    """
    if html is None:
        return None
    try:
        soup = BeautifulSoup(html, 'html.parser')
        table = soup.find('table')

        return table 
    except Exception as e:
        print("Error processing html ", e)

def extract_files_from_table(table, target_date):
    """
    Extracts files from table that were "Last modified" in the target_date and appends them in a list.
    List with all CSV files is returned. 
    """
    if table is None:
        return None
    try:
        files_date = []

        rows = table.find_all('tr')
        # print(rows)

        for row in rows:
            tds = row.find_all('td')
            if len(tds) >= 2:
                name_file = tds[0]
                date_file = tds[1]
                date_file_text = date_file.get_text(strip=True)
                name_file_text = name_file.get_text(strip=True)

                if date_file_text == target_date:
                    files_date.append(name_file_text)
        return files_date
    except Exception as e:
        print("Error getting file ids:", e)

def csv_read_pandas(list, url):
    """
    Gets the first element of the list of CSVs and reads it. 
    Prints the with the highest `HourlyDryBulbTemperature`
    """
    if len(list) > 0:
        file_url = f"{url}{list[0]}"
        csv = requests.get(file_url).content
        df = pd.read_csv(io.StringIO(csv.decode('utf-8')))

        df_sorted = df.sort_values('HourlyDryBulbTemperature',ascending=False).iloc[:5,]
        print(df_sorted)
   
    
    

def main():
    url = "https://www.ncei.noaa.gov/data/local-climatological-data/access/2021/"
    html = get_html(url)
    file_table = parse_html_bs4(html)
    file_date_list = extract_files_from_table(file_table, "2022-02-07 14:03")
    csv_read_pandas(file_date_list, url)
   
    #Get max row
    # print(df[df['HourlyDryBulbTemperature']==df['HourlyDryBulbTemperature'].max()])
    

if __name__ == "__main__":
    main()

