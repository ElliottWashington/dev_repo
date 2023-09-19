import requests
import pandas as pd
from io import StringIO
import os
from datetime import datetime
import argparse

def change_url(date: str):
    # Convert the date string to a datetime object
    date_obj = datetime.strptime(date, "%Y-%m-%d")

    # Extract the year, month and day
    year = date_obj.year
    month = f"{date_obj.month:02d}"  # Pad with leading zero if necessary
    day = f"{date_obj.day:02d}"  # Pad with leading zero if necessary

    # Format the URL with the extracted year, month and day
    url = f"https://www.cboe.com/us/options/market_statistics/penny_tick_type/{year}/{month}/cone_options_rpt_penny_tick_type_{year}{month}{day}.csv-dl?mkt=cone"
    return url

def get_penny_list(date):
    # Use the function
    url = change_url(date)

    response = requests.get(url)
    assert response.status_code == 200, 'Failed to download data'

    data = StringIO(response.text)
    df = pd.read_csv(data)

    # Save the dataframe only if the file does not exist
    filename = f"pennies_pilot_{date}.csv"
    if not os.path.isfile(filename):
        df.to_csv(filename)
    return df

def run(date):
    penny_df = get_penny_list(date)
    penny_df = penny_df.drop(columns=['Tick Type', 'Underlying Description'])
    penny_df = penny_df[~penny_df['OSI Root'].str.startswith(tuple('0123456789'))]
    output_file = "/home/elliott/Development/files/penny.cfg"
    penny_df.to_csv(output_file, index=False, header=False, lineterminator='\n')


if __name__ == "__main__":
    # Parse command line arguments
    parser = argparse.ArgumentParser(description="Run queries for a given date")
    parser.add_argument("date", type=str, help="Date in YYYY-MM-DD format")
    args = parser.parse_args()

    # Call the main function with the date argument
    run(args.date)