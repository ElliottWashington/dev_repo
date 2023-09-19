import requests
import csv
import os
from datetime import datetime, timedelta

# Define the URL of the CSV file
url = 'https://home.treasury.gov/resource-center/data-chart-center/interest-rates/daily-treasury-rates.csv/2023/all?type=daily_treasury_yield_curve&field_tdr_date_value=202301'

# Send a GET request to the URL to download the CSV data
response = requests.get(url)

new_file_path = '/opt/scalp/rates/daily-treasury-rates.csv'
old_dir = '/opt/scalp/rates/old/'

# Get yesterday's date
yesterday = datetime.now() - timedelta(days=1)
yesterday_str = yesterday.strftime('%Y-%m-%d')

if response.status_code == 200:
    # Check if the old file exists
    if os.path.exists(new_file_path):
        # Move the old file to the /old directory and append the previous date to the file name
        old_file_path = os.path.join(old_dir, f'daily-treasury-rates_{yesterday_str}.csv')
        os.rename(new_file_path, old_file_path)

    # Save the new file
    with open(new_file_path, 'wb') as f:
        f.write(response.content)
    
    # Change file permissions to 700
    os.chmod(new_file_path, 0o700)

    # Open the new file and read the first two rows
    with open(new_file_path, 'r') as f:
        reader = csv.reader(f)
        first_row = next(reader)
        second_row = next(reader)

        print('First row:', first_row)
        print('Second row:', second_row)

else:
    print('Error:', response.status_code)
