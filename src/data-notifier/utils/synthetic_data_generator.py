import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import pytz
import requests

def get_data():
    url = "https://data.melbourne.vic.gov.au//api/explore/v2.1/catalog/datasets/pedestrian-historical-data/exports/json"
    response = requests.get(url)

    if response.status_code == 200:
        try:
            data = response.json()  
        except requests.exceptions.JSONDecodeError:
            print("Failed to decode JSON response")
    else:
        print("Failed to fetch data. Status code:", response.status_code)
    df = pd.DataFrame(data)
    return df

def clean_data(df):
    # Convert datetime column to datetime type
    df['datetime'] = pd.to_datetime(df['datetime'], utc=True)

    # Filter out negative visitors and select necessary columns
    filtered_data = df[df['numvisitors'] >= 0][['datetime', 'region', 'numvisitors']]

    # Convert to Melbourne timezone
    melbourne_tz = pytz.timezone('Australia/Melbourne')
    filtered_data['datetime'] = filtered_data['datetime'].dt.tz_convert('UTC').dt.tz_convert(melbourne_tz)

    # Filter data up to June 30, 2021
    filtered_data = filtered_data[filtered_data['datetime'] <= '2021-06-30']

    # Calculate average visitors by hour and region
    avg_visitors_by_hour_and_region = filtered_data.groupby([filtered_data['region'], filtered_data['datetime'].dt.hour])['numvisitors'].mean().unstack()

    # Define the date range for synthetic data
    synthetic_start_date = melbourne_tz.localize(datetime(2024, 3, 1, 0, 0, 0))
    current_date = datetime.now(melbourne_tz)

    # Create synthetic data using Poisson distribution
    regions = filtered_data['region'].unique()
    synthetic_data = []
    current = synthetic_start_date

    while current <= current_date:
        hour = current.hour
        for region in regions:
            lam = avg_visitors_by_hour_and_region.loc[region, hour] if hour in avg_visitors_by_hour_and_region.columns else 0
            if lam > 0:
                num_visitors = np.random.poisson(lam)
                synthetic_data.append({
                    'datetime': current,
                    'region': region,
                    'numVisitors': num_visitors
                })
        current += timedelta(hours=1)

    # Create DataFrame from synthetic data
    synthetic_df = pd.DataFrame(synthetic_data)
    return synthetic_df

def main_synth():
    df = get_data()
    df_synth = clean_data(df)
    return df_synth