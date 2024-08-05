import pytz, logging
import numpy as np
import pandas as pd
from datetime import datetime

logger = logging.getLogger()
logger.setLevel(logging.INFO)
if len(logger.handlers) == 0:
    handler = logging.StreamHandler()
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    handler.setFormatter(formatter)
    logger.addHandler(handler)

def get_average_pedestrian_data(df):
    if df.empty:
        return pd.DataFrame()

    df['datetime'] = pd.to_datetime(df['datetime'], utc=True)

    filtered_data = df[df['numvisitors'] >= 0][['datetime', 'region', 'numvisitors']]

    melbourne_tz = pytz.timezone('Australia/Melbourne')
    filtered_data['datetime'] = filtered_data['datetime'].dt.tz_convert('UTC').dt.tz_convert(melbourne_tz)

    filtered_data = filtered_data[filtered_data['datetime'] <= '2021-06-30']

    avg_visitors_by_hour_and_region = filtered_data.groupby([filtered_data['region'], filtered_data['datetime'].dt.hour])['numvisitors'].mean().unstack()

    return avg_visitors_by_hour_and_region

def generate_synthetic_data(avg_visitors_by_hour_and_region):
    melbourne_tz = pytz.timezone('Australia/Melbourne')

    current_date = datetime.now(melbourne_tz)

    regions = avg_visitors_by_hour_and_region.index
    synthetic_data = []

    hour = current_date.hour
    for region in regions:
        lam = avg_visitors_by_hour_and_region.loc[region, hour] if hour in avg_visitors_by_hour_and_region.columns else 0
        if lam > 0:
            num_visitors = round(np.random.poisson(lam) / 12)
            synthetic_data.append({
                'datetime': current_date.isoformat(),
                'region': region,
                'numVisitors': num_visitors
            })

    synthetic_df = pd.DataFrame(synthetic_data)
    synthetic_df = synthetic_df.sort_values(by='datetime', ascending=False)
    
    return synthetic_df
