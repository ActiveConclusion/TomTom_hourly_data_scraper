from numpy import save
import pandas as pd
import numpy as np
from pathlib import Path


SCRAPED_DATA_FILE = 'scraped_data.csv'
TRAFFIC_INDEX_FILE = 'tomtom_traffic_index.csv'
COLUMNS = [
    'country', 'city', 'jams_count', 'jams_delay', 'jams_length', 
    'traffic_index_live', 'update_datetime_live', 'update_time_live',
    'traffic_index_week_before', 'update_datetime_week_before', 
    'update_time_week_before',
]


def process_scraped_data():
    data = pd.read_csv(SCRAPED_DATA_FILE, low_memory=False)
    data['update_datetime_live'] = pd.to_datetime(
        data['update_time_live'], unit='ms'
    )
    data['update_datetime_week_before'] = pd.to_datetime(
        data['update_time_week_before'], unit='ms'
    )
    return data

def merge_with_historical_data(new_data):
    historical_data = pd.read_csv(TRAFFIC_INDEX_FILE, low_memory=False)
    updated_data = pd.concat([historical_data, new_data]).reset_index(
        drop=True
    )
    updated_data.drop_duplicates(
        subset=['country', 'city', 'update_time_live'], 
        keep='last', 
        inplace=True,
    )
    return updated_data

def save_data(data):
    data = data.sort_values(
        by=["country", "city", "update_time_live"]).reset_index(
        drop=True
    )
    data.to_csv(TRAFFIC_INDEX_FILE, index=False, columns=COLUMNS)

if __name__ == "__main__":
    data = process_scraped_data()
    if Path(TRAFFIC_INDEX_FILE).is_file():
        data = merge_with_historical_data(data)
    save_data(data)
    

