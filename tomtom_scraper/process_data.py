import pandas as pd


SCRAPED_DATA_FILE = 'scraped_data.csv'
TRAFFIC_INDEX_FILE = 'tomtom_traffic_index.csv'


def process_scraped_data():
    scraped_data = pd.read_csv(SCRAPED_DATA_FILE, low_memory=False)
    scraped_data['update_time_live'] = pd.to_datetime(
        scraped_data['update_time_live'], unit='ms'
    )
    print(scraped_data.head())


if __name__ == "__main__":
    process_scraped_data()

