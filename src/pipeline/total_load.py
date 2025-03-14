
import requests
import pandas as pd
from datetime import datetime
from tqdm import tqdm
from huggingface_hub import login
import os
from loguru import logger

from src.utils import upload_dataframe_hf

URL_LIVE = 'https://opendata.elia.be/api/explore/v2.1/catalog/datasets/ods002/exports/json?limit=-1&timezone=UTC&use_labels=false'


def load_realtime_forecasts():
   result = requests.get(URL_LIVE)
   df = pd.DataFrame.from_records(result.json())
   df['datetime'] = pd.to_datetime(df['datetime'])
   df.sort_values('datetime', inplace=True)
   return df

def generate_url_history(valuedate: datetime):
  year = valuedate.year
  month = str(valuedate.month).zfill(2)
  day = str(valuedate.day).zfill(2)
  return f'https://opendata.elia.be/api/explore/v2.1/catalog/datasets/ods001/exports/json?limit=-1&timezone=UTC&use_labels=false&epsg=4326&refine=datetime%3A%22{year}%2F{month}%2F{day}%22'


def load_history_forecasts(start_date: datetime, end_date: datetime):
   '''
   Load historical data over a time range
   '''

   date_range = pd.date_range(start_date, end_date)
   df_list = []
   pbar = tqdm(date_range)
   for date in pbar:
        try:
            pbar.set_description(f"Loading {date}")
            url = generate_url_history(date)

            result = requests.get(url)
            df = pd.DataFrame.from_records(result.json())

            df_list.append(df)
        except Exception as e:
            logger.warning(f'{date}: {e}')
            continue
   output_df = pd.concat(df_list)
   output_df['datetime'] = pd.to_datetime(output_df['datetime'])
   output_df.sort_values('datetime', inplace=True)
   output_df.reset_index(drop=True, inplace=True)
   return output_df



def TotalloadHistoryPipeline(start_date: datetime, end_date: datetime):
   '''
   Download historycal measures and append to existing dataset
   '''

   df_history = load_history_forecasts(start_date=start_date, end_date=end_date)
   login(token=os.getenv('HF_TOKEN'), write_permission=True) 
   upload_dataframe_hf(df=df_history, filename='tso/total_load_history.parquet', subset_drop_duplicates=['datetime'], concat=True)



def TotalloadRealtimePipeline():
   '''
   Pipeline to download real time measures and append to existing dataset
   '''

   logger.info('Download realtime measures')
   df_live = load_realtime_forecasts()   

   logger.info('Update total_load')
   login(token=os.getenv('HF_TOKEN'), write_permission=True)
   upload_dataframe_hf(df=df_live, filename='tso/total_load_realtime.parquet', subset_drop_duplicates=['datetime'], concat=False)
   upload_dataframe_hf(df=df_live, filename='tso/total_load_history.parquet', subset_drop_duplicates=['datetime'], concat=True)


   logger.info('Done')


if __name__ == '__main__':
   
   TotalloadHistoryPipeline(start_date=datetime(2017, 1, 1), end_date=datetime(2024, 12, 18))
   #TotalloadRealtimePipeline()