
import xarray as xr
import fsspec
from tqdm import tqdm
from datetime import datetime
import pandas as pd
from typing import List, Tuple
from itertools import product
import os
from huggingface_hub import login
from typing import List, Iterable
from copy import deepcopy
import s3fs
from loguru import logger

from src.utils import upload_dataframe_hf

MAX_STEP = 51
ONSHORE_BOX={'latitude': (51.5, 49.5), 'longitude': (2.5, 6.5)}
OFFSHORE_BOX={'latitude': (), 'longitude': ()}

def uri_generator(forecastdate: datetime, run: str, step: int) -> str:
    step_str = str(step).zfill(3)
    return f'simplecache::s3://noaa-gefs-pds/gefs.{forecastdate.strftime("%Y%m%d")}/{run}/atmos/pgrb2sp25/geavg.t{run}z.pgrb2s.0p25.f{step_str}'


def build_dataset(uri: str) -> pd.DataFrame:
    file = fsspec.open_local(uri, s3={'anon': True}, filecache={'cache_storage':'/tmp/'})

    # 10m variables
    m10 = xr.open_dataset(file, 
                     engine="cfgrib", 
                     filter_by_keys={'typeOfLevel': 'heightAboveGround', 'level': 10}
                     )
    m10 = m10.sel(latitude=slice(51.5, 49.5), longitude=slice(2.5, 6.5)).to_dataframe()
    m10.reset_index(inplace=True)


    # Surface variables
    surface = xr.open_dataset(file, 
                     engine="cfgrib", 
                     filter_by_keys={'typeOfLevel': 'surface'}
                     )

    surface = surface.sel(latitude=slice(51.5, 49.5), longitude=slice(2.5, 6.5))
    surface = surface[['gust', 'cape']].to_dataframe()
    surface.reset_index(inplace=True)

    data = pd.merge(m10, surface, on=['latitude', 'longitude', 'valid_time', 'step', 'time'])
    data.drop(['heightAboveGround', 'surface'], axis=1, inplace=True)

    os.remove(file)
    return data

def download_forecastrun(date: datetime, run: str, steps: List[int]) -> pd.DataFrame:
    '''
    Download data from a single forecastrun
    '''

    run_data = []
    for step in steps:
        try:
            if (run == '06') and step > 42:
                continue

            uri = uri_generator(forecastdate=date, run=run, step=step)
            tmp = build_dataset(uri=uri)
            run_data.append(tmp)
        except Exception as e:
            continue

    run_data = pd.concat(run_data, axis=0)
    return run_data


def deduce_gfs_lastrun() -> Tuple:
    '''
    Return last forecastdate and run available on aws
    '''
    fs = s3fs.S3FileSystem(anon=True)
    folders = fs.ls('s3://noaa-gefs-pds/')
    folders.sort()

    lastrundate_path=folders[-2]
    lastrundate = datetime.strptime(lastrundate_path.split('/')[-1].replace('gefs.', ''), '%Y%m%d')
    lastrun_path = fs.ls(f's3://{lastrundate_path}')[-1]
    lastrun = lastrun_path.split('/')[-1]
    return lastrundate, lastrun

def GfsHistoryPipeline(date_range: Iterable, runs: List[str] = ['00', '06'], steps: List[int] = list(range(0, MAX_STEP, 3))):
    '''
    Download historical weather data from aws
    '''

    PRODUCT = product(date_range, runs)
    LENGTH=len(list(deepcopy(PRODUCT)))
    
    login(token=os.getenv('HF_TOKEN'), write_permission=True)


    pbar = tqdm(PRODUCT, total=LENGTH) 
    for (d, run) in pbar: 
        pbar.set_postfix_str(f'{d} {run}')  
        run_data = download_forecastrun(date=d, run=run, steps=steps)
        upload_dataframe_hf(df=run_data, filename='gfs_history.parquet', concat=True)


def GfsLastrunPipeline():
    '''
    Download last run available from gfs bucket and upload on huggingface
    '''
    steps = list(range(0, MAX_STEP, 3))

    lastrundate, lastrun = deduce_gfs_lastrun()
    logger.info(f'Download run {lastrundate.strftime("%Y-%m-%d")} {lastrun}z')
    run_data = download_forecastrun(date=lastrundate, run=lastrun, steps=steps)

    login(token=os.getenv('HF_TOKEN'), write_permission=True)

    logger.info('Write gfs_lastrun.parquet')
    upload_dataframe_hf(df=run_data, filename='weather/gfs_lastrun.parquet', concat=False)

    logger.info('Update gfs_history.parquet')
    upload_dataframe_hf(df=run_data, filename='weather/gfs_history.parquet', concat=True)

    logger.info('Done')



if __name__ == '__main__':
    GfsLastrunPipeline()