
import xarray as xr
import fsspec
from tqdm import tqdm
from datetime import datetime
import pandas as pd
from typing import List
from itertools import product
import os
from huggingface_hub import login
from typing import List, Iterable
from copy import deepcopy
from dotenv import load_dotenv

from utils import upload_dataframe_hf

SECRET = load_dotenv('.secret')


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




def GfsPipeline(date_range: Iterable, runs: List[str] = ['00', '06'], steps: List[int] = list(range(0, 51, 3))):
    '''
    '''

    PRODUCT = product(date_range, runs)
    LENGTH=len(list(deepcopy(PRODUCT)))
    
    login(token=SECRET['HF_TOKEN'], write_permission=True)


    pbar = tqdm(PRODUCT, total=LENGTH) 
    for (d, run) in pbar: 
        run_data = []
        for step in steps:
            pbar.set_postfix_str(f'{d} {run} {step}')
            try:
                if (run == '06') and step > 42:
                    continue

                uri = uri_generator(forecastdate=d, run=run, step=step)
                tmp = build_dataset(uri=uri)
                run_data.append(tmp)
            except Exception as e:
                continue
    
        run_data = pd.concat(run_data, axis=0)
        upload_dataframe_hf(df=run_data, filename='gfs_history.parquet', concat=True)
