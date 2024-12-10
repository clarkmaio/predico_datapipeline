
import pandas as pd

from huggingface_hub import upload_file
import polars as pl
from retrying import retry
from typing import List


def read_parquet(url: str):
    df = pl.read_parquet(url).to_pandas().drop('__index_level_0__', axis=1, errors='ignore')
    df = df.drop('__index_level_0__', axis=1, errors='ignore') # Drop useless column just in case

    # Cast valuedate
    DATE_COLUMNS=['datetime', 'time', 'valid_time']
    for c in DATE_COLUMNS:
        if c in df.columns:
            df[c] = pd.to_datetime(df[c])
    return df




#@retry(wait_random_min=3000, wait_random_max=5000)
def upload_dataframe_hf(df: pd.DataFrame, filename: str, subset_drop_duplicates: List[str] = None, concat: bool =False) -> None:
    '''
    Upload dataframe to huggingface dataset as raw file.

    df: datafrae to upload
    filename: path where the file will be stored in hf dataset space
    concat: If True concat to existing dataframe and drop duplicates
    '''
    import io
    buffer = io.BytesIO()

    if concat:
        try:
            current_data = read_parquet(url=f'https://huggingface.co/datasets/clarkmaio/elia_predico/resolve/main/{filename}')
            df = pd.concat([current_data, df])
            df = df.drop_duplicates(keep='last', subset = subset_drop_duplicates)
        except pl.exceptions.ComputeError:
            print(f'{filename} does not exist in hf dataset. Dataframe will be uplaoded directly')
    
    df.to_parquet(buffer, engine='pyarrow')

    upload_file(path_or_fileobj=buffer, 
                repo_type='dataset',
                path_in_repo=filename,
                repo_id='clarkmaio/elia_predico')
