#!/usr/bin/env python 
from pathlib import Path
import numpy as np
import pandas as pd

def run():
    basepth = Path(__file__).parent.parent
    (basepth / "pbp").mkdir(exist_ok=True)
    pth = basepth / 'nflfastR-data' / 'data'   
    for f in pth.glob('play_by_play*.parquet'):
        df = pd.read_parquet(f)
        cols = df.select_dtypes('float')
        df.loc[:, cols] = df.loc[:, cols].astype(np.float32)
        df.to_parquet(basepth / 'pbp', partition_cols=['season'])


if __name__ == '__main__':
    run()
