#!/usr/bin/env python 
from pathlib import Path
import pandas as pd


def run():
    basepth = Path(__file__).parent.parent
    pth = basepth / 'nflfastR-data' / 'data'
    df = pd.concat([pd.read_parquet(f) for f in pth.glob('play_by_play*.parquet')])
    df.to_parquet('playbyplay', partition_cols=['season'])
    for f in (basepth / 'playbyplay').glob('*'):
        print(f)


if __name__ == '__main__':
    run()
