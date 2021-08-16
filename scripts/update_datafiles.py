#!/usr/bin/env python 
import os
from pathlib import Path
import re
import tempfile

import requests
import numpy as np
import pandas as pd


def dl_datafiles(urls, pth):
    """Downloads datafiles from github
    
    Args:
        urls (List[str]): list of URL str
        pth (Path): directory to save int

    Returns:
        None

    """
    for url in urls:
        r = requests.get(url, stream=True)
        if "Content-Disposition" in r.headers.keys():
            fname = pth / re.findall("filename=(.+)", r.headers["Content-Disposition"])[0]
        else:
            fname = pth / url.split("/")[-1]
        if r.ok:
            with fname.open('wb') as f:
                for chunk in r.iter_content(chunk_size=1024 * 8):
                    if chunk:
                        f.write(chunk)
                        f.flush()
                        os.fsync(f.fileno())


def nflfastr_datafiles():
    """Gets list of files from github nflfastR-data files"""
    url = 'https://api.github.com/repos/nflverse/nflfastR-data/contents/data'
    headers = {"Accept": "application/vnd.github.v3+json"}
    r = requests.get(url, headers=headers)
    return r.json()


def run():
    basepth = Path(__file__).parent.parent
    (basepth / "pbp").mkdir(exist_ok=True)
    rtpth = Path(os.getenv('RUNNER_TEMP'))
    urls = [item['download_url'] for item in nflfastr_datafiles()
            if 'parquet' in item['download_url'] and 
            '2020' in item['download_url'] and 
            item['type'] == 'file']
    dl_datafiles(urls, rtpth)

    for f in rtpth.glob('play_by_play*.parquet'):
        print(f)
        df = pd.read_parquet(f)
        cols = df.select_dtypes('float').columns
        df.loc[:, cols] = df.loc[:, cols].astype(np.float32)
        df.to_parquet(basepth / 'pbp', partition_cols=['season'])


if __name__ == '__main__':
    run()
