#!/usr/bin/env python 
from pathlib import Path
import pyarrow as pa
import pyarrow.parquet as pq
import pyarrow.dataset as ds


def run():
    basepth = Path(__file__).parent.parent
    (basepth / "pbp").mkdir(exist_ok=True)
    pth = basepth / 'nflfastR-data' / 'data'   
    for f in pth.glob('play_by_play*.parquet'):
        dataset = ds.parquet_dataset(str(f))
        part = ds.partitioning(
          pa.schema([("season", pa.int32())]),
          flavor="hive"
        )
        
        ds.write_dataset(dataset, basepth / 'pbp', format="parquet", partitioning=part)


if __name__ == '__main__':
    run()
