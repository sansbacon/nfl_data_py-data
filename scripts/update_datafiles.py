#!/usr/bin/env python 
from pathlib import Path

def run():
    pth = Path(__file__).parent.parent
    for f in pth.glob('*'):
        print(f)


if __name__ == '__main__':
    run()
