import sys
import pandas as pd

print(f'Pandas version: {pd.__version__}')
print(f'Sys arguments: {sys.argv}')

day = sys.argv[1]

print(f'Job finished successfully for day: {day}')
