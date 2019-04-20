import os
import sys
import pandas as pd

file = sys.argv[1]
xlsx_files = []
if file.endswith('.xlsx'):
    xlsx_files.append(file)
    path = '.'
else:
    path = sys.argv[1]
    xlsx_files = os.listdir(path)
    xlsx_files = [item for item in xlsx_files if item.endswith('.xlsx')]
    xlsx_files = sorted([item for item in xlsx_files if not item.startswith('~')])

for file in xlsx_files:
    print("Reading {}".format(file))
    df = pd.read_excel(os.path.join(path, file))
    print(file, len(df))
    print(list(df.columns))
    print(df[:3])
