import os
import sys
import pandas as pd

path, file = os.path.split(sys.argv[1])
files = []
files.append(file)
if not file.endswith('.xlsx'):
    path = sys.argv[1]
    files = os.listdir(sys.argv[1])
    files = [item for item in files if item.endswith('.xlsx')]
    files = [item for item in files if not item.startswith('~')]

for file in files:
    print("Reading {}".format(file))
    df = pd.read_excel(os.path.join(path, file))
    result = df[df['id']==sys.argv[2]]
    if len(result)>0:
        print(file, len(df))
        print(list(df.columns))
        print(result)
