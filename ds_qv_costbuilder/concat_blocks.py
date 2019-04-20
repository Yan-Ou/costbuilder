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

for index, file in enumerate(xlsx_files):
    print("Reading {}".format(file))
    df = pd.read_excel(os.path.join(path, file))
    df['file'] = file.split(".")[0]
    print(file, len(df))
    print(list(df.columns))
    if index == 0:
        all_blocks = df[['item_block','id','formula','file']]
    else:
        all_blocks = pd.concat([all_blocks, df[['item_block','id','formula','file']]])

all_blocks.to_csv("all_blocks.csv", index=False)
print("Saved all_blocks.csv")