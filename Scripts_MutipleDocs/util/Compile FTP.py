import pandas as pd
import glob
path = r'Z:\FTP_CAPTURE_REPORT\1120S'
filenames = glob.glob(path + "/*.csv")
dfs = []
for filename in filenames:
    dfs.append(pd.read_csv(filename))
big_frame = pd.concat(dfs, ignore_index=True)
big_frame.to_csv(r'Z:\FTP_CAPTURE_REPORT\1120S\Result.csv')