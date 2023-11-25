import pandas as pd

print("Enter your url to download data:")
url = input()
print("enter file name to save without extension")
fl = input()

df = pd.read_parquet(url,engine='pyarrow')
df.to_csv(fl,index=False)
