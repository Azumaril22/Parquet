import pandas as pd

df = pd.DataFrame(columns=['id', 'username', 'email'])
df.to_parquet('parquetapp/data/users.parquet')