import pandas as pd
import json

df = pd.read_parquet("spark-output")

record = json.loads(df.iloc[1]["value"])

print(json.dumps(record, indent=2, ensure_ascii=False))