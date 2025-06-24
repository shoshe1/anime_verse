import pandas as pd

df = pd.read_csv('/data/gold/final_report.csv')
assert not df.empty, "Gold layer is empty!"
print("✅ Gold data is valid and non-empty.")
