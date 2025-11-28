from dune_client.client import DuneClient
import pandas as pd

# Initialize client
dune = DuneClient("vsGtjVNtCbs3sSV7bfKi7AhBe1lKEcBz")

# Fetch latest result
query_result = dune.get_latest_result(6233410)

# Convert result records to DataFrame
df = pd.DataFrame(query_result.result.rows)

# Save to CSV
df.to_csv("dune_results.csv", index=False)

print("CSV saved as dune_results.csv")
