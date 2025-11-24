# %%
import pandas as pd
import glob
import os
import re

ROOT = "./research_network"
loaded_tables = {}

# find folders that contain part.0.parquet
folders = sorted(set(os.path.dirname(p) for p in glob.glob(f"{ROOT}/**/part.0.parquet", recursive=True)))

for folder in folders:
    folder_name = os.path.basename(folder)
    table_name = re.sub(r"\.parquet$", "", folder_name)

    part_files = sorted(glob.glob(os.path.join(folder, "part.*.parquet")))
    if not part_files:
        print(f"⚠️ No part files found in {folder}")
        continue

    dfs = []
    for f in part_files:
        try:
            dfs.append(pd.read_parquet(f))
        except Exception as e:
            print(f"❌ Error reading {f}: {e}")

    df = pd.concat(dfs, ignore_index=True)

    # rename exact 'code' columns to {table_name}_code
    rename_map = {c: f"{table_name}_code" for c in df.columns if c.strip().lower() == "code"}
    if rename_map:
        df.rename(columns=rename_map, inplace=True)
        print(f"✅ Renamed in '{table_name}': {rename_map}")

    df["source_name"] = table_name
    loaded_tables[table_name] = df
    globals()[table_name] = df  # optional

    print(f"✅ Loaded '{table_name}' ({len(part_files)} parts) → shape {df.shape}")

print("\nSummary of merged tables:")
for name, df in loaded_tables.items():
    print(f"  • {name:<20} {df.shape[0]} rows × {df.shape[1]} cols")


# %%
# --- Use the already-loaded procedure table ---
df = loaded_tables['procedure'].copy()
print(f"Procedure table shape: {df.shape}")
print(df.head())


