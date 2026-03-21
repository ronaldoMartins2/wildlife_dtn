import glob
import os

files = glob.glob('/home/abinadabe/wild_life/wildlife_dtn/scripts/Results/jaguar_mamiraua/map_9*.csv')
print(f"Found {len(files)} files.")
for f in sorted(files):
    if "interpolation" in f or "outliers" in f: continue
    try:
        with open(f, 'r') as fh:
            count = len(fh.readlines())
        print(f"{os.path.basename(f)}: {count}")
    except Exception as e:
        print(f"{os.path.basename(f)}: Error {e}")
