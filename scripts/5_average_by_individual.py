import pandas as pd
from datetime import datetime
import sys

# python 5_average_by_individual.py map_94_interpolation_merged

#current_animal = sys.argv [1]
source = sys.argv [1]

interpolation = sys.argv[2] if len(sys.argv) > 2 else "default_value"

if len(sys.argv) > 1:
    # Read the CSV file into a DataFrame
    df = pd.read_csv(f'{source}.csv', header=None, names=['ID', 'Timestamp', 'Longitude', 'Latitude'])
#else:
    # Read the CSV file into a DataFrame
#    df = pd.read_csv(f'map_{current_animal}.csv', header=None, names=['ID', 'Timestamp', 'Longitude', 'Latitude'])

# Convert the 'Timestamp' column to datetime objects
df['Timestamp'] = pd.to_datetime(df['Timestamp'], format='%m/%d/%y %H:%M')

# Calculate the time differences between consecutive rows in hours
df['Time Difference (hours)'] = df['Timestamp'].diff().dt.total_seconds() / 3600

# Calculate the average time difference
average_time_difference = df['Time Difference (hours)'].mean()

# Print the result
print(f"Average time difference (in hours) between consecutive timestamps: {average_time_difference:.2f} hours len {len(df)}")