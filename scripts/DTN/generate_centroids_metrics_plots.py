import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

def normalize_df(df, metric_name):
    # Converts wide format (8, 16, 32 as columns) to long format
    df_long = df.melt(id_vars=['method'], var_name='clusters', value_name=metric_name)
    df_long['clusters'] = df_long['clusters'].astype(int)
    # Standardize algorithm names for merging
    df_long['method'] = df_long['method'].str.lower().replace({'k-means': 'kmeans'})
    return df_long

# 1. Load and Prepare Data
df_spatial = pd.read_csv('scripts\\Results\\jaguar_mamiraua\\distances_to_own_centroid\\stats_animal_to_own_centroid.csv')
df_visits = pd.read_csv('scripts\\Results\\jaguar_mamiraua\\figuras_tempo_retorno\\table_mean_number_of_visits.csv')
df_return = pd.read_csv('scripts\\Results\\jaguar_mamiraua\\figuras_tempo_retorno\\table_mean_return_time_hours.csv')

# Normalize network metrics
visits = normalize_df(df_visits, 'Visits')
return_t = normalize_df(df_return, 'Return Time (h)')

# Merge spatial and network data
df_merged = df_spatial.merge(visits, left_on=['algorithm', 'clusters'], right_on=['method', 'clusters'])
df_merged = df_merged.merge(return_t, left_on=['algorithm', 'clusters'], right_on=['method', 'clusters'])

# Map algorithm names for a professional legend
df_merged['Algorithm'] = df_merged['algorithm'].str.replace('kmeans', 'K-Means').str.upper()

# 2. Scientific Plotting
sns.set_theme(style="whitegrid")
fig, axes = plt.subplots(1, 3, figsize=(18, 6))
fig.suptitle('Scientific Analysis: Network Performance vs. Infrastructure Cost (K)', fontsize=16)

# Subplot 1: Mean Distance (Signal Propagation Quality)
sns.barplot(data=df_merged, x='clusters', y='mean', hue='Algorithm', ax=axes[0])
axes[0].set_title('Geographic Proximity (Lower is better)')
axes[0].set_ylabel('Mean Distance (m)')
axes[0].set_xlabel('Number of Clusters (K)')
axes[0].axhline(250, color='red', linestyle='--', label='LoRA Limit (250m)')
axes[0].legend()

# Subplot 2: Mean Visits (Throughput/Contact Frequency)
sns.barplot(data=df_merged, x='clusters', y='Visits', hue='Algorithm', ax=axes[1])
axes[1].set_title('Contact Frequency (Higher is better)')
axes[1].set_ylabel('Average Visits')
axes[1].set_xlabel('Number of Clusters (K)')
axes[1].get_legend().remove()

# Subplot 3: Return Time (Latency/Update Interval)
sns.barplot(data=df_merged, x='clusters', y='Return Time (h)', hue='Algorithm', ax=axes[2])
axes[2].set_title('Waiting Time (Lower is better)')
axes[2].set_ylabel('Return Time (h)')
axes[2].set_xlabel('Number of Clusters (K)')
axes[2].get_legend().remove()

plt.tight_layout(rect=[0, 0.03, 1, 0.95])
plt.savefig('scientific_comparative_analysis.png')