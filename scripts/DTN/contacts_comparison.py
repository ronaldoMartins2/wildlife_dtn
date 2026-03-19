import pandas as pd
import os


def get_files(path):
    files_list = []
    for file in os.listdir(path):
        if file.startswith("down_contact") and file.endswith("_rawdata.csv"):
            files_list.append(file)
    return files_list


def generate_contact_table(file_list):
    consolidated_data = []

    for file in file_list:
        file_path = os.path.join("scripts", "Results", "jaguar_mamiraua", "contacts", file)
        if os.path.exists(file_path):
            # Load the CSV file
            df = pd.read_csv(file_path)
            
            # Extract info from filename
            # Example: 'down_contact_93_centroids_8_birch_rawdata.csv'
            parts = file.split('_')
            num_centroids = int(parts[4])  # Extract 8, 16, or 32
            algorithm = parts[5].upper()   # Extract algorithm name
            
            # Count registered contacts
            # We assume each 'up' state represents the start of a contact
            num_contacts = len(df[df['state'] == 'up'])
            
            consolidated_data.append({
                'Algorithm': algorithm,
                'Centroids': num_centroids,
                'Contacts': num_contacts
            })
        else:
            print(f"Warning: File {file} not found.")

    # Create a DataFrame with the results
    summary_df = pd.DataFrame(consolidated_data)

    # Create a Pivot Table for better comparison
    pivot_table = summary_df.pivot_table(index='Algorithm', columns='Centroids', values='Contacts', aggfunc='sum', margins=True, margins_name='Total')
    
    return summary_df, pivot_table

files = get_files(r"scripts\Results\jaguar_mamiraua\contacts")

# Execute the script
detailed_results, comparison_pivot = generate_contact_table(files)

# Save the results to CSV files
detailed_results.to_csv('contact_details.csv', index=False)
comparison_pivot.to_csv('centroid_comparison_table.csv')

print("Processing complete!")
print("\nComparison Pivot Table:")
print(comparison_pivot)