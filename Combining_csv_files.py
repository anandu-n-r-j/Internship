import os
import csv

# Path to the folder containing the original .csv files
folder_path = r'C:\Users\anand\OneDrive\Documents\Intern_Project\yt_reports_csv\appended_channel_id'

# Path to the new folder to store combined .csv files
new_folder_path = os.path.join(folder_path, 'combined_csv_files')

# Create a new directory to store combined .csv files
if not os.path.exists(new_folder_path):
    os.mkdir(new_folder_path)
    print(f"New folder '{new_folder_path}' created.")

# Dictionary to store file contents based on their first names
file_contents = {}

# Loop through each file in the directory
for filename in sorted(os.listdir(folder_path)):  # Sort files alphabetically
    if filename.endswith('.csv'):
        # Extract the first part of the file name
        first_name = filename.split('_')[0]
        last_name = filename.split('_')[-1]

        # Read file content and header
        with open(os.path.join(folder_path, filename), 'r', newline='') as file:
            csv_reader = csv.reader(file)
            content = list(csv_reader)  # Read all rows

        # Ensure only one header row
        if len(content) > 1 and content[1:] and content[0] == content[1]:
            content = content[1:]  # Remove duplicate header

        # Add header and content to dictionary
        if first_name not in file_contents:
            file_contents[first_name] = {'header': content[0] if content else [], 'rows': []}
        file_contents[first_name]['rows'].extend(content[1:])

# Write combined content to a single .csv file for each first_name
for first_name, data in file_contents.items():
    combined_filename = f'{first_name}_combined_{last_name}.csv'
    combined_file_path = os.path.join(new_folder_path, combined_filename)

    with open(combined_file_path, 'w', newline='') as combined_file:
        csv_writer = csv.writer(combined_file)

        # Write the header only once
        if data['header']:
            csv_writer.writerow(data['header'])

        # Write the data rows
        csv_writer.writerows(data['rows'])

print("Combining .csv files and saving in a new folder is complete.")
