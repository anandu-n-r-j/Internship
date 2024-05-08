import csv
import os

# Function to convert .txt to .csv
def txt_to_csv(txt_filename, csv_filename):
    with open(txt_filename, 'r') as txt_file, open(csv_filename, 'w', newline='') as csv_file:
        csv_writer = csv.writer(csv_file)
        for line in txt_file:
            # Modify this logic based on how your data is structured in the .txt files
            # For instance, split the line by a specific delimiter
            data = line.strip().split(',')  # Change the delimiter if necessary
            csv_writer.writerow(data)

# Path to the folder containing .txt files
folder_path = 'C:\\Users\\anand\\OneDrive\\Documents\\Intern_Project\\yt_reports'

# Check if the folder path exists
if os.path.exists(folder_path):
    # Iterate through each file in the folder
    for filename in os.listdir(folder_path):
        if filename.endswith('.txt'):
            # Construct paths for input (.txt) and output (.csv) files
            txt_file_path = os.path.join(folder_path, filename)
            csv_file_path = os.path.join(folder_path, filename[:-4] + '.csv')  # Change the extension

            # Convert .txt to .csv
            txt_to_csv(txt_file_path, csv_file_path)
            
            print(f"Converted {filename} to {filename[:-4]}.csv")

            # Remove the original .txt file
            os.remove(txt_file_path)
            print(f"Removed {filename}")
    print("Conversion and removal complete!")
else:
    print("Folder path does not exist.")
