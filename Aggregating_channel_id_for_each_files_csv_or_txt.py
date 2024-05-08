import csv
import os
import pandas as pd

# Function to convert .txt to .csv and add channel_id to file name
def process_files(folder_path):
    for filename in os.listdir(folder_path):
        if filename.endswith('.txt'):
            # Construct paths for input (.txt) and output (.csv) files
            txt_file_path = os.path.join(folder_path, filename)
            csv_file_path = os.path.join(folder_path, filename[:-4] + '.csv')  # Change the extension

            # Convert .txt to .csv
            with open(txt_file_path, 'r') as txt_file, open(csv_file_path, 'w', newline='') as csv_file:
                csv_writer = csv.writer(csv_file)
                for line in txt_file:
                    # Modify this logic based on how your data is structured in the .txt files
                    # For instance, split the line by a specific delimiter
                    data = line.strip().split(',')  # Change the delimiter if necessary
                    csv_writer.writerow(data)

            # Remove the original .txt file
            os.remove(txt_file_path)
            print(f"Converted {filename} to {filename[:-4]}.csv and removed {filename}")

        elif filename.endswith('.csv'):
            # Rename the .csv file and move it to a new folder with channel_id in the name
            try:
                # Read the CSV file
                df = pd.read_csv(os.path.join(folder_path, filename))

                # Get the channel_id from the first row of the 'channel_id' column
                channel_id = df['channel_id'].iloc[0]

                # Create the new file name
                new_filename = f'{os.path.splitext(filename)[0]}_{channel_id}.csv'

                # Construct the new file path in the same folder
                new_file_path = os.path.join(folder_path, new_filename)

                # Rename the file
                os.rename(os.path.join(folder_path, filename), new_file_path)

                print(f'Renamed and saved file: {filename} to {new_filename} in {folder_path}')

            except Exception as e:
                print(f'Error processing file {filename}: {str(e)}')

# Path to the folder containing .txt and .csv files
folder_path = r'C:\Users\anand\OneDrive\Documents\Intern_Project\Files\token_vedantu_master_tamil_8___9'

# Check if the folder path exists
if os.path.exists(folder_path):
    # Call the function to process files
    process_files(folder_path)
else:
    print("Folder path does not exist.")
