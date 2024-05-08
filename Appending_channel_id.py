import os
import pandas as pd

folder_path = r'C:\Users\anand\OneDrive\Documents\Intern_Project\yt_reports_csv'
output_folder = r'C:\Users\anand\OneDrive\Documents\Intern_Project\yt_reports_csv\appended_channel_id'

def change_file_names_and_save(folder_path, output_folder):
    # Create the output folder if it doesn't exist
    if not os.path.exists(output_folder):
        os.makedirs(output_folder)

    for filename in os.listdir(folder_path):
        if filename.endswith('.csv'):
            file_path = os.path.join(folder_path, filename)
            try:
                # Read the CSV file
                df = pd.read_csv(file_path)
                
                # Get the channel_id from the first row of the 'channel_id' column
                channel_id = df['channel_id'].iloc[0]
                
                # Create the new file name
                new_filename = f'{os.path.splitext(filename)[0]}_{channel_id}.csv'
                
                # Construct the new file path in the output folder
                new_file_path = os.path.join(output_folder, new_filename)
                
                # Rename the file and move it to the output folder
                os.rename(file_path, new_file_path)
                
                print(f'Renamed and saved file: {filename} to {new_filename} in {output_folder}')
                
            except Exception as e:
                print(f'Error processing file {filename}: {str(e)}')

# Call the function to change file names and save in the new folder
change_file_names_and_save(folder_path, output_folder)
