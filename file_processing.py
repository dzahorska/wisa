import os
import zipfile
import shutil
from datetime import datetime
import pandas as pd


def unzip_files(source_dir):
    """Unzip all .zip files in the specified directory and remove the original zip files."""
    for item in os.listdir(source_dir):
        if item.endswith('.zip'):
            file_path = os.path.join(source_dir, item)
            extract_to_path = os.path.join(source_dir, os.path.splitext(item)[0])
            os.makedirs(extract_to_path, exist_ok=True)
            with zipfile.ZipFile(file_path, 'r') as zip_ref:
                zip_ref.extractall(extract_to_path)
            os.remove(file_path)
            flatten_directory_structure(extract_to_path)


def flatten_directory_structure(directory):
    """Move all files from subfolders to the main directory and remove empty subfolders."""
    for root, dirs, files in os.walk(directory, topdown=False):
        for name in files:
            file_path = os.path.join(root, name)
            if root != directory:
                shutil.move(file_path, os.path.join(directory, name))
        for name in dirs:
            dir_path = os.path.join(root, name)
            if os.listdir(dir_path) == []:
                os.rmdir(dir_path)


def clear_directory(directory):
    """ Removes all files and folders in the specified directory """
    if os.path.exists(directory):
        for item in os.listdir(directory):
            item_path = os.path.join(directory, item)
            if os.path.isdir(item_path):
                shutil.rmtree(item_path)
            else:
                os.remove(item_path)


def process_file_by_timestamp(file_path, timestamps, output_dir):
    """Process and save data from a file according to specified timestamp ranges into separate trial folders."""
    df = pd.read_csv(file_path)

    # Define the list of possible timestamp columns
    possible_timestamp_columns = [
        'start timestamp [ns]', 'timestamp', 'Timestamp', 'Phone timestamp',
        'timestamp_unix', 'timestamp [ns]', 'TimeStamp',
    ]

    # Identify the correct timestamp column from the list of possibilities
    timestamp_column = next((col for col in possible_timestamp_columns if col in df.columns), None)

    if timestamp_column is None:
        print(f"No valid timestamp column found in {file_path}. Skipping file.")
        return  # Skip this file if no valid timestamp column is found

    # Process each timestamp range
    last_end_time = None
    # Process each timestamp range
    trial_index = 1
    for (start, end) in timestamps:
        mask = (pd.to_datetime(df[timestamp_column]) >= start) & (pd.to_datetime(df[timestamp_column]) <= end)
        filtered_data = df.loc[mask]
        if not filtered_data.empty:
            trial_dir = os.path.join(output_dir, f'trial{trial_index}')
            os.makedirs(trial_dir, exist_ok=True)
            filtered_data.to_csv(os.path.join(trial_dir, os.path.basename(file_path)), index=False)
            print(f"Processed and saved data for trial {trial_index} in {file_path}.")
            trial_index += 1
        last_end_time = end

    # Handle remaining data beyond last timestamp
    if last_end_time:
        remaining_mask = pd.to_datetime(df[timestamp_column]) > last_end_time
        remaining_data = df.loc[remaining_mask]
        if not remaining_data.empty:
            remaining_trial_dir = os.path.join(output_dir, f'trial{trial_index}')
            os.makedirs(remaining_trial_dir, exist_ok=True)
            remaining_data.to_csv(os.path.join(remaining_trial_dir, os.path.basename(file_path)), index=False)
            print(f"Processed and saved remaining data in {remaining_trial_dir}.")


def process_directory_by_timestamps(data_dir, timestamps, output_dir):
    clear_directory(output_dir)
    """Process all CSV and TXT files within a directory according to the provided timestamps."""
    for root, dirs, files in os.walk(data_dir):
        for file_name in files:
            if file_name.endswith(('.csv', '.txt')) and not file_name.startswith('.'):
                file_path = os.path.join(root, file_name)
                process_file_by_timestamp(file_path, timestamps, output_dir)


def read_timestamps(timestamps_file):
    """Reads timestamps from a given file and returns a list of tuples (start, end)."""
    timestamps = []
    with open(timestamps_file, 'r') as file:
        for line in file:
            parts = line.strip().split(',')
            if len(parts) == 2:
                start = datetime.strptime(parts[0].strip(), '%Y-%m-%d %H:%M:%S')
                end = datetime.strptime(parts[1].strip(), '%Y-%m-%d %H:%M:%S')
                timestamps.append((start, end))
    return timestamps

