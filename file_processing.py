import os
import zipfile
import shutil
from datetime import datetime
import pandas as pd
from avro.datafile import DataFileReader
from avro.io import DatumReader
import csv


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


def avro_conversion(avro_file, output):
    # Open the Avro file for reading
    try:
        with DataFileReader(open(avro_file, "rb"), DatumReader()) as reader:
            base_filename = os.path.splitext(os.path.basename(avro_file))[0]
            # Loop over each record in the Avro file
            for data in reader:
                # Process and save EDA data
                eda = data["rawData"]["eda"]
                eda_timestamps = [round(eda["timestampStart"] + i * (1e6 / eda["samplingFrequency"]))
                                  for i in range(len(eda["values"]))]
                eda_csv_file = os.path.join(output, f'eda_{base_filename}.csv')
                with open(eda_csv_file, 'w', newline='') as f:
                    writer = csv.writer(f)
                    writer.writerow(["unix_timestamp", "eda"])
                    writer.writerows(zip(eda_timestamps, eda["values"]))

                # Process and save Temperature data
                tmp = data["rawData"]["temperature"]
                tmp_timestamps = [round(tmp["timestampStart"] + i * (1e6 / tmp["samplingFrequency"]))
                                  for i in range(len(tmp["values"]))]
                temperature_csv_file = os.path.join(output, f'temperature_{base_filename}.csv')
                with open(temperature_csv_file, 'w', newline='') as f:
                    writer = csv.writer(f)
                    writer.writerow(["unix_timestamp", "temperature"])
                    writer.writerows(zip(tmp_timestamps, tmp["values"]))

        # Delete the Avro file after successful processing
        os.remove(avro_file)
        print(f"Deleted Avro file: {avro_file}")

    except Exception as e:
        print(f"Failed to process and delete {avro_file}: {e}")


def flatten_directory_structure(directory):
    """Move all files from subfolders to the main directory and remove empty subfolders."""
    for root, dirs, files in os.walk(directory, topdown=False):  # Start from the deepest level
        for name in files:
            file_path = os.path.join(root, name)
            if root != directory:
                shutil.move(file_path, os.path.join(directory, name))  # Move files to the main directory
        for name in dirs:
            dir_path = os.path.join(root, name)
            if os.listdir(dir_path) == []:  # Check if the directory is empty now
                os.rmdir(dir_path)  # Remove the empty directory


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
        'timestamp_unix', 'timestamp [ns]', 'TimeStamp', 'unix_timestamp'
    ]

    # Identify the correct timestamp column from the list of possibilities
    timestamp_column = next((col for col in possible_timestamp_columns if col in df.columns), None)

    if timestamp_column is None:
        print(f"No valid timestamp column found in {file_path}. Skipping file.")
        return  # Skip this file if no valid timestamp column is found

    # Process each timestamp range
    trial_index = 1
    for (start, end) in timestamps:
        if os.path.basename(file_path).startswith('mindMonitor'):
            timestamp_column_converted = timestamp_column
        else:
            timestamp_column_converted = f'{timestamp_column}_converted'
        mask = (pd.to_datetime(df[timestamp_column_converted]) >= start) & (pd.to_datetime(df[timestamp_column_converted]) <= end)
        filtered_data = df.loc[mask]
        if not filtered_data.empty:
            trial_dir = os.path.join(output_dir, f'trial{trial_index}')
            os.makedirs(trial_dir, exist_ok=True)
            filtered_data.to_csv(os.path.join(trial_dir, os.path.basename(file_path)), index=False)
            print(f"Processed and saved data for trial {trial_index} in {file_path}.")
            trial_index += 1


def process_directory_by_timestamps(data_dir, timestamps, output_dir):
    clear_directory(output_dir)
    """Process all CSV and TXT files within a directory according to the provided timestamps."""
    for root, dirs, files in os.walk(data_dir):
        for file_name in files:
            if file_name.endswith(('.csv', '.txt')) and not file_name.startswith('.') and 'metadata' not in file_name:
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
