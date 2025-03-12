import os
import zipfile
import shutil
from datetime import datetime
import pandas as pd
from avro.datafile import DataFileReader
from avro.io import DatumReader
import csv
import re
from openpyxl import load_workbook, Workbook



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


def process_file_by_timestamp(file_path, timestamps, output_dir, participant):
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
        os.makedirs(output_dir, exist_ok=True)
        if not filtered_data.empty:
            db_filename = get_db_filename(file_path, participant, trial_index)
            filtered_data.to_csv(os.path.join(output_dir, db_filename), index=False)
            trial_index += 1


def get_db_filename(file_path, participant, trial_idx):
    file_map = {
        "eda": "eda",
        "Polar": "polar",
        "temperature": "temperature",
        "tracklog": "tracklog",
        "blinks": "blinks",
        "mindMonitor": "mindMonitor",
        "labels": "labels",
        "gaze": "gaze",
        "world": "world_timestamps",
        "imu": "imu",
        "blinks": "blinks",
        "sections": "sections",
        "3d": "3d_eye_states",
        "events": "events",
        "saccades": "saccades",
        "enrichment": "enrichement",
        "fixations": "fixations"
    }

    ''' Grab file name'''
    file_name = os.path.basename(file_path)

    ''' Some file names have delimiters such as "_" or "-" or "." '''
    paritioned_file_name = re.split(r'[-_.]', file_name)

    file_map_key = paritioned_file_name[0]
    file_extension = paritioned_file_name[-1]
    db_value = f'{trial_idx}_{participant}_{file_map[file_map_key]}'


    return db_value + '.' + file_extension


def process_directory_by_timestamps(data_dir, timestamps, output_dir, participant):
    clear_directory(output_dir)
    """Process all CSV and TXT files within a directory according to the provided timestamps."""
    for root, dirs, files in os.walk(data_dir):
        for file_name in files:
            if file_name.endswith(('.csv', '.txt')) and not file_name.startswith('.') and 'metadata' not in file_name:
                file_path = os.path.join(root, file_name)
                process_file_by_timestamp(file_path, timestamps, output_dir, participant)


def read_timestamps(timestamps_file):
    """Reads timestamps from a given file and returns a list of tuples (start, end)."""
    timestamps = []
    with open(timestamps_file, 'r') as file:
        for line in file:
            parts = line.strip().split(',')
            if len(parts) == 2:
                start = datetime.strptime(parts[0].strip(), '%Y-%m-%d %H:%M:%S.%f')
                end = datetime.strptime(parts[1].strip(), '%Y-%m-%d %H:%M:%S.%f')
                timestamps.append((start, end))
    return timestamps


def process_instructor_file(file_path, output_dir, pilot):
    action_to_trial = {
        "general": "master",
        "normal takeoff": 2,
        "steep turn": 3,
        "stall (power on)": 4,
        "normal approach and landing": 5,
        "circuit": 6
    }

    wb = load_workbook(file_path)

    for sheet in wb.sheetnames:
        lower_name = sheet.lower().strip().lower()
        if lower_name in action_to_trial:
            new_name = f"{action_to_trial[lower_name]}_{pilot}_instructor_sheet.csv"
            print(f"Processing instructor file for ${pilot} with ${lower_name}, new file name: ${new_name}")
            new_file_path = os.path.join(output_dir, new_name)

            with open(new_file_path, mode="w", newline="") as csv_file:
                writer = csv.writer(csv_file)

                original_ws = wb[sheet]
                for row in original_ws.iter_rows(values_only=True):
                    writer.writerow(row)


            print(f"Saved: {new_file_path}")

    print(f"All sheets for {file_path} and {pilot} processed successfully")
    

