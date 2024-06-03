import os
import pandas as pd
import re
from datetime import datetime


def is_timestamp_in_correct_format(timestamp):
    try:
        if isinstance(timestamp, float) or isinstance(timestamp, int):
            # Convert numeric timestamps to datetime to check format
            datetime.fromtimestamp(timestamp)  # This line checks if the timestamp is in seconds since epoch
            return False  # Indicates it's a numeric timestamp not in human-readable format
        elif re.match(r'\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}', str(timestamp)):
            return True  # Matches human-readable format
        else:
            return False
    except:
        return False


def read_and_convert(file_path, time_unit, delimiter=',', expected_headers=None, timezone='America/Toronto'):
    """ Read files and convert timestamps based on expected headers and timestamp units. """
    directory, filename = os.path.split(file_path)
    _, file_extension = os.path.splitext(filename)
    directory_name = os.path.basename(directory)
    try:
        if filename.startswith('mindMonitor') and directory_name.startswith('mindMonitor'):
            # Simply open and resave the file
            df = pd.read_csv(file_path, delimiter=delimiter)
            df.to_csv(file_path, index=False)
            print(f"File {file_path} opened and resaved without modifications.")
            return
        else:
            df_headers = pd.read_csv(file_path, delimiter=delimiter, nrows=0)
            headers = df_headers.columns.tolist()

            if filename.startswith('tracklog') and expected_headers and set(headers) != set(expected_headers):
                header_row = 2
            else:
                header_row = 0

            df = pd.read_csv(file_path, delimiter=delimiter, header=header_row)

            possible_timestamp_columns = ['start timestamp [ns]', 'timestamp', 'Timestamp', 'Phone timestamp',
                                          'timestamp_unix', 'timestamp [ns]', 'TimeStamp']
            timestamp_column = next((col for col in possible_timestamp_columns if col in df.columns), None)

            if timestamp_column:
                first_entry = df[timestamp_column].dropna().iloc[0]

                if is_timestamp_in_correct_format(first_entry):
                    print(f"Skipping {file_path}, timestamps are already in the correct format.")
                    return

                # Convert all timestamps in the column
                df[timestamp_column] = pd.to_datetime(df[timestamp_column], unit=time_unit, utc=True)
                if 'txt' in file_path:
                    df[timestamp_column] = df[timestamp_column].dt.strftime('%Y-%m-%d %H:%M:%S')
                else:
                    df[timestamp_column] = df[timestamp_column].dt.tz_convert(timezone).dt.tz_localize(None)

                df.to_csv(file_path, index=False)
                print(f"Processed and saved {file_path} with timezone conversion and formatting.")
            else:
                print(f"No timestamp column found in {file_path}")
    except Exception as e:
        print(f"Failed to process {file_path}: {e}")


def process_directory(base_dir):
    """ Process all files in a directory recursively. """
    for root, dirs, files in os.walk(base_dir):
        for file in files:
            if file.endswith(('.csv', '.txt')) and not file.startswith('.') and not file.startswith('enrichment_info') \
                    and not file.startswith('sections'):
                file_path = os.path.join(root, file)
                unit = 's' if 'tracklog' in file or file.startswith('tracklog') else 'ns'
                delimiter = ';' if file.endswith('.txt') else ','  # Adjust delimiter based on your file's format
                expected_headers = [
                    'Timestamp', 'Latitude', 'Longitude', 'Altitude', 'Course', 'Speed',
                    'Bank', 'Pitch', 'Horizontal Error', 'Vertical Error', 'g Load'
                ] if 'tracklog' in file or file.startswith('tracklog') else None
                read_and_convert(file_path, unit, delimiter, expected_headers)
