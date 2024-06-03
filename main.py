from timestamps_formatting import process_directory as process_directory_for_conversion
from file_processing import unzip_files, process_directory_by_timestamps, read_timestamps
import os


def main():
    numbers_pilots = [9, 50, 77, 78, 99, 101]
    for n in numbers_pilots:
        base_dir = f'/Users/dashazagorskaya/Canada/waterloo_project/{n}'
        raw_dir = os.path.join(base_dir, 'raw')
        output_dir = os.path.join(base_dir, 'output')
        timestamps_file = os.path.join(base_dir, 'timestamps.txt')
        timestamps = read_timestamps(timestamps_file)

        unzip_files(raw_dir)
        process_directory_for_conversion(raw_dir)
        process_directory_by_timestamps(raw_dir, timestamps, output_dir)


if __name__ == '__main__':
    main()
