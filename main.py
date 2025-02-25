from timestamps_formatting import process_directory as process_directory_for_conversion
from file_processing import unzip_files, process_directory_by_timestamps, read_timestamps, avro_conversion
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
        for file in os.listdir(raw_dir):
            if file.endswith('.avro'):
                avro_file_path = os.path.join(raw_dir, file)
                avro_conversion(avro_file_path, raw_dir)

        process_directory_for_conversion(raw_dir)
        process_directory_by_timestamps(raw_dir, timestamps, output_dir)


if __name__ == '__main__':
    main()
