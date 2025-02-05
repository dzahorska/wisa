from timestamps_formatting import process_directory as process_directory_for_conversion
from file_processing import unzip_files, process_directory_by_timestamps, read_timestamps, avro_conversion
from data_processing import Blinks, insert_data
import os


# def main():
#     numbers_pilots = [47, 86]
#     for n in numbers_pilots:
#         base_dir = f'/Users/kanishksk/Documents/WISA/{n}'
#         raw_dir = os.path.join(base_dir, 'raw')
#         output_dir = os.path.join(base_dir, 'output')
#         timestamps_file = os.path.join(base_dir, 'timestamps.txt')
#         timestamps = read_timestamps(timestamps_file)

#         unzip_files(raw_dir)
#         for file in os.listdir(raw_dir):
#             if file.endswith('.avro'):
#                 avro_file_path = os.path.join(raw_dir, file)
#                 avro_conversion(avro_file_path, raw_dir)

#         process_directory_for_conversion(raw_dir)
#         process_directory_by_timestamps(raw_dir, timestamps, output_dir)

TABLE_MAPPING = {
    "blinks.csv": Blinks
}

def main():
    numbers_pilots = [47, 86]
    for n in numbers_pilots:
        base_dir = f'/Users/kanishksk/Documents/WISA/{n}'
        raw_dir = os.path.join(base_dir, 'raw')
        output_dir = os.path.join(base_dir, 'output')
        timestamps_file = os.path.join(base_dir, 'timestamps.txt')
        #timestamps = read_timestamps(timestamps_file)

        trial_folders = [d for d in os.listdir(output_dir)]

        result = {}
        for trial_number in os.listdir(output_dir):
            dir_path = os.path.join(output_dir, trial_number)
            if os.path.isdir(dir_path):  # Check if it's a directory
                for f in os.listdir(dir_path):
                    if "blinks.csv" in f:
                        print("INSIDE IF")
                        full_path = os.path.join(dir_path, f)
                        print("FULLL PATH")
                        insert_data(full_path, n, trial_number, Blinks)




if __name__ == '__main__':
    main()
