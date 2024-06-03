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


print()
