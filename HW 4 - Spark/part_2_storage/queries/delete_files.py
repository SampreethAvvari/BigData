import subprocess

def delete_hdfs_files(file_list):
    """
    Deletes multiple files from HDFS.

    Args:
    file_list (list of str): List of HDFS file paths to delete.

    Returns:
    None
    """
    # Base command to delete files in HDFS
    base_command = ["hadoop", "fs","-rm", "-r"]

    # Add '-f' to force delete without confirmation and '-skipTrash' to bypass the trash, if needed
    for file_path in file_list:
        command = base_command + [file_path]
        try:
            # Execute the command
            result = subprocess.run(command, check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
            print(f"Deleted {file_path}: {result.stdout}")
        except subprocess.CalledProcessError as e:
            # Handle errors that occur during subprocess execution
            print(f"Error deleting {file_path}: {e.stderr}")

# Example usage
file_paths_to_delete = ["hdfs:/user/br2543_nyu_edu/homework-4-69/part_2_storage/people_small.parquet",
        "hdfs:/user/br2543_nyu_edu/homework-4-69/part_2_storage/people_moderate.parquet",
        "hdfs:/user/br2543_nyu_edu/homework-4-69/part_2_storage/people_big.parquet"]
opt_paths_to_delete  = ["hdfs:/user/br2543_nyu_edu/homework-4-69/part_2_storage/people_small_big_spender.parquet",
        "hdfs:/user/br2543_nyu_edu/homework-4-69/part_2_storage/people_moderate_big_spender.parquet",
        "hdfs:/user/br2543_nyu_edu/homework-4-69/part_2_storage/people_big_big_spender.parquet"]
delete_hdfs_files(file_paths_to_delete)
delete_hdfs_files(opt_paths_to_delete)