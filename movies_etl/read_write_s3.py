import s3fs
import pandas as pd

def save_df_to_s3(df, bucket_name, file_name, aws_access_key, aws_secret_key, aws_region):
    """
    Saves a DataFrame to an S3 bucket as a CSV file.

    Args:
        df (DataFrame): The DataFrame to be saved.
        bucket_name (str): The name of the S3 bucket.
        file_name (str): The name of the file in the S3 bucket.
        aws_access_key (str): The AWS access key.
        aws_secret_key (str): The AWS secret key.
        aws_region (str): The AWS region.

    Returns:
        str: The S3 path of the saved file.
    """
    # Convert DataFrame to CSV content
    csv_content = df.to_csv(index=False)

    # Set up S3 file system
    s3 = s3fs.S3FileSystem(
        key=aws_access_key,
        secret=aws_secret_key,
        client_kwargs={'region_name': aws_region}
    )

    # Define the S3 path for the file
    s3_path = f"{bucket_name}/{file_name}"

    # Upload the CSV data to S3
    with s3.open(s3_path, "w") as s3_file:
        s3_file.write(csv_content)

    return f"S3://{bucket_name}/{file_name}"

def read_csv_from_s3(bucket_name, file_name, aws_access_key, aws_secret_key, aws_region):
    """
    Reads a CSV file from an S3 bucket and returns it as a DataFrame.

    Args:
        bucket_name (str): The name of the S3 bucket.
        file_name (str): The name of the file in the S3 bucket.
        aws_access_key (str): The AWS access key.
        aws_secret_key (str): The AWS secret key.
        aws_region (str): The AWS region.

    Returns:
        DataFrame: The DataFrame read from the CSV file.
    """
    # Set up S3 file system
    s3 = s3fs.S3FileSystem(
        key=aws_access_key,
        secret=aws_secret_key,
        client_kwargs={'region_name': aws_region}
    )

    # Define the S3 path for the file
    s3_path = f"{bucket_name}/{file_name}"

    # Read the CSV file from S3
    with s3.open(s3_path, "r") as s3_file:
        df = pd.read_csv(s3_file)

    return df


def delete_file_from_s3(bucket_name, file_name, aws_access_key, aws_secret_key, aws_region):
    """
    Deletes a file from an S3 bucket.

    Args:
        bucket_name (str): The name of the S3 bucket.
        file_name (str): The name of the file to be deleted.
        aws_access_key (str): The AWS access key.
        aws_secret_key (str): The AWS secret key.
        aws_region (str): The AWS region.
    """
    # Initialize a session using your AWS credentials
    s3 = s3fs.S3FileSystem(
        key=aws_access_key,
        secret=aws_secret_key,
        client_kwargs={'region_name': aws_region}
    )

    # Delete the file from the bucket
    s3.rm(f"{bucket_name}/{file_name}")


def check_file_exists_in_s3(bucket_name, file_name, aws_access_key, aws_secret_key, aws_region):
    """
    Checks if a file exists in an S3 bucket.

    Args:
        bucket_name (str): The name of the S3 bucket.
        file_name (str): The name of the file to be checked.
        aws_access_key (str): The AWS access key.
        aws_secret_key (str): The AWS secret key.
        aws_region (str): The AWS region.

    Returns:
        bool: True if the file exists, False otherwise.
    """
    # Initialize a session using your AWS credentials
    s3 = s3fs.S3FileSystem(
        key=aws_access_key,
        secret=aws_secret_key,
        client_kwargs={'region_name': aws_region}
    )

    # Check if the file exists in the bucket
    return s3.exists(f"{bucket_name}/{file_name}")
