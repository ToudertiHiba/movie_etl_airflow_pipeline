from scraping import get_urls, get_pages, get_html_data, get_data
from read_write_s3 import save_df_to_s3, read_csv_from_s3, delete_file_from_s3
from datetime import datetime
import pandas as pd
import concurrent.futures
import os
from airflow.models import Variable


AWS_S3_BUCKET = Variable.get("AWS_S3_BUCKET")
AWS_ACCESS_KEY_ID = Variable.get("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = Variable.get("AWS_SECRET_ACCESS_KEY")
AWS_REGION = Variable.get("AWS_REGION")

def count_number_urls():
    """
    Counts the number of URLs in the 'urls.txt' file.
    
    Returns:
        int: Number of URLs in the file.
    """
    try:
        with open('./urls.txt', 'r') as file:
            return len(file.readlines())
    except:
        return 1

def process_url(i, url):
    """
    Processes a URL by scraping its pages and writing them to a file.
    
    Args:
        i (int): Index of the URL.
        url (str): URL to be processed.
    
    Returns:
        int: Index of the processed URL.
    """
    url = url.strip()  
    pages = get_pages(url)
    
    with open(f'./pages_{i}.txt', 'w') as page_file:
        page_file.writelines('\n'.join(pages))
    
    return i

def delete_file(file_path):
    """
    Deletes a file from the filesystem.
    
    Args:
        file_path (str): Path of the file to be deleted.
    """
    try:
        os.remove(file_path)
        print(f"Deleted: {file_path}")
    except Exception as e:
        print(f"Error deleting {file_path}: {e}")

def generate_urls():    
    """
    Generates URLs within a specific date range and writes them to 'urls.txt' file.
    """
    urls = get_urls(start_date = datetime(1916, 1, 1), end_date  = datetime(1917, 1, 1))
    with open('./urls.txt', 'w') as f:
        f.writelines('\n'.join(urls))

def generate_pages(**kwargs):    
    """
    Generates pages for each URL in 'urls.txt' and writes them to separate files.
    
    Args:
        **kwargs: Keyword arguments (used for Airflow context).
    
    Returns:
        int: Number of tasks (equal to the number of URLs processed).
    """
    with open('./urls.txt', 'r') as file:
        urls = file.readlines()

    with concurrent.futures.ThreadPoolExecutor() as executor:
        results = [executor.submit(process_url, i, url) for i, url in enumerate(urls)]

    num_tasks = len(urls)
    ti = kwargs['ti']
    ti.xcom_push(key='num_tasks', value= num_tasks)
    return num_tasks

def scrape_transform(index):
    """
    Scrapes and transforms data for a given index and saves it to an S3 bucket.
    
    Args:
        index (int): Index for processing data.
    """
    dfs = []
    with open(f'./pages_{index}.txt', 'r') as file:
        pages = file.readlines()
        for page_url in pages:
            # Pandas DataFrame obtained from scraping and transformation
            movie_data = get_html_data(page_url)
            movieDF = get_data(movie_data)   
            dfs.append(movieDF) 
    
    # Combine all dfs into one df without duplicates
    combined_df = pd.concat(dfs, ignore_index=True).drop_duplicates()

    # Save df to S3
    save_df_to_s3(combined_df, AWS_S3_BUCKET, f"movies_{index}.csv", AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_REGION)

def combine_and_save_to_s3(**kwargs):
    """
    Combines DataFrames from S3 and saves the result back to S3.
    
    Args:
        **kwargs: Keyword arguments (used for Airflow context).
    """
    # Retrieve num_tasks from XCom
    ti = kwargs['ti']
    num_files = ti.xcom_pull(key='num_tasks', task_ids='generate_pages')
    
    dfs = []
    for i in range(num_files):
        movieDF = read_csv_from_s3(AWS_S3_BUCKET, f"movies_{i}.csv", AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_REGION)
        dfs.append(movieDF)  
    # Combine all dfs into one df without duplicates
    combined_df = pd.concat(dfs, ignore_index=True).drop_duplicates()

    # Save df to S3
    save_df_to_s3(combined_df, AWS_S3_BUCKET, "global_movies.csv", AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_REGION)

def delete_files_and_clean_s3(**kwargs):
    """
    Deletes files from S3.
    
    Args:
        **kwargs: Keyword arguments (used for Airflow context).
    """
    # Retrieve num_tasks from XCom
    ti = kwargs['ti']
    num_files = ti.xcom_pull(key='num_tasks', task_ids='generate_pages')
    for i in range(num_files):
        delete_file_from_s3(AWS_S3_BUCKET, f"movies_{i}.csv", AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_REGION)

def delete_files_and_clean_local_folder(**kwargs):
    """
    Deletes files from the local folder.
    
    Args:
        **kwargs: Keyword arguments (used for Airflow context).
    """
    # Retrieve num_tasks from XCom
    ti = kwargs['ti']
    num_files = ti.xcom_pull(key='num_tasks', task_ids='generate_pages')
    
    # Get the current directory
    current_directory = os.getcwd()
    files_to_delete = []

    # Delete urls.txt
    files_to_delete.append(os.path.join(current_directory, 'urls.txt'))

    # Delete pages_i.txt files
    for i in range(num_files):
        files_to_delete.append(os.path.join(current_directory, f'pages_{i}.txt'))
    
    # Parallelize file deletion
    with concurrent.futures.ThreadPoolExecutor() as executor:
        executor.map(delete_file, files_to_delete)
