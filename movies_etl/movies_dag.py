from datetime import datetime
from datetime import timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from movies_airflow_functions import count_number_urls, generate_urls, generate_pages, scrape_transform, combine_and_save_to_s3, delete_files_and_clean_s3, delete_files_and_clean_local_folder
from airflow.utils.task_group import TaskGroup

# Define default_args for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'email': ['hibatouderti@gmail.com'],
    'start_date': datetime(2023, 11, 4),
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

# Define the DAG
dag = DAG(
    'movie_dags',
    default_args=default_args,
    description='A DAG to manage the web scraping workflow',
)

# Define tasks for each step in the DAG
with dag:
    # Task 1: Scrape URLs and write to a file
    task_1 = PythonOperator(
        task_id='generate_urls',
        python_callable=generate_urls,
        dag=dag,
    )

    # Task 2: Generate Pages using URLs
    task_2 = PythonOperator(
        task_id='generate_pages',
        python_callable=generate_pages,
        dag=dag,
    )

    # Task 3: Scrape and Transform (Dynamic)
    task_3_group = TaskGroup(
        group_id='task_3_group',
    )
    with task_3_group:
        # Iterate through the generated pages and perform scraping
        num_tasks = count_number_urls()
        for i in range(num_tasks):
            task_3 = PythonOperator(
                task_id=f'scrape_transform_{i}',
                python_callable=scrape_transform,
                op_args=[i],
                provide_context=True,
                dag=dag,
            )
            
    # Task 4: Combine DataFrames and Save to S3
    task_4 = PythonOperator(
        task_id='combine_and_save_to_s3',
        python_callable=combine_and_save_to_s3,
        dag=dag,
    )
    
    # Task 5: Delete unnecessary files from S3
    task_5 = PythonOperator(
        task_id='delete_files_and_clean_s3',
        python_callable=delete_files_and_clean_s3,
        dag=dag,
    )

    # Task 6: Delete unnecessary files from local folder
    task_6 = PythonOperator(
        task_id='delete_files_and_clean_local_folder',
        python_callable=delete_files_and_clean_local_folder,
        dag=dag,
    )

    # Task 7: Read from S3 and visualize
    task_7 = BashOperator(
        task_id='visualize_data',
        bash_command='streamlit run ../../opt/airflow/dags/dynamic_visualisation.py',  # Replace with your visualization script
        dag=dag,
    )

    # Set task dependencies
    task_1 >> task_2 >> task_3_group >> task_4 >> [task_5, task_6]
    task_5 >> task_7
    task_6 >> task_7