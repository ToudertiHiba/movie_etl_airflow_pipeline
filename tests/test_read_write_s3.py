import pandas as pd
from movies_etl.read_write_s3 import read_csv_from_s3, save_df_to_s3, delete_file_from_s3, check_file_exists_in_s3
from airflow.models import Variable

AWS_S3_BUCKET = Variable.get("AWS_S3_BUCKET")
AWS_ACCESS_KEY_ID = Variable.get("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = Variable.get("AWS_SECRET_ACCESS_KEY")
AWS_REGION = Variable.get("AWS_REGION")

def test_save_df_to_s3():
    # Arrange
    filename = "test_save.csv"
    df = pd.DataFrame({
        'Name of movie': ['Miss Jerry'],
        'Year of release': [1894],
        'Watchtime': [45],
        'Genre': ['Romance'],
        'Movie Rating': [5.3],
        'Metascore': ['N/A'],
        'Votes': [207],
        'Gross collection': ['N/A']
    })

    # Act
    save_df_to_s3(df, AWS_S3_BUCKET, filename, AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_REGION)
    result = read_csv_from_s3(AWS_S3_BUCKET, filename, AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_REGION)
    
    # Assert
    assert  result.equals(df)
    
    # Teardown
    delete_file_from_s3(AWS_S3_BUCKET, filename, AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_REGION)

def test_delete_df_from_s3():    
    # Arrange
    filename = "test_save.csv"

    # Teardown
    delete_file_from_s3(AWS_S3_BUCKET, filename, AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_REGION)

    # Assert
    assert not check_file_exists_in_s3(AWS_S3_BUCKET, filename, AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_REGION)