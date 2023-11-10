# Movie ETL Project

## Overview

This project is designed to perform an Extract, Transform, Load (ETL) process on movie data. The workflow is orchestrated using Apache Airflow, and it consists of several tasks that work together to achieve the desired outcome.

## Files

- `dockerfile`: Docker configuration for the project.
- `movies.csv`: Input CSV file containing movie data.
- `poetry.lock` and `pyproject.toml`: Poetry configuration files.
- `movies_etl/`: Directory containing Python scripts for the ETL process.
- `tests/`: Directory containing unit tests for the project.

## Directory Structure

- `movies_etl/`: Contains the following Python scripts:
  - `dynamic_visualization.py`: Streamlit app for dynamic visualization.
  - `global_visualization.py`: Streamlit app for global visualization.
  - `plotly_visualize.py`: Streamlit app for Plotly visualization.
  - `read_write_s3.py`: Functions for reading and writing data to/from Amazon S3.
  - `scraping.py`: Functions for web scraping movie data.
  - `movies_dag.py`: Airflow DAG for orchestrating the ETL process.
  - `movies_airflow_functions.py`: Functions used in the Airflow DAG.

- `tests/`: Contains unit tests for the project.

## Data Flow

1. **Task 1: Generate URLs**
   - Reads a range of dates and generates a list of URLs.
   - Writes the URLs to a file (`urls.txt`).

2. **Task 2: Generate Pages**
   - Reads the `urls.txt` file.
   - For each URL, generates a list of pages and writes them to separate files (`pages_0.txt`, `pages_1.txt`, etc.).

3. **Task 3 (Dynamic): Scrape and Transform**
   - Reads a page file and scrapes data.
   - Combines dataframes, removes duplicates, and saves the result to an S3 bucket (`movies_i.csv`).

4. **Task 4: Combine and Save to S3**
   - Reads the number of tasks (`num_tasks`) from XCom.
   - Combines all the `movies_i.csv` files from the S3 bucket, removes duplicates, and saves the combined data to `global_movies.csv` in the S3 bucket.

5. **Task 5: Visualize Data**
   - Launches a Streamlit app to visualize the data.

6. **Task 6: Delete Files and Clean S3**
   - Deletes individual `movies_i.csv` files from the S3 bucket.

7. **Task 7: Delete Files and Clean Local Folder**
   - Deletes `urls.txt` and `pages_i.txt` files from the local folder.

## Running the Project

To run the project, follow these steps:

1. Build the Docker image using the provided `dockerfile` which copies the project files into the appropriate directory within the container.
   ```bash
   docker build -t my_airflow_image .
   ```

2. Start the Docker container and login to Airflow.
   ```bash
   docker run -d -p 8080:8080 -p 8501:8501 my_airflow_image
   ```

3. Configure your Amazon S3 credentials in Airflow Variables.

4. Start the Airflow DAG.

5. View the Streamlit visualizations.

## Using Poetry and Virtual Environment

This project utilizes Poetry to manage dependencies and create a virtual environment. The `pyproject.toml` and `poetry.lock` files contain the necessary information for creating a consistent environment.

To set up the environment, run the following command:

```bash
poetry install
```

This will create a virtual environment with all the required packages.

## Dashboard

The project includes Streamlit apps for data visualization. These apps provide an interactive and dynamic way to explore the movie data. The dashboard consists of various visualizations and interactive elements to enhance the user experience.

## AWS Credentials

AWS credentials should be specified as Airflow Variables for secure access to the S3 bucket. This ensures that sensitive information is stored securely within Airflow.

## Notes

- Data is stored in the S3 bucket to handle large datasets.
- XCom is used to communicate between tasks, especially for dynamic task generation.
- Unit tests are available in the `tests/` directory for testing individual components.

## Dependencies

- Python
- Apache Airflow
- Streamlit
- BeautifulSoup
- Pandas
- Plotly
- S3fs
