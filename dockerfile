FROM apache/airflow:2.3.0
USER root

# Update package lists, install dependencies, and clean up
RUN apt-get update && apt-get install -y python3-pip 

# Open the 8080 (airflow) and 8501 (streamlit) ports
EXPOSE 8080
EXPOSE 8501

# Create a directory inside the container
RUN mkdir -p dags
COPY ./movies_etl/ ./dags

USER airflow

# Install necessary Python packages
RUN pip install apache-airflow \
    pandas requests 'apache-airflow[amazon]' \
    s3fs bs4 streamlit plotly matplotlib wordcloud
    
# Define the command to run when the container starts
CMD ["standalone"]