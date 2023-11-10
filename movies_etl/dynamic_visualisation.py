from visualization_tools import genre_pie_chart, year_bar_chart, year_line_chart, title_wordcloud, watchtime_rating_scatterplot, float_metric_histogram, genre_bar_chart
from visualization_tools import get_year_counts,  generate_rainbow_colors, metrics_list, metric_mean_rectangle
import streamlit as st
from read_write_s3 import read_csv_from_s3
from airflow.models import Variable

AWS_S3_BUCKET = Variable.get("AWS_S3_BUCKET")
AWS_ACCESS_KEY_ID = Variable.get("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = Variable.get("AWS_SECRET_ACCESS_KEY")
AWS_REGION = Variable.get("AWS_REGION")

# Displays the first row of visualizations: Table and Pie Chart
def line1_visualisation(df, global_genres):
    # Create a layout with two columns
    col1, col2 = st.columns([3, 1])
    
    # Column 1: Display a sample of the data in a table
    with col1:
        st.write('### Table')
        st.dataframe(
            df.head(10)
        )        

    # Column 2: Display a pie chart showing genre distribution
    with col2:
        genre_pie_chart(df, global_genres)

# Displays the second row of visualizations: Line Chart, Bar Chart, Rectangles
def line2_visualisation(df, color, global_genres):
    # Create a layout with three columns
    col1, col2, col3 = st.columns([4,1,3])
    metrics = metrics_list(df)

    # Column 1: Display a line or bar chart for the year of release
    with col1:
        year_counts = get_year_counts(df).to_dict()
        if len(year_counts) <= 10:
            year_bar_chart(df, color)
        else:
            year_line_chart(df, color)

    # Column 2: Display key metrics (Mean values in rectangles)
    with col2:
        st.write('#### Key Metrics')
        
        metrics = metrics_list(df)
        for metric in metrics:
            metric_mean_rectangle(df, metric)

    # Column 3: Display histograms for ratings
    with col3:
        genre_bar_chart(df,global_genres)

# Displays the third row of visualizations: Word Cloud, Scatter Plot, and Bar Plot
def line3_visualisation(df, color, global_genres):
    # Create a layout with three columns
    col1, col2, col3 = st.columns(3)

    # Column 1: Display a word cloud for movie titles
    with col1:
        title_wordcloud(df)

    # Column 2: Display a scatter plot for watchtime vs. movie rating
    with col2:
        watchtime_rating_scatterplot(df, color)

    # Column 3: Display a bar plot for metrics
    with col3:
        metrics = metrics_list(df)
        metric = st.selectbox("Select Metric:", metrics, index=0)
        float_metric_histogram(df, metric, color)

# Combines all visualizations into a single page
def global_visualisation(df):
    st.title('Movie Data Visualization')
    
    genres_list = df['Genre'].str.split(', ').explode()
    genre_counts = genres_list.value_counts()
    # This is the list of all genres !!
    genre_options = ['None'] +  genre_counts.index.tolist()  # Add 'None' option

    genre = st.selectbox("Select Genre:", genre_options, index=0)
    if genre == 'None':
        filtered_df = df  # If 'None' is selected, use the original DataFrame
    else:
        # Filter the DataFrame based on the selected genre
        filtered_df = df[df['Genre'].str.contains(genre, na=False)] 

    # Generate colors and associate a color to the selected genre
    colors = generate_rainbow_colors(len(genre_options))
    index = genre_options.index(genre)

    # Run the subplots
    line1_visualisation(filtered_df, genre_options)
    line2_visualisation(filtered_df, colors[index], genre_options)
    line3_visualisation(filtered_df, colors[index], genre_options)


# Main function to read data and perform visualizations
def main():
    movie_df = read_csv_from_s3(AWS_S3_BUCKET, 'global_movies.csv', AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_REGION)
    
    st.set_page_config(layout="wide")
    global_visualisation(movie_df)

# Entry point of the script
if __name__ == '__main__':
    main()