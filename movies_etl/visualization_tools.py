import streamlit as st
import pandas as pd
import plotly.express as px
import os
import math
from wordcloud import WordCloud
import colorsys

@st.cache_data
# Generates a list of rainbow colors for visualization
def generate_rainbow_colors(n, add_default = True):
    if add_default: 
        default_color = '#aaaaaa'
        # Generate evenly spaced hues (0 to 1)
        if n == 1:
            return [default_color]
        else:
            hex_colors = generate_rainbow_colors(n - 1, add_default = False)
            return  [default_color]  + hex_colors
    else:
        hues = [i / n for i in range(n)]
        # Convert HSV to RGB
        rgb_colors = [colorsys.hsv_to_rgb(hue, 0.5, 1) for hue in hues]
        # Convert RGB to hex format
        hex_colors = [f'#{int(r * 255):02x}{int(g * 255):02x}{int(b * 255):02x}' for (r, g, b) in rgb_colors]

        return hex_colors

@st.cache_data
# Generates a list of colors for each genre in the provided sublist
def genre_color_list(global_genres, sublist_genres):
    genres = global_genres
    colors = generate_rainbow_colors(len(genres))
    print(colors)
    list_colors = []
    for genre in sublist_genres.index.tolist():
        index  = genres.index(genre)
        list_colors.append(colors[index])
    return list_colors

@st.cache_data
# Creates a mapping of genres to colors
def genre_color_map(global_genres):
    genres = global_genres
    colors = generate_rainbow_colors(len(genres))
    color_map = {}
    for i in range(11):
        color_map[genres[i]] = colors[i]
    return color_map

@st.cache_data
# Counts the number of movies released each year
def get_year_counts(df):
    # Convert 'Year of release' to datetime for plotting
    df['Year of release'] = pd.to_numeric(df['Year of release'], errors='coerce')

    # Drop rows with NaN values in 'Year of release'
    df = df.dropna(subset=['Year of release'])
    
    # Count the number of movies released each year
    year_counts = df['Year of release'].value_counts().sort_index()
    return year_counts

@st.cache_data
# Calculates the mean of a specified metric in the DataFrame
def mean_metric(df, column):
    # Get the column values from the DataFrame
    df[column] = pd.to_numeric(df[column], errors='coerce')
    return  df[column].mean()
      
@st.cache_data
# Returns a list of valid metrics with non-NaN means
def metrics_list(df):
    metrics = ['Watchtime', 'Movie Rating','Metascore','Votes']
    for metric in metrics:
        metric_mean = mean_metric(df, metric)
        if  math.isnan(metric_mean):
            metrics.remove(metric)
    return metrics     
     
@st.cache_data
# Displays a metric's mean value in a Streamlit metric widget
def metric_mean_rectangle(df, column):
    metric_mean = mean_metric(df, column)
    if not math.isnan(metric_mean):
        st.metric(label= "Mean " + column, value=metric_mean.round(2))

@st.cache_data
# Displays a pie chart showing genre distribution
def genre_pie_chart(df, global_genres):
    # Group by 'Genre' and count the occurrences
    genres_list = df['Genre'].str.split(', ').explode()
    genre_counts = genres_list.value_counts()

    list_colors = genre_color_list(global_genres, genre_counts)
    
    # Create a pie chart
    genre_distribution = px.pie(
        values=genre_counts, 
        names=genre_counts.index, 
        color_discrete_sequence=list_colors,
        title = 'Pie Chart for Genre Distribution'
    )
    genre_distribution.update_layout(width=400, height=400)

    # Display the pie chart using Streamlit
    st.plotly_chart(genre_distribution)

@st.cache_data
# Displays a line chart for the year of release
def year_line_chart(df, color):
    # Count the number of movies released each year
    year_counts = get_year_counts(df)

    fig = px.line(
        year_counts,
        title = 'Line Chart for Year of Release'
    )
    fig.update_traces(textposition="top center", marker_color = color)
    
    fig.update_layout(showlegend=False)
    st.plotly_chart(fig, use_container_width=True)

@st.cache_data
# Displays a bar chart for the year of release
def year_bar_chart(df, color):
    # Count the number of movies released each year
    year_counts = get_year_counts(df)

    fig = px.bar(
        year_counts,
        title = 'Bar Chart for Year of Release',
    )
    fig.update_traces(marker_color = color)
    fig.update_xaxes(title_text='Year of release')
    fig.update_yaxes(title_text='Number of Movies')
    fig.update_layout(showlegend=False)
    st.plotly_chart(fig, use_container_width=True)

@st.cache_data
# Displays a histogram for a floating-point metric
def float_metric_histogram(df, metric, color):
    # Get the metric from the DataFrame
    df[metric] = pd.to_numeric(df[metric], errors='coerce')

    # Drop rows with NaN values in the metric
    df = df.dropna(subset=[metric])
    
    # Get the metric_data from the DataFrame
    metric_data = df[metric]

    # Create a histogram with custom bins for the ranges of watchtime
    fig = px.histogram(
        metric_data, 
        x=metric, 
        title='Histogram for '+ metric +' Distribution', 
    )

    # Update x-axis labels and title
    fig.update_xaxes(title_text= metric + ' Ranges')
    fig.update_yaxes(title_text='Number of Movies')
    fig.update_layout(bargap = 0.1)
    fig.update_traces(marker_color = color)
    st.plotly_chart(fig, use_container_width=True)

# Displays a scatter plot for metrics
def watchtime_rating_scatterplot(df, color):
    col1, col2 = st.columns([1, 1])
    metrics = metrics_list(df)
    
    # Add dropdowns for x and y variables
    with col1:
        x_variable = st.selectbox("Select X Variable:", metrics, index=0)
    with col2:
        y_variable = st.selectbox("Select Y Variable", metrics, index=1)

    dynamic_watchtime_rating_scatterplot(df, x_variable, y_variable, color)

@st.cache_data
# Displays a scatter plot for metrics
def dynamic_watchtime_rating_scatterplot(df, x_variable, y_variable, color):
    # Get the movie ratings and watchtime from the DataFrame
    df_copy = df.copy()
    df_copy[x_variable] = pd.to_numeric(df_copy[x_variable], errors='coerce')
    df_copy[y_variable] = pd.to_numeric(df_copy[y_variable], errors='coerce')

    # Drop rows with NaN values in 'Movie Rating'
    df_copy = df_copy.dropna(subset=[x_variable, y_variable])
    
    # Create the scatter plot based on selected variables
    scatter_plot = px.scatter(
        df_copy, 
        x=x_variable, 
        y=y_variable,
        title = 'Scatter Plot for ' + x_variable + ' vs. ' + y_variable
    )
    
    scatter_plot.update_traces(marker_color = color)
    st.plotly_chart(scatter_plot, use_container_width=True)

@st.cache_data
# Generates and displays a word cloud for movie titles
def title_wordcloud(df):
    st.subheader('Word Cloud for Movie Titles')
    
    # Get the movie titles from the DataFrame
    movie_titles = df['Name of movie']

    # Create a string with all movie titles
    all_titles = ' '.join(movie_titles)

    # Generate the word cloud
    wordcloud = WordCloud(width=800, height=400, background_color='white').generate(all_titles)

    # Display the word cloud using Streamlit
    st.image(
        wordcloud.to_image(),  
        caption='Word Cloud for Movie Titles', 
        use_column_width=True
    )

@st.cache_data
# Generates and displays a word cloud for movie descriptions
def description_wordcloud(df):
    st.subheader('Word Cloud for Movie Descriptions')
    
    # Get the movie titles from the DataFrame
    movie_titles = df['Description']

    # Create a string with all movie titles
    all_titles = ' '.join(movie_titles)

    # Generate the word cloud
    wordcloud = WordCloud(width=800, height=400, background_color='white').generate(all_titles)

    # Display the word cloud using Streamlit
    st.image(wordcloud.to_image(), caption='Word Cloud for Movie Descriptions', use_column_width=True)

# Displays a bar chart for a specified metric's mean value by genre
def genre_bar_chart(df, global_genres):
    metrics = metrics_list(df)
    metric = st.selectbox("Select Metric:", metrics, index=1)
    genre_mean_metric_bar_chart(df, metric, global_genres)

@st.cache_data
# Displays a bar chart for a specified metric's mean value by genre
def genre_mean_metric_bar_chart(df, metric, global_genres):
     # Create a copy of the DataFrame to avoid modifying the original
    df_copy = df.copy()

    # Split genres and create a new row for each genre
    df_copy['Genre'] = df_copy['Genre'].str.split(', ')
    df_copy = df_copy.explode('Genre')

    # Get the metric from the DataFrame
    df_copy[metric] = pd.to_numeric(df_copy[metric], errors='coerce')
    # Drop rows with NaN values in the metric
    df_copy = df_copy.dropna(subset=[metric])

    # Group by 'Genre' and calculate the mean metric value for each genre
    genre_mean_metric = df_copy.groupby('Genre')[metric].mean().reset_index()
    
    # Create a bar chart with genre bins for the ranges of watchtime
    fig = px.bar(
        genre_mean_metric,
        x='Genre',
        y=metric,
        title= 'Bar Chart for ' + metric + ' Distibution by Genre',
        labels={'Movie ' + metric: 'Mean ' + metric},
        text= metric,  # Display the mean rating on the bars
        color = 'Genre',
        color_discrete_map=genre_color_map(global_genres)
    )
    
    # Customize the layout (optional)
    fig.update_layout(
        xaxis_title='Genre',
        yaxis_title='Mean Rating',
        showlegend=False,
        width=800,
        height=500,
    )
    # fig.update_xaxes(title_text= metric + ' Ranges')
    # fig.update_yaxes(title_text='Number of Movies')
    # fig.update_layout(bargap = 0.1)
    # fig.update_traces(marker_color = color)
    st.plotly_chart(fig, use_container_width=True)
