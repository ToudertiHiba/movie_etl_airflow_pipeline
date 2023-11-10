from bs4 import BeautifulSoup
import requests
import re
import pandas as pd
from datetime import datetime, timedelta

# Function to get a list of dates from start_date to end_date
def get_dates(start_date, end_date):
    """
    Generates a list of dates from start_date to end_date with a step of 30 days.

    Args:
        start_date (datetime): The start date.
        end_date (datetime): The end date.

    Returns:
        list: A list of dates in the format 'YYYY-MM-DD'.
    """
    current_date = start_date
    dates_list = []
    
    date_format = "%Y-%m-%d"

    while current_date < end_date:
        dates_list.append(current_date.strftime(date_format))
        current_date += timedelta(days=30)
    dates_list.append(end_date.strftime(date_format)) 
    return dates_list

# Function to get a list of URLs containing release dates from start_date to end_date
def get_urls(start_date = datetime(1900, 1, 1), end_date  = datetime(2030, 1, 1)):
    """
    Generates a list of URLs containing release dates from start_date to end_date.

    Args:
        start_date (datetime, optional): The start date. Defaults to January 1, 1900.
        end_date (datetime, optional): The end date. Defaults to January 1, 2030.

    Returns:
        list: A list of URLs.
    """
    dates_list = get_dates(start_date, end_date)
    date_format = "%Y-%m-%d"

    # Create pairs of consecutive dates
    date_pairs = [f'https://www.imdb.com/search/title/?title_type=feature&release_date={dates_list[i]},{(datetime.strptime(dates_list[i+1], date_format) - timedelta(days=1)).strftime(date_format)}' for i in range(0, len(dates_list)-1)]
    
    # This line was commented out because it will cause issues for testing with a small number of dates
    # date_pairs.insert(0, f'https://www.imdb.com/search/title/?title_type=feature&release_date=,{(datetime.strptime(dates_list[0], date_format) - timedelta(days=1)).strftime(date_format)}')
    # date_pairs.append(f'https://www.imdb.com/search/title/?title_type=feature&release_date={dates_list[-1]},')

    #print(date_pairs)
    return date_pairs

# Function to get a list of URL pages for a specific URL
def get_pages(url):
    """
    Generates a list of URL pages for a specific URL.

    Args:
        url (str): The base URL.

    Returns:
        list: A list of page URLs.
    """
    start = 1
    pages = []

    while True:
        page_url = f'{url}&start={start}&ref_=adv_nxt'
        start +=50
        movie_data = get_html_data(page_url)
        if not movie_data:
            break
        pages.append(page_url)
    
    return pages

# Function to get the HTML content from a URL
def get_html_data(url):
    """
    Retrieves the HTML content from a given URL.

    Args:
        url (str): The URL.

    Returns:
        list: A list of movie data.
    """
    # request page source from url
    page = requests.get(url)
    soup = BeautifulSoup(page.content, 'html.parser')

    # get all divs that contain info about a movie
    movie_data = soup.findAll('div', attrs = {'class': 'lister-item mode-advanced'})
    return movie_data

# Function to parse movie data and return a DataFrame
def get_data(movie_data):
    """
    Parses movie data and returns a DataFrame.

    Args:
        movie_data (list): List of movie data.

    Returns:
        DataFrame: DataFrame containing movie information.
    """

    movie_name, year, time, rating, metascore, votes, gross, genre = [], [], [], [], [], [], [], []
    
    for item in movie_data:
        name = item.h3.a.text
        year_of_release = item.h3.find('span', class_ = 'lister-item-year text-muted unbold').text.replace('(', '').replace(')', '')
        year_match = re.search(r'\d{4}', year_of_release)
        year_of_release = year_match.group() if year_match else 'N/A'

        runtime_element = item.p.find('span', class_ = 'runtime')
        runtime = runtime_element.text.replace(' min', '') if runtime_element is not None else 'N/A'

        genres_element = item.p.find('span', class_ = 'genre')
        genres = genres_element.text.strip().replace('\n', '')  if genres_element is not None else 'N/A'
        
        rate_element = item.find('div', class_ = 'inline-block ratings-imdb-rating')
        rate = rate_element.text.replace('\n', '')  if rate_element is not None else 'N/A'
        
        meta = item.find('span', class_ = 'metascore').text.replace(' ', '') if item.find('span', class_ = 'metascore') else 'N/A'
        value = item.find_all('span', attrs = {'name': 'nv'})
        vote = value[0].text if len(value) > 0 else 'N/A'
        
        if vote == 'N/A':
            vote = vote
        elif 'K' in vote:
            vote = float(vote.replace('K', '')) * 1000
        elif 'M' in votes:
            vote = float(vote.replace('M', '')) * 1000000
        else :
            vote = int(vote.replace(",", ""))
        grosses = value[0].text if len(value) > 1 else 'N/A'

        movie_name.append(name)
        year.append(year_of_release)
        time.append(runtime)
        rating.append(rate)
        metascore.append(meta)
        votes.append(vote)
        gross.append(grosses)
        genre.append(genres)

    movieDF = pd.DataFrame({'Name of movie': movie_name, 'Year of release': year, 'Watchtime': time, 'Genre': genre, 'Movie Rating': rating, 'Metascore': metascore, 'Votes': votes, 'Gross collection': gross})
    return movieDF
