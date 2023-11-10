from movies_etl.scraping import get_dates, get_urls, get_pages, get_html_data, get_data
from datetime import datetime
import requests_mock
import pandas as pd

def test_get_dates():
    # Arrange
    start_date = datetime(1900, 1, 1)
    end_date = datetime(1900, 5, 1)
    expected_dates = ["1900-01-01", "1900-01-31", "1900-03-02", "1900-04-01", "1900-05-01"]
    
    # Act
    result = get_dates(start_date=start_date, end_date=end_date)
    
    # Assert
    assert result == expected_dates

def test_get_dates_2():
    # Arrange
    start_date = datetime(1900, 1, 1)
    end_date = datetime(1900, 4, 15)
    expected_dates = ["1900-01-01", "1900-01-31", "1900-03-02", "1900-04-01", "1900-04-15"]
    
    # Act
    result = get_dates(start_date=start_date, end_date=end_date)
    
    # Assert
    assert result == expected_dates

def test_get_urls():    
    # Arrange
    start_date = datetime(1900, 1, 1)
    end_date = datetime(1900, 5, 1)
    expected_urls = [
        # "https://www.imdb.com/search/title/?title_type=feature&release_date=,1899-12-31",
        "https://www.imdb.com/search/title/?title_type=feature&release_date=1900-01-01,1900-01-30", 
        "https://www.imdb.com/search/title/?title_type=feature&release_date=1900-01-31,1900-03-01", 
        "https://www.imdb.com/search/title/?title_type=feature&release_date=1900-03-02,1900-03-31", 
        "https://www.imdb.com/search/title/?title_type=feature&release_date=1900-04-01,1900-04-30", 
        # "https://www.imdb.com/search/title/?title_type=feature&release_date=1900-05-01,"
    ]
    
    # Act
    result = get_urls(start_date=start_date, end_date=end_date)
    
    # Assert
    assert result == expected_urls

def test_get_pages():    
    # Arrange
    url = 'https://www.imdb.com/search/title/?title_type=feature&release_date=,1899-12-31'
    expected_pages = ['https://www.imdb.com/search/title/?title_type=feature&release_date=,1899-12-31&start=1&ref_=adv_nxt']

    # Act
    result = get_pages(url)
    
    # Assert
    assert result == expected_pages

def test_get_pages_2():    
    # Arrange
    url = 'https://www.imdb.com/search/title/?title_type=feature&release_date=1900-01-01,1910-01-01'
    expected_pages = ['https://www.imdb.com/search/title/?title_type=feature&release_date=1900-01-01,1910-01-01&start=1&ref_=adv_nxt', 'https://www.imdb.com/search/title/?title_type=feature&release_date=1900-01-01,1910-01-01&start=51&ref_=adv_nxt', 'https://www.imdb.com/search/title/?title_type=feature&release_date=1900-01-01,1910-01-01&start=101&ref_=adv_nxt']

    # Act
    result = get_pages(url)
    
    # Assert
    assert result == expected_pages


def test_get_html_data():
    # Arrange: Mock the HTTP response
    with requests_mock.Mocker() as mock:
        mock.get('http://example.com', text='<html><body><div class="lister-item mode-advanced">Movie 1</div><div class="lister-item mode-advanced">Movie 2</div></body></html>')
        
        # Act: Call the function with the mocked URL
        result = get_html_data('http://example.com')
        
        # Assert: Check if the function returns the correct data
        assert len(result) == 2
        assert result[0].text == 'Movie 1'
        assert result[1].text == 'Movie 2'

def test_get_data():
    # Arrange
    page_url = 'https://www.imdb.com/search/title/?title_type=feature&release_date=,1899-12-31&start=1&ref_=adv_nxt'
    expected_DF = pd.DataFrame({
        'Name of movie': ['Miss Jerry'],
        'Year of release': ['1894'],
        'Watchtime': ['45'],
        'Genre': ['Romance'],
        'Movie Rating': ['5.3'],
        'Metascore': ['N/A'],
        'Votes': [207],
        'Gross collection': ['N/A']
    })
    

    # Act
    movie_data = get_html_data(page_url)
    assert movie_data != None
    result = get_data(movie_data)
    
    # Assert
    assert  result.equals(expected_DF)
