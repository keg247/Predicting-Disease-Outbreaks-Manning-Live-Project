from typing import Tuple
import pandas as pd

# Define URLs for the data
global_aggregates_location = "https://raw.githubusercontent.com/CSSEGISandData/COVID-19/web-data/data/cases_country.csv"

global_confirmed_cases_location = "https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_time_series/time_series_covid19_confirmed_global.csv"

global_death_cases_location = "https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_time_series/time_series_covid19_deaths_global.csv"

global_recovered_cases_location = "https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_time_series/time_series_covid19_recovered_global.csv"

us_confirmed_cases_location = "https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_time_series/time_series_covid19_confirmed_US.csv"

us_death_cases_location = "https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_time_series/time_series_covid19_deaths_US.csv"

# Generic method for loading a csv into a pandas dataframe
def load_csv(path: str) -> pd.DataFrame:
    return pd.read_csv(path)

# Method for loading global aggregates into a pandas dataframe
def load_global_aggregates() -> pd.DataFrame:
    """Loads global aggregate COVID-19 data from URL into a pandas dataframe

    Returns:
        pd.DataFrame: A pandas dataframe with data for global aggregates of COVID-19  
        confirmed, recovered, and death cases
    """
    return load_csv(global_aggregates_location)

# Method for loading global confirmed cases into a pandas dataframe
def load_global_confirmed() -> pd.DataFrame:    
    """Loads time series data for global COVID-19 confirmed cases

    Returns:
        pd.DataFrame: A pandas dataframe with time series data for global COVID-19 cofirmed cases
    """

    return load_csv(global_confirmed_cases_location)

# Method for loading global death cases into a pandas dataframe
def load_global_death() -> pd.DataFrame:
    """Loads time series data for global COVID-19 death cases

    Returns:
        pd.DataFrame: A pandas dataframe with time series data for global confirmed COVID-19 cases
    """

    return load_csv(global_death_cases_location)

# Method for loading global recovered cases into a pandas dataframe
def load_global_recovered() -> pd.DataFrame:
    """Loads time series data for global COVID-19 recovered cases

    Returns:
        pd.DataFrame: A pandas dataframe with time series data for global COVID-19 recovered cases
    """

    return load_csv(global_recovered_cases_location)

# Method for loading US confirmed cases into a pandas dataframe
def load_us_confirmed() -> pd.DataFrame:
    """Loads time series data for US COVID-19 confirmed cases

    Returns:
        pd.DataFrame: A pandas dataframe with time series data for US COVID-19 confirmed cases
    """

    return load_csv(us_confirmed_cases_location)

# Method for loading US death cases into a pandas dataframe
def load_us_death() -> pd.DataFrame:
    """Loads time series data for US COVID-19 death cases

    Returns:
        pd.DataFrame: A pandas dataframe with time series data for US COVID-19 death cases
    """

    return load_csv(us_death_cases_location)
