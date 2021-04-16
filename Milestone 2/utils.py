from typing import Tuple
from datetime import datetime
import pandas as pd
import requests
import pathlib

class CovidDataService:

    # Define data directory
    __data_dir = pathlib.Path("data")

    # Define URLs and names for the data
    __global_aggregates_location = "https://raw.githubusercontent.com/CSSEGISandData/COVID-19/web-data/data/cases_country.csv"

    __global_aggregates_name = "global_aggregates"

    __global_confirmed_cases_location = "https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_time_series/time_series_covid19_confirmed_global.csv"

    __global_confirmed_cases_name = "global_confirmed"

    __global_death_cases_location = "https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_time_series/time_series_covid19_deaths_global.csv"

    __global_death_cases_name = "global_deaths"

    __global_recovered_cases_location = "https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_time_series/time_series_covid19_recovered_global.csv"

    __global_recovered_cases_name = "global_recovered"

    __us_confirmed_cases_location = "https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_time_series/time_series_covid19_confirmed_US.csv"

    __us_confirmed_cases_name = "us_confirmed"

    __us_death_cases_location = "https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_time_series/time_series_covid19_deaths_US.csv"

    __us_death_cases_name = "us_deaths"

    # Create dictionary of data names and URLs
    __location_dict = {
        __global_aggregates_name : __global_aggregates_location,
        __global_confirmed_cases_name : __global_confirmed_cases_location,
        __global_death_cases_name : __global_death_cases_location,
        __global_recovered_cases_name : __global_recovered_cases_location,
        __us_confirmed_cases_name : __us_confirmed_cases_location,
        __us_death_cases_name : __us_death_cases_location
    }

    def __init__(self) -> None:
        
        self.__data_dict = {}

        for key in self.__location_dict:
            self.__data_dict[key] = pd.DataFrame()

    def forced_reload(self) -> None:
        """Retrieves data from source regardless of data cache availability or data cache age and 
        reloads data
        """
        if not self.__data_dir.exists():
            self.__data_dir.mkdir()
        
        for key in self.__location_dict:
            name = key
            url = self.__location_dict[key]
            path = self.__data_dir / f"{name}.csv"

            self.__download_data(name, url)

            if path.exists():
                dataframe = self.__load_csv(str(path))
                self.__data_dict[name] = dataframe

    def __is_it_a_new_day(self, file: pathlib.Path) -> bool:
        
        if not file.exists():
            return True
        
        file_timestamp = file.stat().st_ctime
        creation_date = datetime.utcfromtimestamp(file_timestamp).date()
        now_date = datetime.utcnow().date()
        return now_date > creation_date

    def __download_data(self, name: str, url: str) -> None:    

        r = requests.get(url)
        path = self.__data_dir / f"{name}.csv"

        with open(path, "w") as f:
            f.write(r.text)

    def __load_data_if_needed(self, name: str) -> None:
        path = self.__data_dir / f"{name}.csv"
        url = self.__location_dict[name]

        if not self.__data_dir.exists():
            self.__data_dir.mkdir()

        if not path.exists() or self.__is_it_a_new_day(path):
            self.__download_data(name, url)
        
        if path.exists():
            dataframe = self.__load_csv(str(path))
            self.__data_dict[name] = dataframe

    # Generic method for loading a csv into a pandas dataframe
    def __load_csv(self, path: str) -> pd.DataFrame:
        return pd.read_csv(path)

    # Method for loading global aggregates into a pandas dataframe
    def get_global_aggregates(self) -> pd.DataFrame:
        """Gets global aggregate COVID-19 data
        
        Returns:
            pd.DataFrame: A pandas dataframe with data for global aggregates of COVID-19 
            confirmed, recovered, and death cases
    """
        self.__load_data_if_needed(self.__global_aggregates_name)        
        return self.__data_dict[self.__global_aggregates_name]

    # Method for loading global confirmed cases into a pandas dataframe
    def get_global_confirmed(self) -> pd.DataFrame:
        """Gets time series data for Glocabl COVID-19 confirmed cases

        Returns:
            pd.DataFrame: A pandas dataframe with time series data for Global COVID-19 confirmed cases
        """
        self.__load_data_if_needed(self.__global_confirmed_cases_name)     
        return self.__data_dict[self.__global_confirmed_cases_name]

    # Method for loading global death cases into a pandas dataframe
    def get_global_death(self) -> pd.DataFrame:
        """Gets time series data for Glocabl COVID-19 death cases

        Returns:
            pd.DataFrame: A pandas dataframe with time series data for Global COVID-19 death cases
        """
        self.__load_data_if_needed(self.__global_death_cases_name)  
        return self.__data_dict[self.__global_death_cases_name]

    # Method for loading global recovered cases into a pandas dataframe
    def get_global_recovered(self) -> pd.DataFrame:
        """Gets time series data for Glocabl COVID-19 recovered cases

        Returns:
            pd.DataFrame: A pandas dataframe with time series data for Global COVID-19 recovered cases
        """
        self.__load_data_if_needed(self.__global_recovered_cases_name)  
        return self.__data_dict[self.__global_recovered_cases_name]

    # Method for loading US confirmed cases into a pandas dataframe
    def get_us_confirmed(self) -> pd.DataFrame:
        """Gets time series data for US COVID-19 confirmed cases

        Returns:
            pd.DataFrame: A pandas dataframe with time series data for US COVID-19 confirmed cases
        """
        self.__load_data_if_needed(self.__us_confirmed_cases_name)  
        return self.__data_dict[self.__us_confirmed_cases_name]

    # Method for loading US death cases into a pandas dataframe
    def get_us_death(self) -> pd.DataFrame:
        """Loads time series data for US COVID-19 death cases

        Returns:
            pd.DataFrame: A pandas dataframe with time series data for US COVID-19 death cases
        """
        self.__load_data_if_needed(self.__us_death_cases_name)  
        return self.__data_dict[self.__us_death_cases_name]
