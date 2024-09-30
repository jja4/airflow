# from airflow.models import Variable
import requests
import json
from datetime import datetime
from airflow.models import Variable
import os
import time

def fetch_weather_data():
    """
    Fetches weather data from the OpenWeatherMap API for the city specified in the Airflow Variable 'cities'.
    Stores the data in a JSON file with a name corresponding to the current date and time.
    """
    # Set the city list as a JSON string in the Airflow Variable 'cities'
    cities_list = ['berlin', 'paris', 'london']
    cities_json = json.dumps(cities_list)
    Variable.set(key='cities', value=cities_json)

    # Get the city list from the Airflow Variable 'cities'
    cities = Variable.get('cities', default_var=['berlin'], deserialize_json=True)

    # API endpoint and API key
    api_endpoint = "https://api.openweathermap.org/data/2.5/weather"
    api_key = "451116abdac3994c16df8b8cd45921b3"

    for city in cities:
        # Make the API request
        params = {"q": city, "appid": api_key}
        response = requests.get(api_endpoint, params=params)

        # Check if the request was successful
        if response.status_code == 200:
            # Get the current date and time
            now = datetime.now()
            file_name = now.strftime("%Y-%m-%d %H:%M:%S.json")

            # Create the directory if it doesn't exist
            directory = '/app/raw_files'
            os.makedirs(directory, exist_ok=True)

            # Save the data to a JSON file
            file_path = os.path.join(directory, file_name)
            with open(file_path, "w") as file:
                json.dump(response.json(), file, indent=4)
            print(f"Weather data for {city} saved to {file_path}")
        else:
            print(f"Error: {response.status_code} - {response.text}")
        time.sleep(1)

# # Call the function to fetch and save the weather data
# # this is Task 1 within the DAG
# # it should be called once a minute with the schedule_interval
# fetch_weather_data()