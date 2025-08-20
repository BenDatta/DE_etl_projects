import requests
import pandas as pd
from datetime import datetime

API_KEY = "03696cf98d54227b4b3b4a14e0abd019" 
city = "Lisbon"

url = f"http://api.openweathermap.org/data/2.5/weather?q={city}&appid={API_KEY}&units=metric"

def weather_etl(url):
    response = requests.get(url)
    data = response.json()

    city = data["name"]
    weather_description = data["weather"][0]['description']
    temp = data["main"]["temp"]
    feels_like = data["main"]["feels_like"]
    min_temp = data["main"]["temp_min"]
    max_temp = data["main"]["temp_max"]
    pressure = data["main"]["pressure"]
    humidity = data["main"]["humidity"]
    wind_speed = data["wind"]["speed"]

    # Convert Unix timestamps to datetime objects with timezone offset
    time_of_record = datetime.utcfromtimestamp(data['dt'] + data['timezone'])
    sunrise_time = datetime.utcfromtimestamp(data['sys']['sunrise'] + data['timezone'])
    sunset_time = datetime.utcfromtimestamp(data['sys']['sunset'] + data['timezone'])

    # Create a list of dictionaries (so DataFrame can create rows properly)
    extracted_data = [{
        'City': city,
        'Weather_description': weather_description,
        "Temperature (°C)": temp,
        "Feels Like (°C)": feels_like,
        "Minimum Temp (°C)": min_temp,
        "Maximum Temp (°C)": max_temp,
        "Pressure": pressure,
        "Humidity": humidity,
        "Wind Speed": wind_speed,
        "Time of Record": time_of_record,
        "Sunrise (Local Time)": sunrise_time,
        "Sunset (Local Time)": sunset_time
    }]

    return pd.DataFrame(extracted_data)

# Call the function
df_weather = weather_etl(url)

# Save to CSV
df_weather.to_csv('weather.csv', index=False)

print("✅ Weather data saved to weather.csv")
