import requests
import pandas as pd
from google.cloud import storage
from datetime import datetime, timedelta
import re
import uuid

def convert_time(timestamp_ms):
    return datetime.utcfromtimestamp(timestamp_ms / 1000).strftime('%Y-%m-%d %H:%M:%S')

def get_earthquake_data():
    end_time = datetime.now()
    start_time = end_time - timedelta(days=15)

    url = "https://earthquake.usgs.gov/fdsnws/event/1/query"
    params = {
        "format": "geojson",
        "starttime": start_time.strftime("%Y-%m-%d"),
        "endtime": end_time.strftime("%Y-%m-%d")
    }

    response = requests.get(url, params=params)

    if response.status_code == 200 and response.text.strip():
        data = response.json()
    else:
        print("❌ Error fetching data:", response.status_code)
        print(response.text)
        return

    earthquakes = []
    features = data.get("features", [])

    for feature in features:
        properties = feature["properties"]
        geometry = feature["geometry"]
        
        earthquake = {
            "time": properties["time"],
            "place": properties["place"],
            "magnitude": properties["mag"],
            "longitude": geometry["coordinates"][0],
            "latitude": geometry["coordinates"][1],
            "depth": geometry["coordinates"][2],
        }
        earthquakes.append(earthquake)

    df = pd.DataFrame(earthquakes)

    # Convert time
    df['time'] = df['time'].apply(convert_time)

    # Add unique id
    df['id'] = [str(uuid.uuid4()) for _ in range(len(df))]

    # Clean place
    df = clean_place_column(df)

    df.to_csv("earthquake_data_csv", index=False)
    print("✅ Data saved to earthquake_data_csv")


def clean_place_column(df):
    def extract_place(place):
        if "of" in place:
            match = re.search(r"of\s+(.*)", place)
            if match:
                location = match.group(1)
                return location.split(",")[0].strip()
        return place.strip()

    df["place"] = df["place"].apply(extract_place)
    return df


def upload_to_gcs(bucket_name, file_name, content_type):
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(file_name)
    blob.upload_from_filename(file_name, content_type=content_type)
    print(f"📤 Uploaded {file_name} to gs://{bucket_name}/")
    return blob.public_url


# Run ETL
get_earthquake_data()
upload_to_gcs("earthquake_data_1", "earthquake_data_csv", "text/csv")
