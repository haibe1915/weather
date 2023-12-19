from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
import pandas as pd

location = {
    'Location': ['Sydney, Australia', 'New York City, USA', 'Tokyo, Japan', 'London, United Kingdom', 'Rio de Janeiro, Brazil',
                 'Cape Town, South Africa', 'Moscow, Russia', 'Paris, France', 'Beijing, China', 'Rome, Italy',
                 'Cairo, Egypt', 'Buenos Aires, Argentina', 'Mumbai, India', 'Toronto, Canada', 'Berlin, Germany',
                 'Seoul, South Korea', 'Mexico City, Mexico', 'Madrid, Spain', 'Istanbul, Turkey', 'Athens, Greece',
                 'Sydney, Australia', 'New York City, USA', 'Tokyo, Japan', 'London, United Kingdom', 'Rio de Janeiro, Brazil',
                 'Cape Town, South Africa', 'Moscow, Russia', 'Paris, France', 'Beijing, China', 'Rome, Italy',
                 'Cairo, Egypt', 'Buenos Aires, Argentina', 'Mumbai, India', 'Toronto, Canada', 'Berlin, Germany',
                 'Seoul, South Korea', 'Mexico City, Mexico', 'Madrid, Spain', 'Istanbul, Turkey', 'Athens, Greece',
                 'Sydney, Australia', 'New York City, USA', 'Tokyo, Japan', 'London, United Kingdom', 'Rio de Janeiro, Brazil',
                 'Cape Town, South Africa', 'Moscow, Russia', 'Paris, France', 'Beijing, China', 'Rome, Italy'],
    'Latitude': [-33.8688, 40.7128, 35.6895, 51.5074, -22.9068, -33.9249, 55.7512, 48.8566, 39.9042, 41.9028,
                 30.0444, -34.6037, 19.0760, 43.6511, 52.5200, 37.5665, 19.4326, 40.4168, 41.0082, 37.9838,
                 -33.8688, 40.7128, 35.6895, 51.5074, -22.9068, -33.9249, 55.7512, 48.8566, 39.9042, 41.9028,
                 30.0444, -34.6037, 19.0760, 43.6511, 52.5200, 37.5665, 19.4326, 40.4168, 41.0082, 37.9838,
                 -33.8688, 40.7128, 35.6895, 51.5074, -22.9068, -33.9249, 55.7512, 48.8566, 39.9042, 41.9028],
    'Longitude': [151.2093, -74.0060, 139.6917, -0.1278, -43.1729, 18.4241, 37.6184, 2.3522, 116.4074, 12.4964,
                  31.2357, -58.3816, 72.8777, -79.3470, 13.4050, 126.9780, -99.1332, -3.7038, 28.9784, 23.7275,
                  151.2093, -74.0060, 139.6917, -0.1278, -43.1729, 18.4241, 37.6184, 2.3522, 116.4074, 12.4964,
                  31.2357, -58.3816, 72.8777, -79.3470, 13.4050, 126.9780, -99.1332, -3.7038, 28.9784, 23.7275,
                  151.2093, -74.0060, 139.6917, -0.1278, -43.1729, 18.4241, 37.6184, 2.3522, 116.4074, 12.4964]
}

location = pd.DataFrame(location)
location = location.drop_duplicates(subset='Location')
location

def crawl_data():
    api_key = 'ed094a8e64024f09be76b62d281ece64'
    df = pd.DataFrame(columns=['lat', 'lon', 'name', 'timestamp_local','timestamp_utc', 'app_temp', 'azimuth', 'clouds', 'dewpt', 'dhi', 'elev_angle', 'ghi', 'pod', 'precip', 'pres', 'revision_status', 'rh', 'slp', 'snow', 'solar_rad','temp','ts','uv','vis', 'weather_code', 'weather_description', 'wind_dir','wind_gust_spd','wind_spd'])
    index = 19
    lat = location.loc[index,'Latitude']
    lon = location.loc[index,'Longitude']
    city = location.loc[index,'Location']
    start_date = '2022-01-11'
    end_date = '2022-01-12'
    url = f'https://api.weatherbit.io/v2.0/history/hourly?key={api_key}&lat={lat}&lon={lon}&start_date={start_date}&end_date={end_date}'
    response = requests.get(url)
    print(response)

    data = response.json()
    list = data['data']
    for i in list:
        row = {'lat': lat,
            'lon': lon,
            'name': data['city_name'],
            'timestamp_local': i['timestamp_local'],
            'timestamp_utc': i['timestamp_utc'],
            'app_temp': i['app_temp'],
            'azimuth': i['azimuth'],
            'dewpt': i['dewpt'],
            'dhi': i['dhi'],
            'elev_angle': i['elev_angle'],
            'ghi': i['ghi'],
            'pod': i['pod'],
            'precip': i['precip'],
            'pres': i['pres'],
            'clouds': i['clouds'],
            'wind_spd': i['wind_spd'],
            'wind_dir': i['wind_dir'],
            'wind_gust_spd': i['wind_gust_spd'],
            'revision_status': i['revision_status'],
            'rh': i['rh'],
            'slp': i['slp'],
            'snow': i['snow'],
            'solar_rad': i['solar_rad'],
            'temp': i['temp'],
            'ts': i['ts'],
            'uv': i['uv'],
            'vis': i['vis'],
            'weather_code': i['weather']['code'],
            'weather_description': i['weather']['description'],
            }
        df.loc[len(df)] = row


dag = DAG('data_crawling_dag', start_date=datetime(2023, 12, 20), schedule_interval='0 8 * * *')

crawl_task = PythonOperator(
    task_id='crawl_data_task',
    python_callable=crawl_data,
    dag=dag
)
