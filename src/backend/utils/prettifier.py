def prettify_bin_status(bin_status):
    return {
        'id': bin_status['dev_id'],
        'lastEdit': bin_status['last_edit'],
        'battery': bin_status['battery'],
        'fillLevel': bin_status['fill_level'],
        'temperature': bin_status['temperature']
    }

def prettify_bin_statuses(bin_statuses):
    result = []
    for bin_status in bin_statuses:
        result.append(prettify_bin_status(bin_status=bin_status))
    return result

def prettify_bin_details(bin_status, bin_details):
    result = {}
    if bin_details is not None and bin_status is not None:
        result = {
            'id': bin_status['dev_id'],
            'lastEdit': bin_status['last_edit'],
            'battery': bin_status['battery'],
            'fillLevel': bin_status['fill_level'],
            'temperature': bin_status['temperature'],
            'latitude': bin_details.latitude,
            'longitude': bin_details.longitude,
            'sensorName': bin_details.sensor_name
        }
    return result

def prettify_weather_status(weather_status):
    return {
        'id': weather_status['dev_id'],
        'lastEdit': weather_status['last_edit'],
        'battery': weather_status['battery'],
        'precipitation': weather_status['precipitation'],
        'airTemp': weather_status['air_temp'],
        'windSpeed': weather_status['wind_speed'],
        'windDirection': weather_status['wind_direction'],
        'gustSpeed': weather_status['gust_speed'],
        'vapourPressure': weather_status['vapour_pressure'],
        'atmosphericPressure': weather_status['atmospheric_pressure'],
        'relativeHumidity': weather_status['relative_humidity']
    }

def prettify_weather_statuses(weather_statuses):
    result = []
    for weather_status in weather_statuses:
        result.append(prettify_weather_status(weather_status=weather_status))
    return result

def prettify_weather_details(weather_status, weather_details):
    result = {}
    if weather_details is not None and weather_status is not None:
        result = {
            'id': weather_status['dev_id'],
            'lastEdit': weather_status['last_edit'],
            'battery': weather_status['battery'],
            'precipitation': weather_status['precipitation'],
            'airTemp': weather_status['air_temp'],
            'windSpeed': weather_status['wind_speed'],
            'windDirection': weather_status['wind_direction'],
            'gustSpeed': weather_status['gust_speed'],
            'vapourPressure': weather_status['vapour_pressure'],
            'atmosphericPressure': weather_status['atmospheric_pressure'],
            'relativeHumidity': weather_status['relative_humidity'],
            'latitude': weather_details.latitude,
            'longitude': weather_details.longitude,
            'sensorName': weather_details.sensor_name
        }
    return result
