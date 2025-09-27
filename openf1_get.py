import requests
import datetime
import openf1_file_helpers as fh
import time
import pandas as pd

BASE_URL = "https://api.openf1.org/v1/"
TIME_BETWEEN_REQUESTS = 5
VALID_ENDPOINTS_AND_PARAMETERS = {
    "car_data": [  # Some data about each car, at a sample rate of about 3.7 Hz.
        "brake",  # Whether the brake pedal is pressed (100) or not (0).
        "date",  # The UTC date and time, in ISO 8601 format.
        "driver_number",  # The unique number assigned to an F1 driver
        "drs",  # The Drag Reduction System (DRS) status (see mapping table below).
        "meeting_key",  # The unique identifier for the meeting. Use latest to identify the latest or current meeting.
        "n_gear",  # Current gear selection, ranging from 1 to 8. 0 indicates neutral or no gear engaged.
        "rpm",  # Revolutions per minute of the engine.
        "session_key",  # The unique identifier for the session. Use latest to identify the latest or current session.
        "speed",  # Velocity of the car in km/h.
        "throttle"  # Percentage of maximum engine power being used.
    ],

    "drivers": [  # Provides information about drivers for each session.
        "broadcast_name",  # The driver's name, as displayed on TV.
        "country_code",  # A code that uniquely identifies the country.
        "driver_number",  # The unique number assigned to an F1 driver
        "first_name",  # The driver's first name.
        "full_name",  # The driver's full name.
        "headshot_url",  # URL of the driver's face photo.
        "last_name",  # The driver's last name.
        "meeting_key",  # The unique identifier for the meeting. Use latest to identify the latest or current meeting.
        "name_acronym",  # Three-letter acronym of the driver's name.
        "session_key",  # The unique identifier for the session. Use latest to identify the latest or current session.
        "team_colour",  # The hexadecimal color value (RRGGBB) of the driver's team.
        "team_name"  # Name of the driver's team.
    ],

    "intervals": [
        # Fetches real-time interval data between drivers and their gap to the race leader. Available during races only, with updates approximately every 4 seconds.
        "date",  # The UTC date and time, in ISO 8601 format.
        "driver_number",  # The unique number assigned to an F1 driver
        "gap_to_leader",  # The time gap to the race leader in seconds, +1 LAP if lapped, or null for the race leader.
        "interval",  # The time gap to the car ahead in seconds, +1 LAP if lapped, or null for the race leader.
        "meeting_key",  # The unique identifier for the meeting. Use latest to identify the latest or current meeting.
        "session_key"  # The unique identifier for the session. Use latest to identify the latest or current session.
    ],

    "laps": [  # Provides detailed information about individual laps.
        "date_start",  # The UTC starting date and time, in ISO 8601 format. This date is approximate.
        "driver_number",  # The unique number assigned to an F1 driver
        "duration_sector_1",  # The time taken, in seconds, to complete the first sector of the lap.
        "duration_sector_2",  # The time taken, in seconds, to complete the second sector of the lap.
        "duration_sector_3",  # The time taken, in seconds, to complete the third sector of the lap.
        "i1_speed",  # The speed of the car, in km/h, at the first intermediate point on the track.
        "i2_speed",  # The speed of the car, in km/h, at the second intermediate point on the track.
        "is_pit_out_lap",
        # A boolean value indicating whether the lap is an "out lap" from the pit (true if it is, false otherwise).
        "lap_duration",  # The total time taken, in seconds, to complete the entire lap.
        "lap_number",  # The sequential number of the lap within the session (starts at 1).
        "meeting_key",  # The unique identifier for the meeting. Use latest to identify the latest or current meeting.
        "segments_sector_1",
        # A list of values representing the "mini-sectors" within the first sector (see mapping table below).
        "segments_sector_2",
        # A list of values representing the "mini-sectors" within the second sector (see mapping table below).
        "segments_sector_3",
        # A list of values representing the "mini-sectors" within the third sector (see mapping table below).
        "session_key",  # The unique identifier for the session. Use latest to identify the latest or current session.
        "st_speed"
        # The speed of the car, in km/h, at the speed trap, which is a specific point on the track where the highest speeds are usually recorded.
    ],

    "location": [
        # The approximate location of the cars on the circuit, at a sample rate of about 3.7 Hz. Useful for gauging their progress along the track, but lacks details about lateral placement — i.e. whether the car is on the left or right side of the track. The origin point (0, 0, 0) appears to be arbitrary and not tied to any specific location on the track.
        "date",  # The UTC date and time, in ISO 8601 format.
        "driver_number",  # The unique number assigned to an F1 driver
        "meeting_key",  # The unique identifier for the meeting. Use latest to identify the latest or current meeting.
        "session_key",  # The unique identifier for the session. Use latest to identify the latest or current session.
        "x",
        # The 'x' value in a 3D Cartesian coordinate system representing the current approximate location of the car on the track.
        "y",
        # The 'y' value in a 3D Cartesian coordinate system representing the current approximate location of the car on the track.
        "z"
        # The 'z' value in a 3D Cartesian coordinate system representing the current approximate location of the car on the track.
    ],

    "meetings": [
        # Provides information about meetings. A meeting refers to a Grand Prix or testing weekend and usually includes multiple sessions (practice, qualifying, race, ...).
        "circuit_key",  # The unique identifier for the circuit where the event takes place.
        "circuit_short_name",  # The short or common name of the circuit where the event takes place.
        "country_code",  # A code that uniquely identifies the country.
        "country_key",  # The unique identifier for the country where the event takes place.
        "country_name",  # The full name of the country where the event takes place.
        "date_start",  # The UTC starting date and time, in ISO 8601 format.
        "gmt_offset",
        # The difference in hours and minutes between local time at the location of the event and Greenwich Mean Time (GMT).
        "location",  # The city or geographical location where the event takes place.
        "meeting_key",  # The unique identifier for the meeting. Use latest to identify the latest or current meeting.
        "meeting_name",  # The name of the meeting.
        "meeting_official_name",  # The official name of the meeting.
        "year"  # The year the event takes place.
    ],

    "overtakes": [
        # Provides information about overtakes. An overtake refers to one driver (the overtaking driver) exchanging positions with another driver (the overtaken driver). This includes both on-track passes and position changes resulting from pit stops or post-race penalties. This data is only available during races and may be incomplete.
        "date",  # The UTC date and time, in ISO 8601 format.
        "meeting_key",  # The unique identifier for the meeting. Use latest to identify the latest or current meeting.
        "overtaken_driver_number",  # The unique number assigned to the overtaken F1 driver.
        "overtaking_driver_number",  # The unique number assigned to the overtaking F1 driver.
        "position",  # The position of the overtaking F1 driver after the overtake was completed (starts at 1).
        "session_key"  # The unique identifier for the session. Use latest to identify the latest or current session.
    ],

    "pit": [  # Provides information about cars going through the pit lane.
        "date",  # The UTC date and time, in ISO 8601 format.
        "driver_number",  # The unique number assigned to an F1 driver.
        "lap_number",  # The sequential number of the lap within the session (starts at 1).
        "meeting_key",  # The unique identifier for the meeting. Use latest to identify the latest or current meeting.
        "pit_duration",  # The time spent in the pit, from entering to leaving the pit lane, in seconds.
        "session_key"  # The unique identifier for the session. Use latest to identify the latest or current session.
    ],

    "position": [  # Provides driver positions throughout a session, including initial placement and subsequent changes.
        "date",  # The UTC date and time, in ISO 8601 format.
        "driver_number",  # The unique number assigned to an F1 driver.
        "meeting_key",  # The unique identifier for the meeting. Use latest to identify the latest or current meeting.
        "position",  # Position of the driver (starts at 1).
        "session_key"  # The unique identifier for the session. Use latest to identify the latest or current session.
    ],

    "race_control": [  # Provides information about race control (racing incidents, flags, safety car, ...).
        "category",  # The category of the event (CarEvent, Drs, Flag, SafetyCar, ...).
        "date",  # The UTC date and time, in ISO 8601 format.
        "driver_number",  # The unique number assigned to an F1 driver.
        "flag",  # Type of flag displayed (GREEN, YELLOW, DOUBLE YELLOW, CHEQUERED, ...).
        "lap_number",  # The sequential number of the lap within the session (starts at 1).
        "meeting_key",  # The unique identifier for the meeting. Use latest to identify the latest or current meeting.
        "message",  # Description of the event or action.
        "scope",  # The scope of the event (Track, Driver, Sector, ...).
        "sector",  # Segment ("mini-sector") of the track where the event occurred? (starts at 1).
        "session_key",  # The unique identifier for the session. Use latest to identify the latest or current session.
    ],

    "sessions": [
        # Provides information about sessions. A session refers to a distinct period of track activity during a Grand Prix or testing weekend (practice, qualifying, sprint, race, ...).
        "circuit_key",  # The unique identifier for the circuit where the event takes place.
        "circuit_short_name",  # The short or common name of the circuit where the event takes place.
        "country_code",  # A code that uniquely identifies the country.
        "country_key",  # The unique identifier for the country where the event takes place.
        "country_name",  # The full name of the country where the event takes place.
        "date_end",  # The UTC ending date and time, in ISO 8601 format.
        "date_start",  # The UTC starting date and time, in ISO 8601 format.
        "gmt_offset",
        # The difference in hours and minutes between local time at the location of the event and Greenwich Mean Time (GMT).
        "location",  # The city or geographical location where the event takes place.
        "meeting_key",  # The unique identifier for the meeting. Use latest to identify the latest or current meeting.
        "session_key",  # The unique identifier for the session. Use latest to identify the latest or current session.
        "session_name",  # The name of the session (Practice 1, Qualifying, Race, ...).
        "session_type",  # The type of the session (Practice, Qualifying, Race, ...).
        "year"  # The year the event takes place.
    ],

    "session_result": [  # Provides standings after a session.
        "dnf",
        # Indicates whether the driver Did Not Finish the race. This can be true only for qualifying and race sessions.
        "dns",
        # Indicates whether the driver Did Not Start the race. This can be true only for qualifying and race sessions.
        "dsq",  # Indicates whether the driver was disqualified.
        "driver_number",  # The unique number assigned to an F1 driver.
        "duration",
        # Either the best lap time (for practice or qualifying), or the total race time (for races), in seconds. In qualifying, this is an array of three values for Q1, Q2, and Q3.
        "gap_to_leader",
        # The time gap to the session leader in seconds, or +N LAP(S) if the driver was lapped. In qualifying, this is an array of three values for Q1, Q2, and Q3.
        "number_of_laps",  # Total number of laps completed during the session.
        "meeting_key",  # The unique identifier for the meeting. Use latest to identify the latest or current meeting.
        "position",  # The driver’s final position at the end of the session.
        "session_key"  # The unique identifier for the session. Use latest to identify the latest or current session.
    ],

    "starting_grid": [  # Provides the starting grid for the upcoming race.
        "driver_number",  # The unique number assigned to an F1 driver.
        "lap_duration",  # Duration, in seconds, of the qualifying lap.
        "meeting_key",  # The unique identifier for the meeting. Use latest to identify the latest or current meeting.
        "position",  # Position on the grid.
        "session_key"  # The unique identifier for the session. Use latest to identify the latest or current session.
    ],

    "stints": [
        # Provides information about individual stints. A stint refers to a period of continuous driving by a driver during a session.
        "compound",  # The specific compound of tyre used during the stint (SOFT, MEDIUM, HARD, ...).
        "driver_number",  # The unique number assigned to an F1 driver.
        "lap_end",  # Number of the last completed lap in this stint.
        "lap_start",  # Number of the initial lap in this stint (starts at 1).
        "meeting_key",  # The unique identifier for the meeting. Use latest to identify the latest or current meeting.
        "session_key",  # The unique identifier for the session. Use latest to identify the latest or current session.
        "stint_number",  # The sequential number of the stint within the session (starts at 1).
        "tyre_age_at_start"  # The age of the tyres at the start of the stint, in laps completed.
    ],

    "team_radio": [
        # Provides a collection of radio exchanges between Formula 1 drivers and their respective teams during sessions. Please note that only a limited selection of communications are included, not the complete record of radio interactions.
        "date",  # The UTC date and time, in ISO 8601 format.
        "driver_number",  # The unique number assigned to an F1 driver.
        "meeting_key",  # The unique identifier for the meeting. Use latest to identify the latest or current meeting.
        "recording_url",  # URL of the radio recording.
        "session_key"  # The unique identifier for the session. Use latest to identify the latest or current session.
    ],

    "weather": [  # The weather over the track, updated every minute.
        "air_temperature",  # Air temperature (°C).
        "date",  # The UTC date and time, in ISO 8601 format.
        "humidity",  # Relative humidity (%).
        "meeting_key",  # The unique identifier for the meeting. Use latest to identify the latest or current meeting.
        "pressure",  # Air pressure (mbar).
        "rainfall",  # Whether there is rainfall.
        "session_key",  # The unique identifier for the session. Use latest to identify the latest or current session.
        "track_temperature",  # Track temperature (°C).
        "wind_direction",  # Wind direction (°), from 0° to 359°.
        "wind_speed"  # Wind speed (m/s).
    ]
}


def parse_request(endpoint, params):
    """Parse request parameters to see if the request is valid for the API endpoint"""
    if endpoint not in VALID_ENDPOINTS_AND_PARAMETERS.keys():
        raise Exception("Error parsing get() request: Invalid API endpoint", endpoint)

    for param in params:
        if param not in VALID_ENDPOINTS_AND_PARAMETERS[endpoint]:
            raise Exception("Error parsing get() request: Invalid parameter for the endpoint", endpoint, param)

    if params.get('date_start') is not None:
        params['date_start'] = params['date_start'].replace("=", "")

    if params.get('date_end') is not None:
        params['date_end'] = params['date_end'].replace("=", "")

    return True


def parse_response(response):
    """Parse response to see if it's valid"""
    if response is None:
        raise Exception("Error fetching API response: Request never submitted (response is None)")
    elif response.status_code == 200:
        return True
    elif response.status_code == 400:
        raise Exception("Error fetching API response: Bad request (400)")
    elif response.status_code == 401:
        raise Exception("Error fetching API response: Unauthorized (401)")
    elif response.status_code == 403:
        raise Exception("Error fetching API response: Forbidden (403)")
    elif response.status_code == 408:
        raise Exception("Error fetching API response: Request timeout (408)")
    elif response.status_code == 500:
        raise Exception("Error fetching API response: Internal server error (500)")
    elif response.status_code == 502:
        raise Exception("Error fetching API response: Bad gateway (502)")
    elif response.status_code == 503:
        raise Exception("Error fetching API response: Service unavailable (503)")
    elif response.status_code == 504:
        raise Exception("Error fetching API response: Gateway timeout (504)")
    elif response.status_code == 511:
        raise Exception("Error fetching API response: Network authentication required (511)")
    else:
        raise Exception(f"Error fetching API response: Unexpected status code ({response.status_code})")


def spam_check():
    current_time = datetime.datetime.now()
    last_get_date = fh.read_last_get_date()
    diff = current_time - last_get_date
    if diff < datetime.timedelta(seconds=TIME_BETWEEN_REQUESTS):
        print(f"get(): Sleeping for {round(TIME_BETWEEN_REQUESTS - diff.total_seconds())} seconds to prevent spamming the API")
        time.sleep(TIME_BETWEEN_REQUESTS - diff.total_seconds())

    return


def parse_operators(url):
    """For special cases like date_end<=2023"""
    if "%3C" in url or "%3E" in url:
        url = url.replace("=%3C", "<%3D")
        url = url.replace("=%3E", ">%3D")

    return url


def response_to_df(response):
    data = response.json()

    if isinstance(data, list):
        df = pd.DataFrame.from_records(data)

    elif isinstance(data, dict):
        for key in ("results", "data", "items"):
            if key in data and isinstance(data[key], list):
                df = pd.DataFrame.from_records(data[key])
                break
        else:
            df = pd.json_normalize(data)
    else:
        raise TypeError(f"Unexpected JSON type: {type(data)}")

    return df


def get(endpoint, params):
    """Fetch the response from the desired API endpoint"""
    params = dict(sorted(params.items()))

    if parse_request(endpoint, params):
        endpoint_url = BASE_URL + endpoint
        request = requests.Request("GET", endpoint_url, params=params)
        prepared = request.prepare()
        final_url = parse_operators(prepared.url)
        spam_check()
        response = requests.get(final_url)
        fh.save_last_get_date()
        if parse_response(response):
            # filename = final_url[len(endpoint_url) + 1:]
            df = response_to_df(response)
            # fh.cache_response(df, endpoint, filename)
            return df

    raise Exception("Error fetching API response: Something unexpected went wrong.")
