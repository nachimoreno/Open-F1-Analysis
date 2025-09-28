import os
import datetime


def cache_response(df, endpoint, filename):
    """Save output of a certain response so you can query it locally"""
    directory = os.path.join("cache", endpoint)
    if not os.path.exists(directory):
        os.makedirs(directory)

    filepath = os.path.join(directory, filename)
    filetype = ".csv"
    final_file_path = filepath + filetype
    print(f"Writing to file: {final_file_path}")
    df.to_csv(final_file_path, index=False)


def clear_cache(date):
    """Clear cache from before a certain date"""
    pass


def save_analysis(df, analysis, filename):
    """Save the result of an analysis"""
    directory = os.path.join("analyses", analysis)
    if not os.path.exists(directory):
        os.makedirs(directory)

    filepath = os.path.join(directory, filename)
    filetype = ".csv"
    final_file_path = filepath + filetype
    print(f"Writing to file: {final_file_path}")
    df.to_csv(final_file_path, index=False)


def save_last_get_date():
    """Save the date and time of the last get request"""
    directory = "cache/util"
    if not os.path.exists(directory):
        os.makedirs(directory)

    with open(os.path.join(directory, "last_get_date.txt"), "w") as f:
        f.write(str(datetime.datetime.now()))


def read_last_get_date():
    """Return the date and time of the last get request"""
    directory = "cache/util"

    try:
        with open(os.path.join(directory, "last_get_date.txt"), "r") as f:
            last_get_date = f.read()
    except FileNotFoundError:
        last_get_date = str(datetime.datetime.now() )

    return datetime.datetime.strptime(last_get_date, "%Y-%m-%d %H:%M:%S.%f")
