# Functions used to get data from OASIS API

import requests
import zipfile
import io
import xml.etree.ElementTree as ET
import pandas as pd
import time
import config
import openpyxl
from datetime import datetime, timedelta


<<<<<<< HEAD
    return f"{year}{month:02d}01T00:00-0000", f"{year}{month:02d}{end_day}T23:59-0000"
=======
def get_rolling_dates(start_date, end_date, window_size=20):
    """
    Generates non-overlapping start and end dates with a fixed window size.
>>>>>>> d256f4a (Updated data_fetch function for rolling window)

    Parameters:
    - start_date: The beginning date for the data request (YYYY-MM-DD format)
    - end_date: The last date for the data request (YYYY-MM-DD format)
    - window_size: The number of days in each rolling window

    Returns:
    - List of tuples containing (start_time, end_time)
    """
    start_date = datetime.strptime(start_date, "%Y-%m-%d")
    end_date = datetime.strptime(end_date, "%Y-%m-%d")

    rolling_dates = []
    current_start = start_date

    while current_start <= end_date:
        current_end = current_start + timedelta(days=window_size - 1)
        if current_end > end_date:
            current_end = end_date  # Adjust the last window to not exceed end_date

        rolling_dates.append((current_start.strftime("%Y%m%dT00:00-0000"),
                              current_end.strftime("%Y%m%dT23:59-0000")))

        current_start += timedelta(days=window_size)  # Move forward by window size (no overlap)

    return rolling_dates

def fetch_data(start_time, end_time, queryname, mkt_run_id):
    """Fetches SLD_FCST data for a given time period."""
    params = {
        "queryname": queryname,       #"SLD_FCST"
        "startdatetime": start_time,
        "enddatetime": end_time,
        "market_run_id": mkt_run_id,  # "ACTUAL"
        "version": "1"
    }

    response = requests.get(config.BASE_URL, params=params, stream=True)

    # Retry if rate limited
    while response.status_code == 429:
        time.sleep(10)
        response = requests.get(config.BASE_URL, params=params, stream=True)

    if response.status_code == 200:
        print(f"Data fetched for {start_time} - {end_time}")

        with zipfile.ZipFile(io.BytesIO(response.content)) as z:
            file_list = z.namelist()
            xml_filename = [f for f in file_list if f.endswith(".xml")][0]

            with z.open(xml_filename) as xml_file:
                xml_content = xml_file.read().decode("utf-8")

        return xml_content
    else:
        print(f"Error {response.status_code}: {response.text}")
        return None


def parse_xml_data(xml_content):
    """Extracts and structures the XML data into a DataFrame."""
    root = ET.fromstring(xml_content)
    namespace = root.tag.split("}")[0].strip("{")
    ns = {"ns": namespace}

    columns = set()
    for report in root.findall(".//ns:REPORT_DATA", ns):
        for child in report:
            columns.add(child.tag.split("}")[1])

    columns = sorted(columns)

    data = []
    for report in root.findall(".//ns:REPORT_DATA", ns):
        row = {col: report.find(f"ns:{col}", ns).text if report.find(f"ns:{col}", ns) is not None else None for col in
               columns}
        data.append(row)

    return pd.DataFrame(data)


def data_fetch(start_date, end_date, window_size = 20):
    rolling_windows = get_rolling_dates(start_date, end_date, window_size)

    df_load = pd.DataFrame()
    for start_time, end_time in rolling_windows:
        xml_content = fetch_data(start_time, end_time, queryname=config.queryname_load, mkt_run_id=config.mkt_run_id_load)

        if xml_content:
            df_window_load = parse_xml_data(xml_content)
            print(df_window_load.head())
            df_load = pd.concat([df_load, df_window_load], ignore_index=True)
    df_load.to_excel(config.DATA_FILE_PATH_LOAD)

    df_gen = pd.DataFrame()
    for start_time, end_time in rolling_windows:
        xml_content = fetch_data(start_time, end_time, queryname=config.queryname_gen, mkt_run_id=config.mkt_run_id_gen)

        if xml_content:
            df_window_gen = parse_xml_data(xml_content)
            df_gen = pd.concat([df_gen, df_window_gen], ignore_index=True)
    df_gen.to_excel(config.DATA_FILE_PATH_GEN)

    df_trans= pd.DataFrame()
    for start_time, end_time in rolling_windows:
        xml_content = fetch_data(start_time, end_time, queryname=config.queryname_trans, mkt_run_id=config.mkt_run_id_trans)

        if xml_content:
            df_window_trans = parse_xml_data(xml_content)
            df_window_trans = df_window_trans[df_window_trans['DATA_ITEM'].isin(['ISO_TOT_EXP_MW', 'ISO_TOT_EXP_MW'])]
            df_window_trans = df_window_trans[df_window_trans['RESOURCE_NAME'] == 'Caiso_Totals']
            df_trans = pd.concat([df_trans, df_window_trans], ignore_index=True)
    df_trans.to_excel(config.DATA_FILE_PATH_TRANS)

    print("Data fetching and saving completed for df_load, df_gen, and df_trans.")



