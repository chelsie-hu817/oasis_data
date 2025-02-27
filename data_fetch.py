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

def get_available_dates(year, month):
    """Estimate the best start and end dates for a month based on typical publication delays."""
    if month == 2:  # February case, account for leap years
        end_day = 29 if year % 4 == 0 else 28
    elif month in {4, 6, 9, 11}:  # Months with 30 days
        end_day = 30
    else:
        end_day = 31

    return f"{year}{month:02d}03T00:00-0000", f"{year}{month:02d}{end_day - 2}T23:59-0000"


def fetch_monthly_data(start_time, end_time, queryname, mkt_run_id):
    """Fetches SLD_FCST data for a given time period."""
    params = {
        "queryname": queryname,  # "SLD_FCST"
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


def data_fetch(year_start, year_end):
    df_gen = pd.DataFrame()
    for year in range(year_start, year_end):
        for month in range(1, 13):
            start_time, end_time = get_available_dates(year, month)
            xml_content = fetch_monthly_data(start_time, end_time, config.queryname_gen, config.mkt_run_id_gen)

            if xml_content:
                df_month = parse_xml_data(xml_content)
                #print(df_month.head())
                df_gen = pd.concat([df_gen, df_month], ignore_index=True)

    df_gen.to_excel(config.DATA_FILE_PATH_GEN)
    print('Gen saved')

    df_load = pd.DataFrame()
    for year in range(year_start, year_end):
         for month in range(1, 13):
             start_time, end_time = get_available_dates(year, month)
             xml_content = fetch_monthly_data(start_time, end_time, config.queryname_load, config.mkt_run_id_load)

             if xml_content:
                 df_month = parse_xml_data(xml_content)
                 df_load = pd.concat([df_load, df_month], ignore_index=True)

    df_load.to_excel(config.DATA_FILE_PATH_LOAD)
    print('Load saved')


    df_trans = pd.DataFrame()
    for year in range(year_start, year_end):
        for month in range(1, 13):
            start_time, end_time = get_available_dates(year, month)
            xml_content = fetch_monthly_data(start_time, end_time, config.queryname_trans, config.mkt_run_id_trans)

            if xml_content:
                df_month = parse_xml_data(xml_content)
                df_month = df_month[df_month['DATA_ITEM'].isin(['ISO_TOT_EXP_MW', 'ISO_TOT_IMP_MW'])]
                df_month = df_month[df_month['RESOURCE_NAME'] == 'Caiso_Totals']
                df_trans = pd.concat([df_trans, df_month], ignore_index=True)

    df_trans.to_excel(config.DATA_FILE_PATH_TRANS)
    print('Trans saved')






