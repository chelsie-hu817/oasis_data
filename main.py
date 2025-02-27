# main.py

import data_fetch
import data_process


def main():
    year_start = 2023
    year_end = 2024

    print("Fetching data from OASIS...")
    data_fetch.data_fetch(year_start, year_end)

    print("Processing data...")
    df_merged = data_process.process_data()

if __name__ == "__main__":
    main()
