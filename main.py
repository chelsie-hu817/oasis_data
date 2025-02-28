# main.py

import data_fetch
import data_process


def main():
    start_date = "2019-01-01"
    end_date = "2019-03-02"

    print("Fetching data from OASIS...")
    data_fetch.data_fetch(start_date, end_date)

    print("Processing data...")
    df_merged = data_process.process_data()

if __name__ == "__main__":
    main()
