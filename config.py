# config.py

# url
BASE_URL = "http://oasis.caiso.com/oasisapi/SingleZip"

# data paths
DATA_FILE_PATH_LOAD = "data/raw_data_load.xlsx"  # Path to store fetched data
DATA_FILE_PATH_GEN = "data/raw_data_gen.xlsx"  # Path to store fetched data
DATA_FILE_PATH_TRANS = "data/raw_data_trans.xlsx"  # Path to store fetched data


PROCESSED_FILE_PATH = "data/oasis_load.xlsx"  # Path to store processed data

# func parameters
queryname_load = "SLD_FSCT"
mkt_run_id_load = "ACTUAL"

queryname_gen = "SLD_REN_FSCT"
mkt_run_id_gen = "ACTUAL"

queryname_trans = "ENE_SLRS"
mkt_run_id_trans = "RTM"
