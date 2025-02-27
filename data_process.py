# data_process.py

import pandas as pd
import config
from functools import reduce
import pytz

def process_data():
    """Load, clean, and process the dataset."""
    #--------------  Load --------------
    df_load = pd.read_excel(config.DATA_FILE_PATH_LOAD)
    df_load = df_load[df_load['RESOURCE_NAME'] == 'CA ISO-TAC']
    df_load = df_load.rename(columns={'VALUE': 'LOAD'})
    df_load['LOAD'] = pd.to_numeric(df_load['LOAD'])

    #-------------- Generation -------------
    df_gen = pd.read_excel(config.DATA_FILE_PATH_GEN)
    df_gen_np15 = df_gen[df_gen['TRADING_HUB'] == 'NP15']
    df_gen_sp15 = df_gen[df_gen['TRADING_HUB'] == 'SP15']
    df_gen_zp26 = df_gen[df_gen['TRADING_HUB'] == 'ZP26']

    df_solar_np15 = df_gen_np15[df_gen_np15['RENEWABLE_TYPE'] == 'Solar'].rename(columns={'VALUE': 'SOLAR_NP15'})
    df_solar_sp15 = df_gen_sp15[df_gen_sp15['RENEWABLE_TYPE'] == 'Solar'].rename(columns={'VALUE': 'SOLAR_SP15'})
    df_solar_zp26 = df_gen_zp26[df_gen_zp26['RENEWABLE_TYPE'] == 'Solar'].rename(columns={'VALUE': 'SOLAR_ZP26'})

    df_wind_np15 = df_gen_np15[df_gen_np15['RENEWABLE_TYPE'] == 'Wind'].rename(columns={'VALUE': 'WIND_NP15'})
    df_wind_sp15 = df_gen_sp15[df_gen_sp15['RENEWABLE_TYPE'] == 'Wind'].rename(columns={'VALUE': 'WIND_SP15'})
    df_wind_zp26 = df_gen_zp26[df_gen_zp26['RENEWABLE_TYPE'] == 'Wind'].rename(columns={'VALUE': 'WIND_ZP26'})

    df_solar_np15['SOLAR_NP15'] = pd.to_numeric(df_solar_np15['SOLAR_NP15'])
    df_solar_sp15['SOLAR_SP15'] = pd.to_numeric(df_solar_sp15['SOLAR_SP15'])
    df_solar_zp26['SOLAR_ZP26'] = pd.to_numeric(df_solar_zp26['SOLAR_ZP26'])

    df_wind_np15['WIND_NP15'] = pd.to_numeric(df_wind_np15['WIND_NP15'])
    df_wind_sp15['WIND_SP15'] = pd.to_numeric(df_wind_sp15['WIND_SP15'])
    df_wind_zp26['WIND_zp26'] = pd.to_numeric(df_wind_zp26['WIND_ZP26'])

    #-------------- Transmission -------------
    df_trans = pd.read_excel(config.DATA_FILE_PATH_TRANS)
    df_trans['VALUE'] = pd.to_numeric(df_trans['VALUE'])
    df_exp = df_trans[df_trans['DATA_ITEM'] == 'ISO_TOT_EXP_MW']
    df_imp = df_trans[df_trans['DATA_ITEM'] == 'ISO_TOT_IMP_MW']

    # imp and exp are in 5 min range --- convert to hourly
    # imports
    df_imp['INTERVAL_END_GMT'] = pd.to_datetime(df_imp['INTERVAL_END_GMT'])
    df_imp['HOUR'] = df_imp['INTERVAL_END_GMT'].dt.floor('H')  # Floor to start of the hour
    df_imp_hourly = df_imp.groupby('HOUR', as_index=False)['VALUE'].mean()
    df_imp_hourly.rename(columns={'HOUR': 'INTERVAL_END_GMT', 'VALUE': 'IMP_HOURLY_AVG'}, inplace=True)

    # exports
    df_exp['INTERVAL_END_GMT'] = pd.to_datetime(df_exp['INTERVAL_END_GMT'])
    df_exp['HOUR'] = df_exp['INTERVAL_END_GMT'].dt.floor('H')  # Floor to start of the hour
    df_exp_hourly = df_exp.groupby('HOUR', as_index=False)['VALUE'].mean()
    df_exp_hourly.rename(columns={'HOUR': 'INTERVAL_END_GMT', 'VALUE': 'EXP_HOURLY_AVG'}, inplace=True)

    dfs_solar = [df_solar_np15, df_solar_sp15, df_solar_zp26]
    df_solar_caiso = reduce(lambda left, right: pd.merge(left, right,
                                                         on=['DATA_ITEM', 'INTERVAL_END_GMT', 'INTERVAL_NUM',
                                                             'INTERVAL_START_GMT', 'OPR_DATE', 'RENEWABLE_TYPE'],
                                                         how='outer'), dfs_solar)

    dfs_wind = [df_wind_np15, df_wind_sp15, df_wind_zp26]
    df_wind_caiso = reduce(lambda left, right: pd.merge(left, right,
                                                        on=['DATA_ITEM', 'INTERVAL_END_GMT', 'INTERVAL_NUM',
                                                            'INTERVAL_START_GMT', 'OPR_DATE', 'RENEWABLE_TYPE'],
                                                        how='outer'), dfs_wind)

    # ------------- Merge All ---------------------
    df_wind_caiso['INTERVAL_END_GMT'] = pd.to_datetime(df_wind_caiso['INTERVAL_END_GMT'])
    df_merged = pd.merge(df_imp_hourly, df_wind_caiso[['INTERVAL_END_GMT', 'WIND_SP15', 'WIND_NP15', 'WIND_ZP26']],
                         on='INTERVAL_END_GMT', how='outer')

    df_solar_caiso['INTERVAL_END_GMT'] = pd.to_datetime(df_solar_caiso['INTERVAL_END_GMT'])
    df_merged = pd.merge(df_merged, df_solar_caiso[['INTERVAL_END_GMT', 'SOLAR_SP15', 'SOLAR_NP15', 'SOLAR_ZP26']],
                         on='INTERVAL_END_GMT', how='outer')
    df_merged = pd.merge(df_merged, df_exp_hourly, on='INTERVAL_END_GMT', how='outer')
    df_load['INTERVAL_END_GMT'] = pd.to_datetime(df_load['INTERVAL_END_GMT'])
    df_merged = pd.merge(df_merged, df_load[['INTERVAL_END_GMT', 'LOAD']], on='INTERVAL_END_GMT', how='outer')

    # Timezone
    gmt_tz = pytz.timezone("GMT")  # GMT timezone
    pst_tz = pytz.timezone("US/Pacific")  # PST timezone

    # df_merged['INTERVAL_END_GMT'] = df_merged['INTERVAL_END_GMT'].dt.tz_localize(gmt_tz)  # Mark as GMT
    df_merged['INTERVAL_END_PST'] = df_merged['INTERVAL_END_GMT'].dt.tz_convert(pst_tz)  # Convert to PST
    df_merged['INTERVAL_END_GMT'] = df_merged['INTERVAL_END_GMT'].dt.tz_localize(None)
    df_merged['INTERVAL_END_PST'] = df_merged['INTERVAL_END_PST'].dt.tz_localize(None)

    df_merged.to_excel(config.PROCESSED_FILE_PATH, index=False)
    print(f"Processed data saved to {config.PROCESSED_FILE_PATH}")

    return df_merged
