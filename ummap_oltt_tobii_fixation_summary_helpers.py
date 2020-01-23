# helpers.py

import pandas as pd
import numpy as np
import io
import re

from boxsdk import JWTAuth, Client
import os.path


# Regex pattern for target media names, e.g., `LivingRm_Tissues.jpg`
ptrn_envimg = re.compile(r'^\w+_\w+\.jpg$')

# Columns of interest from Excel files
cols_keep = ['MediaName',
             'RecordingTimestamp',
             'LocalTimeStamp',
             'GazeEventType',
             'GazeEventDuration']

cols_idx = ['MediaName', 'Index']

cols_data = ['RecordingTimestamp',
             'LocalTimeStamp',
             'GazeEventType',
             'GazeEventDuration']


## Processing Functions for "Fix Only" and "All Gaze" Data ##

def first_LocalTimeStamp(df):
    return df['LocalTimeStamp'].values[0]


def last_LocalTimeStamp(df):
    return df['LocalTimeStamp'].values[-1]


def filter_out_times(df, min, max):
    return df.loc[((df['LocalTimeStamp'] > min) &
                   (df['LocalTimeStamp'] < max)), :]


def filter_time_gaz(df_gg, df_f):
    media_name = df_gg.index.unique(level=0)[0]

    if media_name in df_f.index.unique(level=0):
        df_fix_sub = df_f.loc[media_name]

        if first_GazeEventType_is_Fixation(df_fix_sub):
            min_time_fix = first_LocalTimeStamp(df_fix_sub)
            max_time_fix = last_LocalTimeStamp(df_fix_sub)
            return filter_out_times(df_gg, min=min_time_fix, max=max_time_fix)
        else:
            return df_gg


def first_GazeEventType(df):
    return df['GazeEventType'].values[0]


def last_GazeEventType(df):
    return df['GazeEventType'].values[-1]


def first_GazeEventType_is_Fixation(df):
    return first_GazeEventType(df) == "Fixation"


def last_GazeEventType_is_Fixation(df):
    return last_GazeEventType(df) == "Fixation"


def first_nonFixation(pds):
    for idx, val in enumerate(pds):
        if val != "Fixation":
            return idx
    return np.nan


def top_GazeEventType_Fixations(df):
    pds_GazeEventType = df['GazeEventType']

    idx_nonFixation = first_nonFixation(pds_GazeEventType)

    if not np.isnan(idx_nonFixation):
        return df.iloc[0:idx_nonFixation, :]
    else:
        return df.iloc[0:0, :]


def bot_GazeEventType_Fixations(df):
    pds_GazeEventType = df['GazeEventType']
    pds_len = len(pds_GazeEventType)
    pds_GazeEventType_rev = pds_GazeEventType.to_numpy()[::-1]

    idx_nonFixation_rev = first_nonFixation(pds_GazeEventType_rev)
    idx_nonFixation = pds_len - idx_nonFixation_rev

    if not np.isnan(idx_nonFixation):
        return df.iloc[idx_nonFixation:, :]
    else:
        return df.iloc[0:0, :]


def calc_ms_diff_top_fixations(df):
    if first_GazeEventType_is_Fixation(df):
        df_gaz_top_fixations = top_GazeEventType_Fixations(df)
        t1 = df_gaz_top_fixations.loc[df_gaz_top_fixations.index[0],
                                      'RecordingTimestamp']
        t2 = df_gaz_top_fixations.loc[df_gaz_top_fixations.index[-1],
                                      'RecordingTimestamp']
        return t2 - t1
    else:
        return np.nan


def calc_ms_diff_bot_fixations(df):
    if last_GazeEventType_is_Fixation(df):
        df_gaz_bot_fixations = bot_GazeEventType_Fixations(df)
        t1 = df_gaz_bot_fixations.loc[df_gaz_bot_fixations.index[0],
                                      'RecordingTimestamp']
        t2 = df_gaz_bot_fixations.loc[df_gaz_bot_fixations.index[-1],
                                      'RecordingTimestamp']
        return t2 - t1
    else:
        return np.nan


def get_GEDs(df):
    return df['GazeEventDuration'].reset_index(level=(0, 1), drop=True)


def replace_GED_if_needed_top(x, s_g):
    media_name = x.index.unique(level=0)[0]
    if media_name in s_g.index and s_g.loc[media_name] >= 60:
        x[0] = s_g.loc[media_name]
    return x


def replace_GED_if_needed_bot(x, s_g):
    media_name = x.index.unique(level=0)[0]
    if media_name in s_g.index and s_g.loc[media_name] >= 60:
        x[-1] = s_g.loc[media_name]
    return x


# https://pandas.pydata.org/pandas-docs/stable/user_guide/io.html?highlight=excelfile#writing-excel-files-to-memory
def df_to_excel_buffer(df):
    # Instantiate a bytes buffer stream
    bio = io.BytesIO()
    # Instantiate and set the engine of ExcelWriter constructor
    writer = pd.ExcelWriter(bio, engine="xlsxwriter")
    # Write the DataFrame as an Excel file to the ExcelWriter object
    df.to_excel(writer, sheet_name='Sheet1')
    # Save the ExcelWriter object contents to the bytes buffer stream
    writer.save()

    return bio


# Fix + Gaze: Get raw DataFrame using Box auth'd client and file ID
def excel_file_id_to_df_raw(client, file_id):
    # Get file contents with auth'd Box client
    file_content = client.file(file_id).content()
    # Read file contents stream as DataFrame
    df_raw = pd.read_excel(io.BytesIO(file_content),
                           sheet_name='Data', usecols=cols_keep, parse_dates=True, engine="xlrd",
                           converters={'MediaName': str, 'RecordingTimestamp': str, 'GazeEventType': str,
                                       'GazeEventDuration': str})
    # Coerce columns to appropriate dtypes
    # df_raw['RecordingTimestamp'] = pd.Series(df_raw['RecordingTimestamp'], dtype='Int64')
    # df_raw['GazeEventDuration'] = pd.Series(df_raw['GazeEventDuration'], dtype='Int64')
    df_raw['RecordingTimestamp'] = pd.to_numeric(df_raw['RecordingTimestamp'], errors='coerce')
    df_raw['GazeEventDuration'] = pd.to_numeric(df_raw['GazeEventDuration'], errors='coerce')
    df_raw['LocalTimeStamp'] = pd.to_datetime(df_raw['LocalTimeStamp'], errors='coerce').dt.time

    return df_raw


# Fix + Gaze: Get Target `MediaName`s
def df_raw_to_s_media_envimg(df_raw):
    s_media = pd.Series(df_raw['MediaName'].unique())
    s_media = s_media[s_media.notnull()].reset_index(drop=True)
    s_media_envimg = s_media[s_media.str.match(ptrn_envimg)].reset_index(drop=True)

    return s_media_envimg


# Fix + Gaze: Filter Data to Include Target `MediaName`s
def df_raw_to_df_mn(df_raw, s_media_envimg):
    df_mn = df_raw.loc[df_raw['MediaName'].isin(s_media_envimg), :].reset_index(drop=True)

    return df_mn


def df_mn_to_df(df_mn):
    df_idx = df_mn.copy()
    df_idx['Index'] = df_idx.index
    idx = pd.MultiIndex.from_frame(df_idx[cols_idx])
    df = df_idx[cols_data].set_index(keys=idx, drop=True, append=False)

    return df


def df_from_excel_file_id(client, file_id):
    df_raw = excel_file_id_to_df_raw(client, file_id)
    s_media_envimg = df_raw_to_s_media_envimg(df_raw)
    df_mn = df_raw_to_df_mn(df_raw, s_media_envimg)
    df = df_mn_to_df(df_mn)

    return df


def df_and_s_media_from_excel_file_id(client, file_id):
    """Reads and pre-processes Excel file, returning preprocessed DataFrame and Series of media names from
    Excel file. Additionally returning the media names from the "Fix Only" file is necessary for filtering
    and ordering the ultimately processed DataFrame.

    Arguments:
        client {Box Client} -- Authenticated Box Client object
        file_id {str} -- String of Excel file to process

    Returns:
        df {pandas DataFrame} -- Preprocessed DataFrame from the Excel file
        s_media_envimg {pandas Series} -- Series of media names from the Excel file
    """
    df_raw = excel_file_id_to_df_raw(client, file_id)
    s_media_envimg = df_raw_to_s_media_envimg(df_raw)
    df_mn = df_raw_to_df_mn(df_raw, s_media_envimg)
    df = df_mn_to_df(df_mn)

    return df, s_media_envimg


def s_fix_adjusted_GEDs_from_dfs(df_gaz, df_fix):
    """Gaze + Fix: Using Fix min/max times, filter out min/max times from Gaze

    Arguments:
        df_gaz {pandas DataFrame} -- Foo bar baz qux
        df_fix {pandas DataFrame} -- Foo bar baz qux

    Returns:
        s_fix_GEDs_top_bot {pandas Series} -- Foo bar baz qux
    """
    # Using Fix min/max times, filter out min/max times from Gaze
    df_gaz_flt = df_gaz.groupby('MediaName').apply(filter_time_gaz, df_f=df_fix).reset_index(level=0, drop=True). \
        sort_index(level=1)

    # GazeFiltered: Calculate Fixation ms diff @ head and tail from GazeFiltered
    s_gaz_diffs_ms_top = df_gaz_flt.groupby('MediaName').apply(calc_ms_diff_top_fixations)
    s_gaz_diffs_ms_bot = df_gaz_flt.groupby('MediaName').apply(calc_ms_diff_bot_fixations)

    # Fix: Capture vectors of `GazeEventDuration`s from Fix
    s_fix_GEDs = df_fix.groupby('MediaName').apply(get_GEDs)

    # DiffsMSTop + GEDs: If DiffMSTop >= 60 then GEDs[0] <- DiffMSTop
    s_fix_GEDs_top = s_fix_GEDs.groupby('MediaName').apply(replace_GED_if_needed_top, s_g=s_gaz_diffs_ms_top)

    # DiffsMSBot + FixGEDs: If DiffMSBot >= 60 then GEDs[-1] <- DiffMSBot
    s_fix_GEDs_top_bot = s_fix_GEDs_top.groupby('MediaName').apply(replace_GED_if_needed_bot, s_g=s_gaz_diffs_ms_bot)

    return s_fix_GEDs_top_bot


def df_adjusted_GEDs_counts_and_means_from_s_fix_GEDs_adjusted(s_fix_GEDs_adjusted, s_media_envimg_fix):
    """GEDsAdj: Calculate counts and means of adjusted GazeEventDurations

    Arguments:
        s_fix_GEDs_adjusted {pandas Series} -- Foo bar baz qux
        s_media_envimg_fix {pandas Series} -- Foo bar baz qux

    Returns:
        df_counts_means {pandas DataFrame} -- Foo bar baz qux
    """
    s_fix_GEDs_adjusted_count = s_fix_GEDs_adjusted.groupby('MediaName').count()[s_media_envimg_fix]
    s_fix_GEDs_adjusted_mean = s_fix_GEDs_adjusted.groupby('MediaName').mean()[s_media_envimg_fix]

    # Create DataFrame from Counts Series and Means Series
    s_fix_GEDs_adjusted_count.name = 'GazeEventDurationCount'
    s_fix_GEDs_adjusted_count.index.name = 'MediaName'
    s_fix_GEDs_adjusted_mean.name = 'GazeEventDurationMean'
    s_fix_GEDs_adjusted_mean.index.name = 'MediaName'

    df_counts_means = pd.concat([s_fix_GEDs_adjusted_count, s_fix_GEDs_adjusted_mean], axis='columns')

    return df_counts_means


## Box Client Functions ##

def get_authenticated_client(configPath):
    """Get an authenticated Box client for a JWT service account

    Arguments:
        configPath {str} -- Path to the JSON config file for your Box JWT app

    Returns:
        Client -- A Box client for the JWT service account

    Raises:
        ValueError -- if the configPath is empty or cannot be found.
    """
    if (os.path.isfile(configPath) == False):
        raise ValueError(f"configPath must be a path to the JSON config file for your Box JWT app")
    auth = JWTAuth.from_settings_file(configPath)
    print("Authenticating...")
    auth.authenticate_instance()
    return Client(auth)


def get_subitems(client, folder, fields=["id", "name", "path_collection", "size"]):
    """Get a collection of all immediate folder items

    Arguments:
        client {Client} -- An authenticated Box client
        folder {Folder} -- The Box folder whose contents we want to fetch

    Keyword Arguments:
        fields {list} -- An optional list of fields to include with each item (default: {["id","name","path_collection"]})

    Returns:
        list -- A collection of Box files and folders.
    """
    items = []
    # fetch folder items and add subfolders to list
    for item in client.folder(folder_id=folder['id']).get_items(fields=fields):
        items.append(item)
    return items


def print_user_info(client):
    """Print the name and login of the current authenticated Box user

    Arguments:
        client {Client} -- An authenticated Box client
    """
    user = client.user('me').get()
    print("")
    print("Authenticated User")
    print(f"Name: {user.name}")
    print(f"Login: {user.login}")


## Driver Function ##

def walk_dir_tree_process_fix_gaz(client, folder, ptrn_fix, ptrn_gaz, ptrn_fix_summ):
    """Count the number of files matching the regex pattern X

    Arguments:
        client {Client} -- An authenticated Box client
        folder {Folder} -- The Box folder whose contents we want to fetch
        ptrn_fix {RegexObject} -- Regex pattern for "Fixation Only" file
        ptrn_gaz {RegexObject} -- Regex pattern for "All Gaze" file
        ptrn_fix_summ {RegexObject} -- Regex pattern for "fixation summary" file created by this script
    """

    # Local variables for function control flow
    bool_fix_exists, bool_gaz_exists, bool_fix_summ_exists = False, False, False
    file_id_fix, file_id_gaz = "", ""

    # Get list of items in current folder
    subitems = get_subitems(client, folder)

    # Recurse down with this function into every subfolder
    for subfolder in filter(lambda i: i.type == "folder", subitems):
        walk_dir_tree_process_fix_gaz(client, subfolder, ptrn_fix, ptrn_gaz, ptrn_fix_summ)

    # Set file existence flags necessary for carrying data processing work
    for file in filter(lambda i: i.type == "file", subitems):
        if re.match(ptrn_fix, file.name):
            file_id_fix = file.id
            bool_fix_exists = True
        if re.match(ptrn_gaz, file.name):
            file_id_gaz = file.id
            bool_gaz_exists = True
        if re.match(ptrn_fix_summ, file.name):
            bool_fix_summ_exists = True

    # If "Fix Only" and "All Gaze" files exist, but generated "Fixation Summary" file does not,
    # then process the "Fix Only" and "All Gaze" files to generate the "Fixation Summary" file
    if bool_fix_exists and bool_gaz_exists and not bool_fix_summ_exists:
        print(folder.name, folder.id)
        # print(folder.name, folder.id, ":", file_id_fix, ";", file_id_gaz)

        # Read FixOnly and AllGaze Excel files as DataFrames
        (df_fix, s_media_envimg_fix) = df_and_s_media_from_excel_file_id(client, file_id_fix)
        df_gaz = df_from_excel_file_id(client, file_id_gaz)

        # Adjust FixOnly `GazeEventDuration`s for bleed-over
        s_fix_GEDs_adjusted = s_fix_adjusted_GEDs_from_dfs(df_gaz, df_fix)

        # Generate DataFrame of adjusted `GazeEventDuration` counts and means
        df_adjusted_GEDs_counts_and_means = \
            df_adjusted_GEDs_counts_and_means_from_s_fix_GEDs_adjusted(s_fix_GEDs_adjusted, s_media_envimg_fix)

        # Write DataFrame to Excel file buffer
        bio = df_to_excel_buffer(df_adjusted_GEDs_counts_and_means)

        # Upload the bytes io stream as Excel to folder
        folder.upload_stream(bio, folder.name + "_fixation_summary.xlsx")
