import openf1_get as g
import openf1_file_helpers as fh
import pandas as pd

def fp_short_run_analysis(session_key):
    """Produce an analysis of short runs in free practice"""
    def get_and_sort_laps(session_key_):
        query = ("laps", {'session_key': session_key_})
        df = g.get(query[0], query[1])
        df.drop(['meeting_key', 'session_key', 'date_start', 'segments_sector_1', 'segments_sector_2', 'segments_sector_3'], axis=1, inplace=True)
        df.sort_values(by=['driver_number', 'lap_number'], inplace=True)

        return df

    def get_and_sort_stints(session_key_):
        query = ("stints", {'session_key': session_key_})
        df = g.get(query[0], query[1])
        df.drop(['meeting_key', 'session_key'], axis=1, inplace=True)
        df.sort_values(by=['driver_number'], inplace=True)

        return df

    def combine_laps_and_stints(df_laps_, df_stints_):
        # Make a common key to join the two dataframes on
        factor = max(df_laps_['lap_number'].max(), df_stints_['lap_end'].max()) + 10
        df_laps_["_key"] = df_laps_['driver_number'] * factor + df_laps_['lap_number']
        df_stints_["_key"] = df_stints_['driver_number'] * factor + df_stints_['lap_start']
        df_laps_ = df_laps_.sort_values('_key')
        df_stints_ = df_stints_.sort_values('_key')

        # Merge the dataframes on the common key, and then drop the now irrelevant "_key" column
        df = pd.merge_asof(
            df_laps_, df_stints_,
            left_on="_key", right_on="_key",
            direction="backward", allow_exact_matches=True
        ).drop(columns=["_key"])

        # Ensure the merge has worked correctly
        in_range = df['lap_number'].ge(df['lap_start']) & df['lap_number'].le(df['lap_end'])
        stint_cols = [c for c in df_stints_.columns if c not in ('driver_number', "_key")]
        df.loc[~in_range, stint_cols] = pd.NA

        return df

    def select_qualifying_runs(df_combi_):
        round_to = 2

        # Selection Variables
        max_time_gap = 2 # 2%
        max_speed_delta = -2.5 # -2.5%
        max_sector_gap = 2 # 2%
        min_fast_sectors = 3

        # Filter unnecessary columns and rows with missing data
        df = df_combi_[df_combi_["is_pit_out_lap"] != True]
        df = df.drop(['is_pit_out_lap', 'driver_number_y','lap_start','lap_end'], axis=1)
        df = df.dropna()

        # Force these columns to float, in case they are accidentally read as strings
        float_columns = [
            'duration_sector_1',
            'duration_sector_2',
            'duration_sector_3',
            'i1_speed',
            'i2_speed',
            'lap_duration',
            'st_speed'
        ]
        float_columns = [column for column in float_columns if column in df.columns]
        for column in float_columns:
            df[column] = pd.to_numeric(df[column], errors="coerce").astype("float64")

        # Give each lap a pace score relative to the stint
        df['best_lap_in_group'] = df.groupby('driver_number_x')['lap_duration'].transform('min')
        df['pct_gap_to_best_lap'] = round(
            ((df["lap_duration"] / df["best_lap_in_group"]) - 1.0) * 100.0
            , round_to)
        df.drop('best_lap_in_group', axis=1, inplace=True)

        # Sector pace scores per stint
        groupby_columns = ['driver_number_x', 'stint_number']
        sector_columns = ['duration_sector_1', 'duration_sector_2', 'duration_sector_3']
        best_sectors = df.groupby(groupby_columns)[sector_columns].transform('min')
        for sector_column in sector_columns:
            df[f"{sector_column}_pct_gap_to_best"] = round(
                ((df[sector_column] / best_sectors[sector_column]) - 1.0) * 100.0
                , round_to)
        sector_gap_columns = [f"{sector_column}_pct_gap_to_best" for sector_column in sector_columns]
        df['fast_sectors'] = (df[sector_gap_columns] <= max_sector_gap).sum(axis=1)
        df['worst_sector_gap'] = df[sector_gap_columns].max(axis=1)

        # Speed scores per stint
        speed_columns = ['i1_speed', 'i2_speed', 'st_speed']
        best_speeds = df.groupby('driver_number_x')[speed_columns].transform('max')
        for speed_column in speed_columns:
            df[f"{speed_column}_delta_to_best"] = round(
                ((df[speed_column] / best_speeds[speed_column]) - 1.0) * 100.0
                , round_to)

        # Reorder columns for readability
        column_order = [
            'driver_number_x',
            'stint_number',
            'compound',
            'tyre_age_at_start',
            'lap_number',
            'lap_duration',
            'pct_gap_to_best_lap',
            'duration_sector_1',
            'duration_sector_1_pct_gap_to_best',
            'duration_sector_2',
            'duration_sector_2_pct_gap_to_best',
            'duration_sector_3',
            'duration_sector_3_pct_gap_to_best',
            'fast_sectors',
            'worst_sector_gap',
            'i1_speed',
            'i1_speed_delta_to_best',
            'i2_speed',
            'i2_speed_delta_to_best',
            'st_speed',
            'st_speed_delta_to_best'
        ]
        df = df.loc[:, column_order]

        # Filter according to our conditions
        df = df[df['pct_gap_to_best_lap'] < max_time_gap]
        df = df[df['fast_sectors'] == min_fast_sectors]
        df = df[df['st_speed_delta_to_best'] > max_speed_delta]

        # Drop irrelevant columns
        df = df.drop([
            'i1_speed',
            'i1_speed_delta_to_best',
            'i2_speed',
            'i2_speed_delta_to_best',
            'lap_number',
            'pct_gap_to_best_lap',
            'fast_sectors',
            'duration_sector_1_pct_gap_to_best',
            'duration_sector_2_pct_gap_to_best',
            'duration_sector_3_pct_gap_to_best',
            'worst_sector_gap',
            'st_speed_delta_to_best',
            'tyre_age_at_start'
        ], axis=1)

        df['gap_to_leader'] = round(df['lap_duration'] - df['lap_duration'].min(), round_to)
        df['sector_1_gap_to_leader'] = round(df['duration_sector_1'] - df['duration_sector_1'].min(), round_to)
        df['sector_2_gap_to_leader'] = round(df['duration_sector_2'] - df['duration_sector_2'].min(), round_to)
        df['sector_3_gap_to_leader'] = round(df['duration_sector_3'] - df['duration_sector_3'].min(), round_to)
        df['st_delta_to_leader'] = round(df['st_speed'].max() - df['st_speed'], round_to)

        fh.cache_response(df, 'test', 'laps_and_stints_sort')

        return df

    df_laps = get_and_sort_laps(session_key)
    df_stints = get_and_sort_stints(session_key)
    df_combi = combine_laps_and_stints(df_laps, df_stints)
    df_qualifying_runs = select_qualifying_runs(df_combi)

def fp_long_run_analysis():
    """Produce an analysis of long runs in free practice"""
    pass


def teammate_comparison():
    """Produce an analysis of the comparison between teammates in a season"""
    SEASON_2025 = {'2025-02-25', '2025-12-10'}