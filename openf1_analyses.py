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
        fh.cache_response(df, 'test', 'laps_sort')

        return df

    def get_and_sort_stints(session_key_):
        query = ("stints", {'session_key': session_key_})
        df = g.get(query[0], query[1])
        df.drop(['meeting_key', 'session_key'], axis=1, inplace=True)
        df.sort_values(by=['driver_number'], inplace=True)
        fh.cache_response(df, 'test', 'stints_sort')

        return df

    def combine_laps_and_stints(df_laps_, df_stints_):
        factor = max(df_laps_['lap_number'].max(), df_stints_['lap_end'].max()) + 10
        df_laps_["_key"] = df_laps_['driver_number'] * factor + df_laps_['lap_number']
        df_stints_["_key"] = df_stints_['driver_number'] * factor + df_stints_['lap_start']

        df_laps_ = df_laps_.sort_values('_key')
        df_stints_ = df_stints_.sort_values('_key')

        df = pd.merge_asof(
            df_laps_, df_stints_,
            left_on="_key", right_on="_key",
            direction="backward", allow_exact_matches=True
        ).drop(columns=["_key"])

        in_range = df['lap_number'].ge(df['lap_start']) & df['lap_number'].le(df['lap_end'])
        stint_cols = [c for c in df_stints_.columns if c not in ('driver_number', "_key")]
        df.loc[~in_range, stint_cols] = pd.NA
        fh.cache_response(df, 'test', 'laps_and_stints')

        return df

    def sort_combined_laps_and_stints(df_combi_):
        df = df_combi_[df_combi_["is_pit_out_lap"] != True]
        fh.cache_response(df, 'test', 'laps_and_stints_sort')

        return df

    df_laps = get_and_sort_laps(session_key)
    df_stints = get_and_sort_stints(session_key)
    df_combi = combine_laps_and_stints(df_laps, df_stints)
    df_combi_sorted = sort_combined_laps_and_stints(df_combi)

def fp_long_run_analysis():
    """Produce an analysis of long runs in free practice"""
    pass


def teammate_comparison():
    """Produce an analysis of the comparison between teammates in a season"""
    SEASON_2025 = {'2025-02-25', '2025-12-10'}