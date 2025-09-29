import openf1_analyses as analyses
import openf1_file_helpers as fh
import openf1_get as g

# All 2025 Practice Sessions
query = ("sessions", {"date_start": ">=2025-03-13", "date_end": "<=2025-09-28"})
df = g.get(query[0], query[1])
df_prac = df[df['session_type'] == 'Practice']
df_qual = df[df['session_type'] == 'Qualifying']
df_race = df[df['session_type'] == 'Race']
fh.cache_response(df_prac, 'test', 'season_practice_sessions')
fh.cache_response(df_qual, 'test', 'season_qualifying_sessions')
fh.cache_response(df_race, 'test', 'season_race_sessions')

for session_key in df_prac['session_key'].unique():
    analyses.qualifying_runs(session_key)