import pandas as pd
df_trip = pd.read_csv(".\\CSV_files\\busDB_trips.csv")
df_stime = pd.read_csv(".\\CSV_files\\busDB_stoptimes.csv")

df_merg = pd.merge(df_trip, df_stime, on=["trip_id_", "trip_id"])

df_merg.to_csv("mrget.csv", index=False)
