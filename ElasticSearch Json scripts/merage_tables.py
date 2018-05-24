import pandas as pd
from elasticsearch import Elasticsearch, helpers

es = Elasticsearch()

df_stops = pd.read_csv("./CSV_files/stops.csv")
df_stime = pd.read_csv("./CSV_files/stoptimes.csv")
df_trips = pd.read_csv("./CSV_files/trips.csv")

df_stops_stime = pd.merge(df_stops, df_stime, on=["stop_id"])

df_stops_stime = df_stops_stime.drop(['stop_id', 'lat', 'log'], axis=1)

df_merg = pd.merge(df_trips, df_stops_stime, on=["trip_id"])
df_merg = df_merg.drop(['block_id'], axis=1)
df_merg['id'] = df_merg.index + 1
df_merg['arrival_time'] = df_merg['arrival_time'] + ".000"
df_merg['departure_time'] = df_merg['departure_time'] + ".000"

df_merg.to_csv("mrget.csv", index=False)

file_name = "es_data_"
i = 0
c = 0
file = open(file_name + str(c) + ".json", '+w')
print(df_merg.columns.values)
print(df_merg.iloc[0, 0], df_merg.iloc[0, 1], df_merg.iloc[0, 2], df_merg.iloc[0, 3], df_merg.iloc[0, 4],
      df_merg.iloc[0, 5], df_merg.iloc[0, 6], df_merg.iloc[0, 7], df_merg.iloc[0, 8], df_merg.iloc[0, 9])
for row in range(len(df_merg.index)):
    if i > 9999:
        file.close()
        c += 1
        file = open(file_name + str(c) + ".json", '+w')
        i = 0
    index_r = "{\"index\": {\"_index\": \"bustime\", \"_type\": \"_doc\", \"_id\": \"" + str(df_merg.iloc[row, 9]) + "\"}}"
    data_r = "{\"route_id\": \"" + df_merg.iloc[row, 0] + "\", \"trip_headsign\": \"" + df_merg.iloc[
        row, 1] + "\", \"service_id\": \"" + df_merg.iloc[row, 2] + "\", \"shape_id\": \"" + df_merg.iloc[
                 row, 3] + "\",\"trip_id\": \"" + df_merg.iloc[row, 4] + "\",\"arrival_time\": \"" + str(
        df_merg.iloc[row, 6]) + "\",\"departure_time\": \"" + str(
        df_merg.iloc[row, 7]) + "\" ,\"stop_sequence\": " + str(df_merg.iloc[row, 8]) + ",\"name_stop\": \"" + \
             str(df_merg.iloc[row, 5]) + "\"}"
    file.write(index_r + "\n")
    file.write(data_r + "\n")
    i += 1
    # if c==2:
    #     exit()
