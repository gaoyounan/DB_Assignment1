import pandas as pd

df_stops = pd.read_csv("./CSV_files/stops.csv")
df_stime = pd.read_csv("./CSV_files/stoptimes.csv")
df_trips = pd.read_csv("./CSV_files/trips.csv")

df_stops_stime=pd.merge(df_stops,df_stime,on=["stop_id"])

df_stops_stime=df_stops_stime.drop(['stop_id','lat','log'],axis=1)

df_merg = pd.merge(df_trips, df_stops_stime, on=["trip_id"])
df_merg =df_merg.drop(['block_id'],axis=1)
df_merg['id']=df_merg.index+1
df_merg.to_csv("mrget.csv", index=False)
