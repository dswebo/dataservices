import pandas as pd
import numpy as np

df = pd.read_csv("csv/58.csv", sep=';', header=None, names=["USER ID","DH SERV","Action type","AAP ID","TE ID",
                                                          "Project ID","Project name","Channel ID","Channel name",
                                                          "Campaign ID","Campaign name","Site ID","Site name",
                                                          "Insertion ID","Insertion name","Creative ID",
                                                          "Creative name","Conversion type","Conversion page ID",
                                                          "Conversion page name","Item number","Client ID",
                                                          "Order number"])


df = df[["USER ID","DH SERV","Site name","Action type", "Conversion page ID", "Insertion name","Conversion page name","Conversion type"]]
df.rename(columns={"USER ID":"customer_id","DH SERV":"date","Site name":"channel","Action type":"conversion"}, inplace=True)
df["Action type"] = df["conversion"]

d={'tracker':1, 'impression':0, 'event':0, 'click':0, 'conversion':1}
df['conversion'] = df['conversion'].map(d)

df.fillna(0, inplace=True)

cooks = df[df['Conversion page ID']==55]['customer_id'].unique()
df = df[(df.customer_id.isin(cooks)) & (df['Conversion page ID'].isin([0,55]))]
print(df.shape)
df = df.drop_duplicates(subset=['customer_id', 'date'])
print(df.shape)

df.date = pd.to_datetime(df.date)
df = df.sort_values(['customer_id','date'], ascending=[True,True]).reset_index(drop=True)

df["path_no"] = df.groupby("customer_id")["conversion"].shift(1).fillna(0)
df["path_no"] = df.groupby("customer_id")["path_no"].cumsum()+ 1
df = df[df['path_no']==1]

df["shift_date"] = df.groupby("customer_id")["date"].shift(1).fillna(df['date'])
df['shift_date'] = pd.to_datetime(df['shift_date'])
df["pause_visit"] = df["date"] - df["shift_date"]
df.info()

insertion_video_name = [name for name in df["Insertion name"].unique() if name.find('video')>=0]

def create_event(Action_type, Insertion_name):
    if (Action_type=='impression') & (Insertion_name in insertion_video_name):
        return 'event'
    else:
        return Action_type


df['Action type'] = np.vectorize(create_event)(df['Action type'], df['Insertion name'])

df = df[df['Action type'].isin(['click','tracker','impression','conversion']) |
           ( (df['Action type']=='event') & 
            ( (df['pause_visit']=='0 seconds') | (df['pause_visit']>'10 seconds') ) )]

df = df[['customer_id', 'date', 'channel', 'conversion', 'Conversion page ID', 'Insertion name', 'Conversion page name', 'Conversion type', 'Action type']]
df.head()

df.to_csv('58_prepared.csv')