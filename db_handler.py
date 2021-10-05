import sqlalchemy
import pandas as pd
engine=sqlalchemy.create_engine('sqlite:///database.db')
df=pd.read_csv('main.csv')
df.to_sql('main',engine,if_exists='replace',index=False)
df=pd.read_csv('open_positions.csv')
df.to_sql('open_positions',engine,if_exists='replace',index=False)
df=pd.read_csv('shortlist.csv')
df.to_sql('shortlist',engine,if_exists='replace',index=False)
