from bs4 import BeautifulSoup
import requests
import pandas as pd
import psycopg2
from sqlalchemy import create_engine

odi = 'https://stats.espncricinfo.com/ci/content/records/282787.html'
test = 'https://stats.espncricinfo.com/ci/content/records/282786.html'
page = requests.get(odi)
soup = BeautifulSoup(page.text, 'html.parser')
table = soup.find('table', {'class': 'engineTable'})
headers=[]
for i in soup.find_all('th'):
    title=i.text.strip()
    headers.append(title)
print(headers)
df = pd.DataFrame(columns=headers)
for row in table.find_all('tr')[1:]:
    data=row.find_all('td')
    row_data= [td.text.strip() for td in data]
    length=len(df)
    df.loc[length]=row_data
print(df)

conn_string = 'postgresql+psycopg2://postgres:prashanth@localhost:5432/youtube'
db = create_engine(conn_string)

df.to_sql('ODI_AllRounders', con=db, if_exists='replace',
          index=False)
