from bs4 import BeautifulSoup
import requests
import pandas as pd
import psycopg2

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

conn = psycopg2.connect(host='localhost', port = 5432, database='youtube',
                        user='postgres', password='prashanth')
cur = conn.cursor()
cur.execute('drop table if exists ODI_Allrounders')
sql = ''' create table if not exists ODI_Allrounders(
            Player varchar(100) not null, 
            Span text not null,
            Matches int not null,
            Runs int not null,
            HS int not null,
            BatAve numeric(5,3) not null,
            hundreds int not null,
            Wkts int not null,
            BBI text not null,
            BowAvg numeric(5,3) not null,
            fifers int not null,
            catches int not null,
            stamps int not null
            )
            '''
cur.execute(sql)
conn.commit()
