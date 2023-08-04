import requests
from bs4 import BeautifulSoup
import re
import pymongo


url = "https://people.dbmi.columbia.edu/~friedma/Projects/DiseaseSymptomKB/index.html"
response = requests.get(url)
soup = BeautifulSoup(response.content, "html.parser")

table = soup.find_all('table')[0]
rows = table.find_all('tr')

client=pymongo.MongoClient("mongodb+srv://rohithgundaram:FWw1u2VXXzsxYgbw@cluster0.msplvac.mongodb.net/")

db = client["Condition_DB"]
collection = db["Conditions"]


def func(value):
    return ''.join(value.split('\n'))

abd={}

# Open the file in write mode

for row in rows[1:]:
    cells = row.find_all('td')
    if cells: 

        first_column = cells[0].get_text(strip=True).split('_')[-1]
        last_column = cells[-1].get_text(strip=True).split('_')[-1]
        

        first_column=func(first_column)
        last_column=func(last_column)
        # print(first_column)
        # print(last_column)
        

        if first_column!='':
            temp={first_column:[last_column]}
            abd.update(temp)
        else:
            abd[list(abd.keys())[-1]].append(last_column)


insert_result = collection.insert_many([{"disease":k,"symptoms":v} for k, v in abd.items()])
            

