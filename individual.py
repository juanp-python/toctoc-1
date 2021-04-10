import random
import time
import pandas as pd
import glob
import requests
import requests_random_user_agent
from tqdm import tqdm
from bs4 import BeautifulSoup as bs

pd.set_option('display.max_columns', None)

files = sorted(glob.glob('bases/venta departamento*.csv'))

df = pd.read_csv(files[-1], parse_dates=['fechaPublicacion'])

s = requests.session()
url = 'https://www.toctoc.com/api/lista/propiedades'
access_token_url = 'https://www.toctoc.com/arriendo/casa/metropolitana/las-condes?o=link_menu'
site = s.get(access_token_url)
x_access_token = str(site.content).split('"token":"')[1].split('"}')[0]

headers = {
    'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/87.0.4280.141 Safari/537.36',
    'x-access-token': x_access_token,
}

url = 'https://www.toctoc.com/propiedades/compracorredorasr/casa/vitacura/vende-casa-comoda-en-vitacura-3d-3b-con-piscina-y-quincho-terraza-techada/1699969'
r = s.get(url, headers=headers)
soup = bs(r.content, 'lxml')
token = soup.find(attrs={'name':"__RequestVerificationToken"})['value']

headers = {
    'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/87.0.4280.141 Safari/537.36',
    'x-access-token': x_access_token,
    'x-requested-with': 'XMLHttpRequest',
    'x-xsrf-token': token,
}

container = list()
url = 'https://www.toctoc.com/api/propiedades/usadas/'
errors = 0
for i, row in tqdm(df.iterrows()):
    headers = {
        'user-agent': random.choice(requests_random_user_agent.USER_AGENTS),
        'x-access-token': x_access_token,
        'x-requested-with': 'XMLHttpRequest',
        'x-xsrf-token': token,
    }
    try:
        r = s.get(url+str(row.id), headers=headers)
        container.append(r.json())
        time.sleep(random.randint(200, 500)/100)
        errors = 0
    except:
        errors += 1

    if errors  >= 50:
        print('too many errors')
        break


pd.DataFrame(container).to_csv('datos toctoc.csv', index=False)
