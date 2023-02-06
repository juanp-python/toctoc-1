import random
import time
import pandas as pd
import glob
import requests
import requests_random_user_agent
from tqdm import tqdm
from bs4 import BeautifulSoup as bs
from datetime import datetime

pd.set_option('display.max_columns', None)

s = requests.session()
access_token_url = 'https://www.toctoc.com/arriendo/casa/metropolitana/las-condes?o=link_menu'
headers = {
    'referer': 'https://www.toctoc.com/Arriendo/departamento/?dormitoriodesde=0&dormitoriohasta=5&banosdesde=0&banoshasta=5&superficeDesde=&superficiehasta=&moneda=2&preciodesde=&preciohasta=&ordenarpor=relevantes&cargarmas=2&tipoOperacion=0',
    'user-agent': random.choice(requests_random_user_agent.USER_AGENTS),
    'set-cookie': '_gcl_au=1.1.1104119419.1616532681; _ga=GA1.2.1767627951.1616532683; NPS_93546e30_last_seen=1616532683280; _hjid=6213fe89-5f1b-416e-b5e1-6f16e931922e; optimizelyEndUserId=oeu1617631046171r0.45152490026734204; optimizelySegments=%7B%222204271535%22%3A%22gc%22%2C%222215970531%22%3A%22false%22%2C%222232940041%22%3A%22campaign%22%7D; optimizelyBuckets=%7B%7D; __insp_wid=2107690165; __insp_nv=true; __insp_targlpu=aHR0cHM6Ly93d3cudG9jdG9jLmNvbS9wcm9waWVkYWRlcy9jb21wcmFjb3JyZWRvcmFzci9jYXNhL251bm9hL3Byb3BpZWRhZC1iYXJyaW8taXRhbGlhLzEyNjA5NTE%3D; __insp_targlpt=Q2FzYSBhIGxhIHZlbnRhIGVuIMORdcOxb2EsIFByb3BpZWRhZCBCYXJyaW8gSXRhbGlh; __insp_norec_sess=true; mp_29ae90688062e4e2e6d80b475cef8685_mixpanel=%7B%22distinct_id%22%3A%20%22178a252edece87-0c11d91f673648-6418227c-384000-178a252ededd07%22%2C%22%24device_id%22%3A%20%22178a252edece87-0c11d91f673648-6418227c-384000-178a252ededd07%22%2C%22utm_source%22%3A%20%22mkt_recomendados%22%2C%22utm_medium%22%3A%20%22mailing_usados%22%2C%22utm_campaign%22%3A%20%22mailing_1705274_boton%22%2C%22%24initial_referrer%22%3A%20%22%24direct%22%2C%22%24initial_referring_domain%22%3A%20%22%24direct%22%7D; __insp_slim=1617920579868; X-DATA=003dee4c-5046-40ca-a9ca-ca529419f9f4; _gid=GA1.2.907897406.1618065582; _hjTLDTest=1; _hjAbsoluteSessionInProgress=1; X-DATA-NPSW={"CantidadVisitas":1,"FechaCreacion":"2021-04-10T11:39:36.1245141-03:00","FechaUltimoIngreso":"2021-04-10T11:45:14.1405686-03:00","Detalles":[{"TipoVistaNPS":8,"Cantidad":3,"FechaUltimoIngreso":"2021-04-10T11:45:14.1405686-03:00"}]}; _gat=1',
}
site = s.get(access_token_url, headers=headers, timeout=20)

x_access_token = str(site.content).split('"token":"')[1].split('"}')[0]

headers = {
    'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/87.0.4280.141 Safari/537.36',
    'x-access-token': x_access_token,
}

# copiar y pegar la url de una propiedad que funcione, con el tiempo van muriendo
url = 'https://www.toctoc.com/propiedades/arriendoparticularsr/departamento/santiago/aldunate-1064-a-pasos-metro-parque-ohiggins/2131478?o=mapa'
r = s.get(url, headers=headers)
soup = bs(r.content, 'lxml')

token = soup.find(attrs={'name':"__RequestVerificationToken"})['value']

headers = {
    'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/87.0.4280.141 Safari/537.36',
    'x-access-token': x_access_token,
    'x-requested-with': 'XMLHttpRequest',
    'x-xsrf-token': token,
}

tipo = 'compra'
propiedad = 'casa'
status = "nuevo"

# status = ['nuevo', 'usado']
# propiedades = ['casa', 'departamento']
# tipos = ['arriendo', 'compra']
# today = datetime.today().strftime("%Y-%m-%d")

headers = {
    'user-agent': random.choice(requests_random_user_agent.USER_AGENTS),
    'x-requested-with': 'XMLHttpRequest',
    'x-xsrf-token': token,
}

def read_filter_df(tipo, propiedad, status, filtro_min, filtro_max):
    filename = f'bases/{tipo}_{propiedad}_{status}*/**.csv'
    file = sorted(glob.glob(filename))[-1]
    print('file es: '+(str(file)))
    df = pd.read_csv(file)
    df.iloc[:,14] = pd.to_datetime(df.iloc[:,14], dayfirst=True)
    df = df.sort_values(by='14', ascending=False)
    df = df.reset_index(drop=True)
    df_2023 = df[df.iloc[:,14]>filtro_min]
    df_2023 = df_2023[df_2023.iloc[:,14]<filtro_max]
    return df_2023

fecha = '_2023_01_15.csv' # ACTUALIZAAAAAR
fecha_min = '2022-12-31' # ACTUALIZAAAAAR
fecha_max = '2023-01-16'  # ACTUALIZAAAAAR

df_arriendo_casa_usado = read_filter_df("arriendo", "casa", "usado", fecha_min, fecha_max)
df_arriendo_departamento_usado = read_filter_df("arriendo", "departamento", "usado", fecha_min, fecha_max)
df_arriendo_local_usado = read_filter_df("arriendo", "local", "usado", fecha_min, fecha_max)
df_arriendo_oficina_usado = read_filter_df("arriendo", "oficina", "usado", fecha_min, fecha_max)
df_compra_casa_nuevo = read_filter_df("compra", "casa", "nuevo", fecha_min, fecha_max)
df_compra_casa_usado = read_filter_df("compra", "casa", "usado", fecha_min, fecha_max)
df_compra_departamento_nuevo = read_filter_df("compra", "departamento", "nuevo", fecha_min, fecha_max)
df_compra_departamento_usado = read_filter_df("compra", "departamento", "usado", fecha_min, fecha_max)
df_compra_local_usado = read_filter_df("compra", "local", "usado", fecha_min, fecha_max)
df_compra_oficina_usado = read_filter_df("compra", "oficina", "usado", fecha_min, fecha_max)
df_compra_parcela_usado = read_filter_df("compra", "parcela", "usado", fecha_min, fecha_max)
df_compra_terreno_usado = read_filter_df("compra", "terreno", "usado", fecha_min, fecha_max)

url_usado = 'https://www.toctoc.com/api/propiedades/usadas/'
url_nuevo = 'https://www.toctoc.com/api/propiedad/nueva/compra-nuevo?id='
def extraer_data_individual(bbdd, link, tipo, propiedad, status):
    container = list()
    for i in tqdm(range(len(bbdd))):
        try:
            r = s.get(link+str(bbdd.iloc[i,1]), headers=headers)
            container.append(r.json())
            time.sleep(random.randint(200, 500)/100)
            errors = 0
            if r.status_code == 200:
                pd.DataFrame(container).to_csv(f'datos_{tipo}_{propiedad}_{status}{fecha}', index=False)
                # print(i, 'saved to disk')
        except Exception:
            errors += 1

        if errors  >= 50:
            print('too many errors')
            break
    return errors

extraer_data_individual(df_arriendo_casa_usado, url_usado, "arriendo", "casa", "usado")
extraer_data_individual(df_arriendo_departamento_usado, url_usado, "arriendo", "departamento", "usado")
extraer_data_individual(df_arriendo_local_usado, url_usado, "arriendo", "local", "usado")
extraer_data_individual(df_arriendo_oficina_usado, url_usado, "arriendo", "oficina", "usado")
extraer_data_individual(df_compra_casa_nuevo, url_nuevo, "compra", "casa", "nuevo")
extraer_data_individual(df_compra_casa_usado, url_usado, "compra", "casa", "usado")
extraer_data_individual(df_compra_departamento_nuevo, url_nuevo, "compra", "departamento", "nuevo")
extraer_data_individual(df_compra_departamento_usado, url_usado, "compra", "departamento", "usado")
extraer_data_individual(df_compra_local_usado, url_usado, "compra", "local", "usado")
extraer_data_individual(df_compra_oficina_usado, url_usado, "compra", "oficina", "usado")
extraer_data_individual(df_compra_parcela_usado, url_usado, "compra", "parcela", "usado")
extraer_data_individual(df_compra_terreno_usado, url_usado, "compra", "terreno", "usado")

# extraer_data_individual(df_arriendo_departamento_usado_continuacion, url_usado, "continuacion_arriendo", "departamento", "usado")

# df_arriendo_departamento_usado.iloc[:,1].unique()
# df_depto = pd.read_csv("datos_arriendo_departamento_usado_2023_01_31.csv")
# propiedad = df_depto['Propiedad'].apply(lambda row: dict(eval(row))).apply(pd.Series)
# df_arriendo_departamento_usado_continuacion = df_arriendo_departamento_usado[~df_arriendo_departamento_usado.iloc[:,1].isin(list(propiedad.Id))]
