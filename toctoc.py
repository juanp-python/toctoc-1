import pandas as pd
import requests
import random
import time
from tqdm import tqdm

pd.set_option('display.max_columns', None)

s = requests.session()
url = 'https://www.toctoc.com/api/lista/propiedades'
access_token_url = 'https://www.toctoc.com/arriendo/casa/metropolitana/las-condes?o=link_menu'
site = s.get(access_token_url)
x_access_token = str(site.content).split('"token":"')[1].split('"}')[0]

headers = {
    'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/87.0.4280.141 Safari/537.36',
    'x-access-token': x_access_token,
}

dict_operacion = {
    'arriendo': 0,
    'venta': -1
}
dict_operacion_familia = {
    'arriendo': 3,
    'venta': 2
}
dict_propiedad = {
    'casa': 1,
    'departamento': 2
}

def params(start=None, cargarmas=30, end=None, operacion='arriendo', tipo='casa'):
    if end is None:
        end = start + 9
    return {
        'dormitoriodesde': 0,
        'dormitoriohasta': 5,
        'banosdesde': 0,
        'banoshasta': 5,
        'superficeDesde': start,
        'superficiehasta': end,
        'moneda': 2,
        'preciodesde': '',
        'preciohasta': '',
        'ordenarpor': 'relevantes',
        'cargarmas': cargarmas,
        'tipoOperacion': dict_operacion[operacion],
        'tipoOperacionFamilia': dict_operacion_familia[operacion],
        'tipoPropiedad': dict_propiedad[tipo],
        'comuna': '',
        'region': 'metropolitana',
        }

errors = list()

for tipo in ['casa', 'departamento']:
    for operacion in ['venta', 'arriendo']:
        container = list()
        for region in ['metropolitana', 'valparaiso']:
            for i in tqdm(range(0, 500, 10)):
                try:
                    p = params(start=i, cargarmas=1, operacion=operacion, tipo=tipo)
                    site = s.get(url, params=p, headers=headers)
                    num_paginas = min(int(site.json()['totalResultados'] / 30) + 1, 30)
                    p = params(start=i, cargarmas=num_paginas, operacion=operacion, tipo=tipo)
                    site = s.get(url, params=p, headers=headers)

                    if 'list' not in site.json().keys():
                        num_paginas -= 1
                        site = s.get(url, params=params(start=i, cargarmas=num_paginas), headers=headers)

                    df = pd.DataFrame(site.json()['list'])
                    df['start'] = i
                    df['resultados'] = site.json()['totalResultados']
                    df['region'] = region
                    container.append(df.assign(start=i))
                except Exception as e:
                    errors.append([operacion, tipo, region, i, e])
                time.sleep(random.randint(1, 7))

        df = pd.concat(container)

        df['fechaPublicacion'] = pd.to_datetime(df['fechaPublicacion'], dayfirst=True)
        filename = f"bases/{operacion} {tipo} toctoc {pd.Timestamp.today().strftime('%Y-%m-%d')}.csv"
        df.to_csv(filename, index=False)

pd.DataFrame(errors).to_excel('errores.xlsx', index=False)
