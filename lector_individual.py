import pandas as pd
import glob
import plotly.graph_objs as go

pd.set_option('display.max_columns', None)
pd.set_option('display.max_colwidth',1000)

files = glob.glob('bases_individual/datos toctoc venta*.csv')
container = [pd.read_csv(file) for file in files]
df = pd.concat(container)
df = df[df.ModelErrors.isna()].copy()
df = df[~df['UriFicha'].isna()].copy()
df = df[df['Propiedad'].apply(lambda x: True if type(x) == str else False)].copy()
df = df[df['Data'].str.contains('Propiedad')].copy()

propiedad = df['Propiedad'].apply(lambda row: dict(eval(row))).apply(pd.Series)
data = df['Data'].apply(lambda row: dict(eval(row))).apply(pd.Series)
df = pd.concat([df, propiedad, data], axis=1)

df.PrecioArriendoEstimadoPesos = pd.to_numeric(df.PrecioArriendoEstimadoPesos.replace('\D+', '', regex = True))

df = df[df['PrecioUF'] > 0].copy()

df['metros_max'] = df[['MetrosUtiles', 'MetrosConstruidos']].max(1)
df = df[df['metros_max'] > 0].copy()
df = df[df['Dormitorios'] < 10].copy()

estadistica = df['Estadistica'].apply(pd.Series)
df = pd.concat([df, estadistica], axis=1)
df = df.loc[:, ~df.columns.duplicated()]
df = df.drop(['Propiedad', 'Data', 'Estadistica'], 1)

interesados_total = df.groupby('Comuna')['CantidadInteresados'].sum().div(df.groupby('Comuna')['CantidadVisitas'].sum()).sort_values(ascending=False)
trace = go.Bar(
    x=interesados_total.index,
    y=interesados_total,
    name='Interesados / Total de Visitas',
)
layout = go.Layout(
    title='% Interesados por Comuna',
    yaxis_tickformat='.0%',
)
fig = go.Figure([trace], layout)
fig


favoritos_total = df[df.Dormitorios < 8].groupby('Dormitorios')['CantidadInteresados'].sum().div(df[df.Dormitorios < 8].groupby('Dormitorios')['CantidadVisitas'].sum()).sort_values(ascending=False)
trace = go.Bar(
    x=favoritos_total.index,
    y=favoritos_total,
    name='Interesados / Total de Visitas',
)
layout = go.Layout(
    title='% Interesados / Total de Visitas',
    yaxis_tickformat='.0%',
    xaxis_title='N° de Dormitorios',
    xaxis_dtick=1,
)
fig = go.Figure([trace], layout)
fig

df.groupby(['Comuna', 'Dormitorios']).median()
