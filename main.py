import dask.dataframe as dd

# Carrega os datasets
crime = dd.read_csv('dataset/crime.csv', delimiter=';')
district = dd.read_csv('dataset/district.csv', delimiter=';')
neighborhood = dd.read_csv('dataset/neighborhood.csv', delimiter=';')
segment = dd.read_csv('dataset/segment.csv', delimiter=';', dtype={'oneway': 'object'})
time = dd.read_csv('dataset/time.csv', delimiter=';', dtype={'month': 'float64'})
vertice = dd.read_csv('dataset/vertice.csv', delimiter=';')

# Tipos de crime mapeados
crime_types = [
    'total_feminicide', 'total_homicide', 'total_felony_murder',
    'total_bodily_harm', 'total_theft_cellphone', 'total_armed_robbery_cellphone',
    'total_theft_auto', 'total_armed_robbery_auto'
]

# Consulta 1 - Qual o total de crimes por tipo e por segmento das ruas do distrito de IGUATEMI durante o ano de 2016?

# Pegar o id do distrito de IGUATEMI
iguatemi_id = district[district['name'] == 'IGUATEMI']['id'].compute().iloc[0]

# Vertices em IGUATEMI
vertices_iguatemi = vertice[vertice['district_id'] == iguatemi_id].compute()

# Segmentos que possuem vertices em IGUATEMI
segments_iguatemi = segment[
    segment['start_vertice_id'].isin(vertices_iguatemi['id']) |
    segment['final_vertice_id'].isin(vertices_iguatemi['id'])
]

segments_iguatemi_ids = segments_iguatemi['id'].compute()

# Crimes desse segmento
crimes_iguatemi_segments = crime[crime['segment_id'].isin(segments_iguatemi_ids)]

# Filtro de 2016
time_2016 = time[time['year'] == 2016]

time_2016_ids = time_2016['id'].compute()

# Crimes de 2016
crimes_2016 = crimes_iguatemi_segments[crimes_iguatemi_segments['time_id'].isin(time_2016_ids)]

# Transforma as colunas em linhas para ter 2 novas colunas: 'crime_type' e 'total'

crimes_melted = dd.melt(
    crimes_2016,
    id_vars=['segment_id'],
    value_vars=crime_types,
    var_name='crime_type',
    value_name='total'
)

# Remove os casos com total igual a zero
crimes_melted = crimes_melted[crimes_melted['total'] > 0]

# Consulta 1 - Resultado
result_1 = crimes_melted.groupby(['crime_type', 'segment_id'])['total'].sum().compute()

# Consulta 2 - Qual o total de crimes por tipo e por segmento das ruas do distrito de IGUATEMI entre 2006 e 2016?

# Filtro para os anos de 2006 a 2016
time_2006_2016 = time[time['year'].isin(range(2006, 2017))]
time_2006_2016_ids = time_2006_2016['id'].compute()

# Crimes nos segmentos de iguatemi durante 2006 a 2016
crimes_2006_2016 = crimes_iguatemi_segments[crimes_iguatemi_segments['time_id'].isin(time_2006_2016_ids)]

# Transforma as colunas em linhas
crimes_melted_2006_2016 = dd.melt(
    crimes_2006_2016,
    id_vars=['segment_id'],
    value_vars=crime_types,
    var_name='crime_type',
    value_name='total'
)

# Remove os casos com total igual a zero
crimes_melted_2006_2016 = crimes_melted_2006_2016[crimes_melted_2006_2016['total'] > 0]

# Consulta 2 - Resultado
result_2 = crimes_melted_2006_2016.groupby(['crime_type', 'segment_id'])['total'].sum().compute()

# Consulta 3 - Total de ocorrências de Roubo de Celular e Roubo de Carro no bairro de SANTA EFIGÊNIA em 2015

# Pegar o id do bairro de SANTA EFIGÊNIA
santa_efigenia_id = neighborhood[neighborhood['name'] == 'Santa Efigênia']['id'].compute().iloc[0]

# Vertices no bairro SANTA EFIGÊNIA
vertices_santa_efigenia = vertice[vertice['neighborhood_id'] == santa_efigenia_id].compute()

# Segmentos que possuem vertices em SANTA EFIGÊNIA
segments_santa_efigenia = segment[
    segment['start_vertice_id'].isin(vertices_santa_efigenia['id']) |
    segment['final_vertice_id'].isin(vertices_santa_efigenia['id'])
]
segments_santa_efigenia_ids = segments_santa_efigenia['id'].compute()

# Crimes nesses segmentos
crimes_santa_efigenia = crime[crime['segment_id'].isin(segments_santa_efigenia_ids)]

# Filtro de 2015
time_2015 = time[time['year'] == 2015]
time_2015_ids = time_2015['id'].compute()

# Crimes de 2015
crimes_2015 = crimes_santa_efigenia[crimes_santa_efigenia['time_id'].isin(time_2015_ids)]

# Seleciona apenas roubo de celular e roubo de carro
crimes_melted_2015 = dd.melt(
    crimes_2015,
    id_vars=['segment_id'],
    value_vars=['total_armed_robbery_cellphone', 'total_armed_robbery_auto'],
    var_name='crime_type',
    value_name='total'
)
crimes_melted_2015 = crimes_melted_2015[crimes_melted_2015['total'] > 0]

# Resultado da consulta 3
result_3 = crimes_melted_2015.groupby(['crime_type', 'segment_id'])['total'].sum().compute()


# Consulta 4 - Total de crimes por tipo em vias de mão única da cidade durante o ano de 2012

# Filtra segmentos de mão única
segments_one_way = segment[segment['oneway'] == 1]
segments_one_way_ids = segments_one_way['id'].compute()

# Crimes nesses segmentos
crimes_one_way = crime[crime['segment_id'].isin(segments_one_way_ids)]

# Filtro de 2012
time_2012 = time[time['year'] == 2012]
time_2012_ids = time_2012['id'].compute()

# Crimes de 2012
crimes_2012 = crimes_one_way[crimes_one_way['time_id'].isin(time_2012_ids)]

# Transforma as colunas em linhas
crimes_melted_2012 = dd.melt(
    crimes_2012,
    id_vars=['segment_id'],
    value_vars=crime_types,
    var_name='crime_type',
    value_name='total'
)
crimes_melted_2012 = crimes_melted_2012[crimes_melted_2012['total'] > 0]

# Resultado da consulta 4
result_4 = crimes_melted_2012.groupby('crime_type')['total'].sum().compute()


# Consulta 5 - Total de roubos de carro e celular em todos os segmentos durante o ano de 2017

# Filtro de 2017
time_2017 = time[time['year'] == 2017]
time_2017_ids = time_2017['id'].compute()

# Crimes de 2017
crimes_2017 = crime[crime['time_id'].isin(time_2017_ids)]

# Seleciona apenas roubo de celular e roubo de carro
crimes_melted_2017 = dd.melt(
    crimes_2017,
    id_vars=['segment_id'],
    value_vars=['total_armed_robbery_cellphone', 'total_armed_robbery_auto'],
    var_name='crime_type',
    value_name='total'
)
crimes_melted_2017 = crimes_melted_2017[crimes_melted_2017['total'] > 0]

# Resultado da consulta 5
result_5 = crimes_melted_2017.groupby('crime_type')['total'].sum().compute()


# Consulta 6 - IDs de segmentos com maior índice criminal em Novembro de 2010

# Filtro de Novembro de 2010
time_nov_2010 = time[(time['year'] == 2010) & (time['month'] == 11)]
time_nov_2010_ids = time_nov_2010['id'].compute()

# Crimes nesse período
crimes_nov_2010 = crime[crime['time_id'].isin(time_nov_2010_ids)]

# Soma total de crimes por segmento
crimes_melted_nov_2010 = dd.melt(
    crimes_nov_2010,
    id_vars=['segment_id'],
    value_vars=crime_types,
    var_name='crime_type',
    value_name='total'
)
crimes_melted_nov_2010 = crimes_melted_nov_2010[crimes_melted_nov_2010['total'] > 0]
total_crimes_by_segment = crimes_melted_nov_2010.groupby('segment_id')['total'].sum().compute()

# Segmentos com maior índice criminal
max_crime = total_crimes_by_segment.max()
result_6 = total_crimes_by_segment[total_crimes_by_segment == max_crime].index.tolist()


# Consulta 7 - IDs dos segmentos com maior índice criminal nos finais de semana de 2018

# Filtro de 2018 e finais de semana (weekday 5 ou 6)
time_weekend_2018 = time[(time['year'] == 2018) & (time['weekday'].isin([5, 6]))]
time_weekend_2018_ids = time_weekend_2018['id'].compute()

# Crimes nesse período
crimes_weekend_2018 = crime[crime['time_id'].isin(time_weekend_2018_ids)]

# Soma total de crimes por segmento
crimes_melted_weekend_2018 = dd.melt(
    crimes_weekend_2018,
    id_vars=['segment_id'],
    value_vars=crime_types,
    var_name='crime_type',
    value_name='total'
)
crimes_melted_weekend_2018 = crimes_melted_weekend_2018[crimes_melted_weekend_2018['total'] > 0]
total_crimes_by_segment_weekend = crimes_melted_weekend_2018.groupby('segment_id')['total'].sum().compute()

# Segmentos com maior índice criminal
max_crime_weekend = total_crimes_by_segment_weekend.max()
result_7 = total_crimes_by_segment_weekend[total_crimes_by_segment_weekend == max_crime_weekend].index.tolist()