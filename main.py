import dask.dataframe as dd

# Carrega os datasets
crime = dd.read_csv('dataset/crime.csv', delimiter=';')
district = dd.read_csv('dataset/district.csv', delimiter=';')
neighborhood = dd.read_csv('dataset/neighborhood.csv', delimiter=';')
segment = dd.read_csv('dataset/segment.csv', delimiter=';', dtype={'oneway': 'object'})
time = dd.read_csv('dataset/time.csv', delimiter=';', dtype={'month': 'float64', 'day': 'float64'})
vertice = dd.read_csv('dataset/vertice.csv', delimiter=';')

# Tipos de crime mapeados
crime_types = [
    'total_feminicide', 'total_homicide', 'total_felony_murder',
    'total_bodily_harm', 'total_theft_cellphone', 'total_armed_robbery_cellphone',
    'total_theft_auto', 'total_armed_robbery_auto'
]

# Função para filtrar os crimes por tempo e segmento
def filter_crimes_by_time_and_segments(crime_df, time_df, year=None, years=None, month=None, weekday=None, segments=None):
    if year:
        time_df = time_df[time_df['year'] == year]
    if years:
        time_df = time_df[time_df['year'].isin(years)]
    if month:
        time_df = time_df[time_df['month'] == month]
    if weekday:
        time_df = time_df[time_df['weekday'].isin(weekday)]
    time_ids = time_df['id'].compute()
    filtered_crimes = crime_df[crime_df['time_id'].isin(time_ids)]
    if segments is not None:
        filtered_crimes = filtered_crimes[filtered_crimes['segment_id'].isin(segments)]
    return filtered_crimes

# Função para reorganizar as colunas em pares chave–valor
def melt_and_group_crimes(crime_df, id_vars, value_vars, group_by, filter_zero=True):
    melted = dd.melt(crime_df, id_vars=id_vars, value_vars=value_vars, var_name='crime_type', value_name='total')
    if filter_zero:
        melted = melted[melted['total'] > 0]
    return melted.groupby(group_by)['total'].sum().compute()

# Consulta 1 - Qual o total de crimes por tipo e por segmento das ruas do distrito de IGUATEMI durante o ano de 2016?
iguatemi_id = district[district['name'] == 'IGUATEMI']['id'].compute().iloc[0]
vertices_iguatemi = vertice[vertice['district_id'] == iguatemi_id].compute()
segments_iguatemi_ids = segment[
    segment['start_vertice_id'].isin(vertices_iguatemi['id']) |
    segment['final_vertice_id'].isin(vertices_iguatemi['id'])
]['id'].compute()
crimes_2016 = filter_crimes_by_time_and_segments(crime, time, year=2016, segments=segments_iguatemi_ids)
result_1 = melt_and_group_crimes(crimes_2016, ['segment_id'], crime_types, ['crime_type', 'segment_id'])

# Consulta 2 - Qual o total de crimes por tipo e por segmento das ruas do distrito de IGUATEMI entre 2006 e 2016?
crimes_2006_2016 = filter_crimes_by_time_and_segments(crime, time, years=range(2006, 2017), segments=segments_iguatemi_ids)
result_2 = melt_and_group_crimes(crimes_2006_2016, ['segment_id'], crime_types, ['crime_type', 'segment_id'])

# Consulta 3 - Total de ocorrências de Roubo de Celular e Roubo de Carro no bairro de SANTA EFIGÊNIA em 2015
santa_efigenia_id = neighborhood[neighborhood['name'] == 'Santa Efigênia']['id'].compute().iloc[0]
vertices_santa_efigenia = vertice[vertice['neighborhood_id'] == santa_efigenia_id].compute()
segments_santa_efigenia_ids = segment[
    segment['start_vertice_id'].isin(vertices_santa_efigenia['id']) |
    segment['final_vertice_id'].isin(vertices_santa_efigenia['id'])
]['id'].compute()
crimes_2015 = filter_crimes_by_time_and_segments(crime, time, year=2015, segments=segments_santa_efigenia_ids)
result_3 = melt_and_group_crimes(crimes_2015, ['segment_id'], ['total_armed_robbery_cellphone', 'total_armed_robbery_auto'], ['crime_type', 'segment_id'])

# Consulta 4 - Total de crimes por tipo em vias de mão única da cidade durante o ano de 2012
segments_one_way_ids = segment[segment['oneway'] == 1]['id'].compute()
crimes_2012 = filter_crimes_by_time_and_segments(crime, time, year=2012, segments=segments_one_way_ids)
result_4 = melt_and_group_crimes(crimes_2012, ['segment_id'], crime_types, ['crime_type'])

# Consulta 5 - Total de roubos de carro e celular em todos os segmentos durante o ano de 2017
crimes_2017 = filter_crimes_by_time_and_segments(crime, time, year=2017)
result_5 = melt_and_group_crimes(crimes_2017, ['segment_id'], ['total_armed_robbery_cellphone', 'total_armed_robbery_auto'], ['crime_type'])

# Consulta 6 - IDs de segmentos com maior índice criminal em Novembro de 2010
crimes_nov_2010 = filter_crimes_by_time_and_segments(crime, time, year=2010, month=11)
total_crimes_by_segment = melt_and_group_crimes(crimes_nov_2010, ['segment_id'], crime_types, ['segment_id'])
result_6 = total_crimes_by_segment[total_crimes_by_segment == total_crimes_by_segment.max()].index.tolist()

# Consulta 7 - IDs dos segmentos com maior índice criminal nos finais de semana de 2018
crimes_weekend_2018 = filter_crimes_by_time_and_segments(crime, time, year=2018, weekday=[5, 6])
total_crimes_by_segment_weekend = melt_and_group_crimes(crimes_weekend_2018, ['segment_id'], crime_types, ['segment_id'])
result_7 = total_crimes_by_segment_weekend[total_crimes_by_segment_weekend == total_crimes_by_segment_weekend.max()].index.tolist()