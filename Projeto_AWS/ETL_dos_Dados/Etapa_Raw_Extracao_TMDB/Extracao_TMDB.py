import requests
import sys 
import os
import csv
from datetime import datetime
import pandas as pd
import json
import boto3
import concurrent.futures
import io


#CRIANDO DATA FRAME DE POPULARIDADE GERAL

# IDs dos gêneros desejados
ids_genero = [35]
# Número máximo de páginas por ano
max_paginas_por_ano = 10
# Dicionário para armazenar os DataFrames de filmes por ano e por gênero
df_por_ano_genero = {}
# Loop pelos anos
for ano in range(1985, 2002):
    resultados_por_ano_genero = {'Ano': [], 'Genre_ID': [], 'id': [], 'title': [], 'release_date': [], 'popularity': [], 'vote_average': [], 'vote_count': [], 'adult':[]}
    # Loop pelos IDs de gênero
    for id_genero in ids_genero:
        # Loop pelas páginas
        for pagina in range(1, max_paginas_por_ano + 1):
            # URL base da API do TMDb para pesquisa de filmes por gênero
            base_url = "https://api.themoviedb.org/3/discover/movie"
            # Parâmetros da pesquisa
            params = {
                "api_key": 
                "with_genres": id_genero,
                "primary_release_year": ano,
                "page": pagina
            }
            
            # Fazer a solicitação à API
            response = requests.get(base_url, params=params)
            
            # Verificar se a solicitação foi bem-sucedida
            if response.status_code == 200:
                # Converter a resposta para JSON
                data = response.json()
                # Adicionar os resultados à lista
                for result in data.get('results', []):
                    resultados_por_ano_genero['Ano'].append(ano)
                    resultados_por_ano_genero['Genre_ID'].append(id_genero)
                    resultados_por_ano_genero['id'].append(result['id'])
                    resultados_por_ano_genero['title'].append(result['title'])
                    resultados_por_ano_genero['release_date'].append(result['release_date'])
                    resultados_por_ano_genero['popularity'].append(result['popularity'])
                    resultados_por_ano_genero['vote_average'].append(result['vote_average'])
                    resultados_por_ano_genero['vote_count'].append(result['vote_count'])
                    resultados_por_ano_genero['adult'].append(result['adult'])
                    
                # Verificar se atingiu a última página
                if pagina >= data['total_pages']:
                    break
            else:
                print("Erro ao fazer a solicitação:", response.status_code)
                break
       
    # Criar um DataFrame com os resultados do ano atual
    df_por_ano_genero[ano] = pd.DataFrame(resultados_por_ano_genero)

# Juntar todos os DataFrames em um único DataFrame
df_tmdb = pd.concat(df_por_ano_genero.values(), ignore_index=True)

# Remover itens duplicados com base na coluna 'id'
df_tmdb.drop_duplicates(subset='id', inplace=True)


# Função para consultar o orçamento de um filme por ID
def consultar_dados_por_id(movie_id):
    url = f"https://api.themoviedb.org/3/movie/{movie_id}?language=en-US"
    params = {
        "api_key": "1aa35133d1a489f4c22da98ed4819d22",
    }
    response = requests.get(url, params=params)
    if response.status_code == 200:
        data = response.json()
        return {
            'budget': data.get('budget', None),
            'imdb_id': data.get('imdb_id', None),
            'runtime': data.get('runtime', None),
            'revenue':data.get('revenue', None),
        }
    else:
        print(f"Erro ao consultar o filme com ID {movie_id}. Status code: {response.status_code}")
        return None


# Função para processar consultas em paralelo
def processar_consultas(movie_ids):
    with concurrent.futures.ThreadPoolExecutor() as executor:
        return list(executor.map(consultar_dados_por_id, movie_ids))

# Lista de IDs de filmes
movie_ids = df_tmdb['id'].tolist()


# Consulta dos dados em paralelo
dados_filmes = processar_consultas(movie_ids)


# Criando DataFrame com os dados consultados
df_dados_filmes = pd.DataFrame(dados_filmes)


# Redefinindo o índice do DataFrame df_tmdb
df_tmdb.reset_index(drop=True, inplace=True)

# Redefinindo o índice do DataFrame df_dados_filmes
df_dados_filmes.reset_index(drop=True, inplace=True)


# Criando DataFrame com os dados consultados
df_dados_filmes = pd.DataFrame(dados_filmes)

# Combinando os DataFrames
df_tmdb = pd.concat([df_tmdb, df_dados_filmes], axis=1)

#Salvando Dataframe no S3

s3_client = boto3.client('s3', 
                      aws_access_key_id=
                      aws_secret_access_key=
                      aws_session_token=


file_path = 'Raw/TMDB/JSON/'
data_atual = datetime.now()
data_processamento = data_atual.strftime("%Y/%m/%d")
file_path_dest = f'{file_path}{data_processamento}/'

# Definir o limite de registros desejado
limite_registros = 100  # Limite de registros por arquivo

# Verificar o número total de registros no DataFrame
num_registros = len(df_tmdb)

# Lista para armazenar os DataFrames menores
dataframes_divididos = []

# Verificar se o número total de registros excede o limite
if num_registros > limite_registros:
    print("Número total de registros excede 100. Dividindo em arquivos menores.")

    # Dividir os dados em DataFrames menores com no máximo 100 registros
    num_dataframes = num_registros // limite_registros
    resto = num_registros % limite_registros

    inicio = 0
    for i in range(num_dataframes):
        fim = inicio + limite_registros
        df_temp = df_tmdb.iloc[inicio:fim]
        dataframes_divididos.append(df_temp)
        inicio = fim

    # Adicionar o DataFrame restante, se houver
    if resto > 0:
        df_temp = df_tmdb.iloc[inicio:]
        dataframes_divididos.append(df_temp)

    json_buffer = io.StringIO()

    try:
            # Escrever todos os dados em um único arquivo JSON
        for i, item in enumerate(dataframes_divididos):
                arquivo_json = f"parte_{i}.json"
                item.to_json(json_buffer,orient='records', lines=True)
                bucket_name = 'data-lake-ahelena'
                s3_client.put_object(
                Bucket=bucket_name,
                Key=file_path_dest+arquivo_json,
                Body=json_buffer.getvalue(),
                ContentType='application/json')
                print(f'Arquivo JSON salvo com sucesso no S3: {file_path_dest}')


    except Exception as e:
            print(f"Erro ao salvar o arquivo JSON no S3: {str(e)}")
            