# Projeto_AWS

## Breve Explicação do Projeto:

## Objetivo: 

Desenvolver uma solução completa para extrair, processar e visualizar dados de filmes a partir da API do TMDb (The Movie Database), utilizando ferramentas da AWS.

## Etapas do Projeto:

Extração de Dados da API do TMDb com Python:

## Utilização da API do TMDb para coletar dados sobre filmes, como títulos, avaliações, gêneros e outros detalhes.

Python foi utilizado para automação da extração e para transformar esses dados em um formato adequado para armazenamento.
Integração com a AWS:

## Armazenamento de Dados no Amazon S3: 

Os dados extraídos foram armazenados de maneira segura e escalável no S3, utilizando o armazenamento de objetos.

## Tratamento de Dados com AWS Glue:

AWS Glue foi utilizado para realizar o processamento e transformação dos dados, como limpeza, formatação e ajustes necessários antes de carregar para o Athena.

## Criação de Views no Amazon Athena:

Com os dados já tratados e prontos, criamos views no Amazon Athena para facilitar as consultas SQL e estruturar os dados de maneira eficiente para a presente a análise e para análises futuras.


## A Análise dos Dados extraidos do TMDB:

![Atores](DesafioFinal/Etapa_Vizualizacao/Artistas.png)

- **Tendências ao Longo do Tempo:**

Análise das tendências crescentes ou decrescentes em filmes estrelados por Adam Sandler e Jim Carrey.

- **Como a produção de filmes estrelados pelos atores tem variado ao longo dos anos?**

Análise das variações significativas nas receitas, orçamentos e popularidade desses filmes ao longo do tempo.


# Análise Comparativa dos Filmes Estrelados por Adam Sandler e Jim Carrey

Este dashboard visa fornecer uma visão detalhada sobre a popularidade, desempenho financeiro e tendências dos filmes estrelados por Adam Sandler e Jim Carrey. A seguir, descrevemos os principais gráficos e suas respectivas análises.

## Gráficos e Análises


![Dashboard](DesafioFinal/Etapa_Vizualizacao/Analise_Comparativa.png)



### 1. Gráfico de Rosca: Comparação da Popularidade Total dos Filmes

- **Descrição:** Este gráfico de rosca exibe a popularidade total dos filmes estrelados por Adam Sandler e Jim Carrey.
- **Objetivo:** Comparar visualmente a popularidade agregada de todos os filmes de cada ator. Ajuda a entender qual ator tem um maior impacto geral em termos de popularidade.



### 2. Gráfico de Combinação de Barras e Linha: Relação entre Lançamentos Anuais e Popularidade

- **Descrição:** Este gráfico combina barras e linha para mostrar a relação entre o número de lançamentos anuais e a popularidade dos filmes.
- **Objetivo:** Visualizar como a frequência de lançamentos de filmes influencia a popularidade de Adam Sandler e Jim Carrey ao longo dos anos.



### 3. Gráfico de Combinação de Barras: Desempenho Financeiro dos Atores: Receita Total vs. Orçamento Total

- **Descrição:** Este gráfico de barras compara a receita total e o orçamento total dos filmes estrelados por Adam Sandler e Jim Carrey.
- **Objetivo:** Avaliar o desempenho financeiro dos filmes, comparando o investimento realizado (orçamento) com o retorno financeiro (receita).



### 4. Gráfico de Linhas: Tendências e Mudanças no Orçamento dos Filmes ao Longo dos Anos

- **Descrição:** Este gráfico de linhas mostra as mudanças no orçamento dos filmes ao longo dos anos.
- **Objetivo:** Identificar tendências e variações no orçamento dos filmes estrelados por Adam Sandler e Jim Carrey ao longo do tempo.



### 5. Gráfico de Barras Verticais: Os 10 Filmes com Maior Receita Entre os Atores

- **Descrição:** Este gráfico de barras verticais exibe os 10 filmes com a maior receita entre Adam Sandler e Jim Carrey.
- **Objetivo:** Destacar os filmes mais lucrativos dentre eles, oferecendo uma visão clara dos maiores sucessos financeiros.



### 6. Gráfico de Sankey: Perfil dos Gêneros Cinematográficos dos Filmes

- **Descrição:** O gráfico de Sankey mostra a distribuição dos gêneros cinematográficos dos filmes estrelados por Adam Sandler e Jim Carrey.
- **Objetivo:** Visualizar como os gêneros dos filmes se distribuem e quais são mais predominantes para cada ator.

## Ingestão de Dados Adicionais

Para garantir a consistência e a precisão dos dados analisados, foi realizada a ingestão de uma base de dados adicional:

- **Base de Dados:** [`actors.csv`](DesafioFinal/Etapa_Vizualizacao/actors.csv)
- **Fonte:** Kaggle
- **Objetivo:** Esta base de dados foi utilizada para comparar e confirmar a consistência das informações dos filmes estrelados por Adam Sandler e Jim Carrey, validando a integridade dos dados analisados.

## Considerações Finais

Este dashboard proporciona uma análise abrangente dos filmes estrelados por Adam Sandler e Jim Carrey, cobrindo aspectos de popularidade, desempenho financeiro e tendências ao longo do tempo. As visualizações ajudam a compreender melhor as dinâmicas de mercado e a influência de cada ator na indústria cinematográfica.

A inclusão da base de dados [actors.csv](DesafioFinal/Etapa_Vizualizacao/actors.csv) reforça a precisão dos dados e assegura que as análises são consistentes e confiáveis.

Para uma análise mais aprofundada, consulte os gráficos.[Dashboard](DesafioFinal/Etapa_Vizualizacao/Analise_Comparativa.png)

## Resultado Final: 

A solução completa oferece uma análise dinâmica e interativa dos dados de filmes, utilizando a infraestrutura da AWS para garantir escalabilidade, processamento eficiente e visualização em tempo real. O projeto combina automação, integração de APIs, e ferramentas de dados da AWS para criar uma plataforma robusta de visualização e insights.

## Scripts, Evidências de Execução e Correções


### 1. [Etapa Raw](DesafioFinal/Etapa_Raw_Nova_Extracao_TMDB)
### 2. [Etapa Trusted](DesafioFinal/Etapa_Trusted_Corrigida)
### 3. [Etapa Refined](DesafioFinal/Etapa_Refined_Corrigida)
### 4. [Etapa de Vizualização](DesafioFinal/Etapa_Vizualizacao)
### 5. [PDF do Dasboard](DesafioFinal/Etapa_Vizualizacao/Dashboard_Analise.pdf)
### 6. [Base de Dados Adicional](DesafioFinal/Etapa_Vizualizacao/actors.csv)
