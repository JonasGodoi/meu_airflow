# Pipeline de Dados de Trânsito com Apache Airflow

Projeto desenvolvido como **Trabalho de Conclusão de Curso (TCC)** do curso de **Engenharia de Computação da Universidade Estadual de Ponta Grossa (UEPG)**.

O projeto consiste no desenvolvimento de pipelines de dados para processamento de informações relacionadas ao trânsito, utilizando **Python, Apache Airflow, Pandas, GeoPandas e MySQL**.

O objetivo é automatizar etapas de **extração, transformação e carregamento (ETL)** de dados provenientes de fontes públicas, organizando o processamento por meio de workflows orquestrados pelo Apache Airflow.

---

## 📌 Sobre o projeto

O projeto foi desenvolvido para estruturar um processo automatizado de Engenharia de Dados aplicado a dados públicos relacionados ao trânsito.

Foram desenvolvidos pipelines para diferentes fontes de dados, incluindo:

- **SIGESGUARDA** — dados de ocorrências;
- **IPPUC** — dados geográficos relacionados ao sistema viário.

Os dados passam por etapas de extração, tratamento, transformação, validação e carregamento em arquivos e banco de dados.

O **Apache Airflow** é utilizado para realizar a orquestração dos processos através de DAGs (Directed Acyclic Graphs).

---

## 🏗️ Arquitetura do projeto

O fluxo geral do projeto pode ser representado da seguinte forma:

```text
                    FONTES DE DADOS
                          │
              ┌───────────┴───────────┐
              │                       │
              ▼                       ▼
        SIGESGUARDA                 IPPUC
       Dados de                    Sistema
       ocorrências                 viário
              │                       │
              ▼                       ▼
        ┌─────────────────────────────────┐
        │          Apache Airflow         │
        │              DAG                │
        └───────────────┬─────────────────┘
                        │
              ┌─────────┴─────────┐
              │                   │
              ▼                   ▼
        Pipeline              Pipeline
        SIGESGUARDA             IPPUC
              │                   │
              ▼                   ▼
        Pandas              GeoPandas
              │                   │
              ▼                   ▼
      Tratamento de        Processamento
      datas e textos       geográfico
              │                   │
              └─────────┬─────────┘
                        ▼
                Dados transformados
                        │
              ┌─────────┴─────────┐
              │                   │
              ▼                   ▼
            CSV                 MySQL

Processos ETL
SIGESGUARDA

O pipeline do SIGESGUARDA realiza o processamento de dados de ocorrências.

Extração

Os dados são carregados a partir de um arquivo CSV contendo registros de ocorrências.

Transformação

Durante o processamento são realizadas etapas como:

conversão de campos de data e hora;
criação de atributos relacionados à data;
criação de ano, mês, dia, hora e minuto;
identificação do dia da semana;
seleção das colunas relevantes;
tratamento de valores ausentes;
normalização de textos;
remoção de acentos;
organização das informações para carregamento.

Entre os atributos temporais gerados estão:
OCORRENCIA_TIMESTAMP
ANO
MES
DIA
HORA
MINUTO
NOME_DIA_SEMANA
DIA_DA_SEMANA_NUM

Load
Após o processamento, os dados são:
salvos em um arquivo CSV transformado;
carregados em uma tabela ocorrencias no banco de dados MySQL.

Pipeline IPPUC

O segundo pipeline utiliza dados geográficos do IPPUC, relacionados ao sistema viário.

Extração

Os dados são obtidos a partir de um arquivo Shapefile.

O processamento utiliza a biblioteca GeoPandas para leitura e manipulação dos dados geográficos.

Transformação

Durante o processo são realizadas etapas como:

seleção das informações relevantes;
renomeação de colunas;
tratamento dos dados geográficos;
conversão da geometria para o formato WKT (Well-Known Text).

As informações utilizadas incluem:
NMVIA
SIST_VIARI
SIST_VIA_1
geometry
Após a transformação, os campos são organizados como:
nome_via
sistema_viario
hierarquia_via
geometry

Load

Os dados transformados são:

exportados para um arquivo CSV;
carregados na tabela sistema_viario do banco de dados MySQL.
Orquestração com Apache Airflow

O Apache Airflow é utilizado para automatizar e orquestrar os pipelines do projeto.

A DAG principal do projeto é:
pipeline_dados_transito_tcc

Ela possui duas tarefas principais:
processar_sigesguarda
        │
        ▼
   sigesguarda.py


processar_ippuc
        │
        ▼
      ippuc.py
As tarefas utilizam BashOperator para executar os respectivos scripts Python.

A DAG está configurada para execução automática através de uma expressão cron.

🐳 Ambiente Docker

O projeto utiliza Docker Compose para estruturar o ambiente de execução do Apache Airflow.

A configuração utiliza:

Apache Airflow 2.9.2;
CeleryExecutor;
PostgreSQL;
Redis;
Docker.

O PostgreSQL é utilizado pelo ambiente do Airflow e o Redis atua como broker do CeleryExecutor.

O ambiente possui serviços separados para:

Airflow Webserver;
Airflow Scheduler;
Airflow Worker;
Airflow Triggerer;
Airflow Init.

A interface web do Airflow é disponibilizada na porta:http://localhost:8080

Tecnologias utilizadas
Linguagem
Python
Engenharia de Dados
Apache Airflow
ETL
Pipelines de dados
Orquestração de workflows
Processamento de dados
Pandas
GeoPandas
Dados geográficos
Shapefile
WKT
GeoDataFrame
Banco de dados
MySQL
PostgreSQL
Infraestrutura
Docker
Docker Compose
Redis
Celery
 Estrutura do projeto
meu_airflow/
│
├── dados_tcc/
│   ├── brutos/
│   │   ├── ippuc/
│   │   └── sigesguarda/
│   │
│   └── transformados/
│       ├── ippuc/
│       └── sigesguarda/
│
├── dags/
│   └── tcc_pipeline_dag.py
│
├── pipelines/
│   ├── ippuc.py
│   └── sigesguarda.py
│
├── logs/
│
└── docker-compose.yaml

Pré-requisitos

Para executar o projeto localmente, é necessário ter instalado:

Docker
Docker Compose
Git

Também é recomendado disponibilizar pelo menos 4 GB de memória para o Docker.
Clonar o repositório
git clone https://github.com/JonasGodoi/meu_airflow.git
Entre na pasta do projeto:
cd meu_airflow
Iniciar o ambiente
Execute:
docker compose up
Após a inicialização dos containers, acesse:
http://localhost:8080
Fluxo dos dados

De forma simplificada, o projeto segue o seguinte fluxo:
                    ┌─────────────────┐
                    │ Dados públicos  │
                    └────────┬────────┘
                             │
                             ▼
                    ┌─────────────────┐
                    │    Extração     │
                    └────────┬────────┘
                             │
                             ▼
                    ┌─────────────────┐
                    │ Processamento   │
                    │    Python       │
                    └────────┬────────┘
                             │
                             ▼
                    ┌─────────────────┐
                    │  Transformação  │
                    └────────┬────────┘
                             │
                             ▼
                    ┌─────────────────┐
                    │    Validação    │
                    └────────┬────────┘
                             │
                             ▼
                    ┌─────────────────┐
                    │      Load       │
                    └───────┬─┬───────┘
                            │ │
                   ┌────────┘ └────────┐
                   ▼                   ▼
                 CSV                  MySQL

Contexto acadêmico

Este projeto foi desenvolvido como Trabalho de Conclusão de Curso (TCC) durante a graduação em Engenharia de Computação na Universidade Estadual de Ponta Grossa (UEPG).

O projeto aplica conceitos de:

Engenharia de Dados;
ETL;
processamento de dados;
automação;
orquestração de pipelines;
bancos de dados;
processamento de dados geográficos.

O trabalho também resultou na publicação científica:

Construção de um pipeline ETL para análise de dados de tráfego utilizando fontes públicas

Autor

Jonas de Godoi

Engenharia de Computação — UEPG

GitHub:
https://github.com/JonasGodoi

LinkedIn:
https://www.linkedin.com/in/jonas-godoi-324220225/

📌 Observações

Este projeto foi desenvolvido para fins acadêmicos e de pesquisa.

O ambiente Docker utilizado no projeto é baseado em uma configuração de desenvolvimento do Apache Airflow e não deve ser considerado uma configuração pronta para produção.

