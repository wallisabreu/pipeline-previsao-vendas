# 🚀 Pipeline de Dados - Previsão de Vendas

## 📌 Descrição
Este projeto implementa um pipeline completo de dados utilizando **Python** e **PostgreSQL**, seguindo a arquitetura em camadas **Raw**, **Trusted** e **Analytics**. O objetivo é demonstrar habilidades em engenharia de dados, incluindo extração, transformação e carga (ETL), garantindo organização, qualidade e disponibilidade dos dados para análises futuras.

O pipeline realiza a extração de dados de um banco transacional, aplica transformações para limpeza e padronização e, por fim, carrega os dados em um modelo analítico no schema `analytics` do PostgreSQL.

## 🎯 Objetivos
- Extrair dados de um banco PostgreSQL.
- Realizar limpeza e padronização dos dados.
- Implementar uma arquitetura de dados em camadas (**Raw → Trusted → Analytics**).
- Carregar os dados tratados em um ambiente analítico.
- Demonstrar boas práticas de engenharia de dados e versionamento com GitHub.

## 🛠️ Tecnologias Utilizadas
- **Python** – Linguagem principal do pipeline.
- **Pandas** – Manipulação e transformação de dados.
- **SQLAlchemy** – Conexão entre Python e PostgreSQL.
- **PostgreSQL** – Banco de dados relacional.
- **python-dotenv** – Gerenciamento de variáveis de ambiente.
- **PyArrow** – Armazenamento eficiente de dados em formato Parquet.
- **Git & GitHub** – Versionamento e documentação do projeto.

## 🏗️ Arquitetura de Dados

Sistema Transacional (Schema: public) → Raw → Trusted → Analytics

### 🔹 Camadas

| Camada | Descrição |
|-------|-----------|
| **Raw** | Dados brutos extraídos do sistema transacional e armazenados em arquivos CSV. |
| **Trusted** | Dados tratados e padronizados, armazenados em formato Parquet. |
| **Analytics** | Dados carregados no schema `analytics` do PostgreSQL para consumo analítico. |

## 🔄 Processo ETL

### 📥 Extract
- **Script:** `src/extract.py`
- **Origem:** Banco PostgreSQL (schema `public`)
- **Ações:**
  - Conexão com o banco de dados.
  - Extração das tabelas `clientes`, `produtos`, `tempo` e `vendas`.
  - Armazenamento dos dados na camada **Raw** em formato CSV.

### 🔄 Transform
- **Script:** `src/transform.py`
- **Ações:**
  - Padronização dos nomes das colunas.
  - Conversão de tipos de dados (datas e valores numéricos).
  - Tratamento de valores nulos.
  - Remoção de duplicidades.
  - Armazenamento dos dados na camada **Trusted** em formato Parquet.

### 📤 Load
- **Script:** `src/load.py`
- **Destino:** Schema `analytics` no PostgreSQL.
- **Ações:**
  - Leitura dos arquivos Parquet da camada Trusted.
  - Carga dos dados nas tabelas analíticas:
    - `analytics.dim_cliente`
    - `analytics.dim_produto`
    - `analytics.dim_tempo`
    - `analytics.fato_vendas`

## 📊 Estrutura das Tabelas Analíticas

### 🔹 dim_cliente
| Campo | Descrição |
|------|-----------|
| id_cliente | Identificador do cliente |
| nome_cliente | Nome do cliente |
| email | E-mail do cliente |
| data_cadastro | Data de cadastro |
| status | Situação do cliente |

### 🔹 dim_produto
| Campo | Descrição |
|------|-----------|
| id_produto | Identificador do produto |
| nome_produto | Nome do produto |
| categoria | Categoria do produto |
| preco | Preço do produto |
| status | Situação do produto |

### 🔹 dim_tempo
| Campo | Descrição |
|------|-----------|
| id_tempo | Identificador da data |
| data_venda | Data da venda |
| ano | Ano |
| mes | Mês |

### 🔹 fato_vendas
| Campo | Descrição |
|------|-----------|
| id_venda | Identificador da venda |
| id_cliente | Chave estrangeira para `dim_cliente` |
| id_produto | Chave estrangeira para `dim_produto` |
| id_tempo | Chave estrangeira para `dim_tempo` |
| quantidade | Quantidade vendida |
| valor_unit | Valor unitário |
| valor_total | Valor total da venda |

## 📁 Estrutura do Projeto
