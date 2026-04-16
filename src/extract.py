"""
extract.py
-----------
Responsável pela extração de dados do banco PostgreSQL e
armazenamento na camada RAW do pipeline de dados.
"""

from sqlalchemy import create_engine, text
import pandas as pd
import os
from dotenv import load_dotenv
from pathlib import Path
from datetime import datetime

# =========================
# 1. Definir diretórios
# =========================
BASE_DIR = Path(__file__).resolve().parent.parent
ENV_PATH = BASE_DIR / ".env"
RAW_DATA_DIR = BASE_DIR / "data" / "raw"

# Criar diretório raw caso não exista
RAW_DATA_DIR.mkdir(parents=True, exist_ok=True)

# =========================
# 2. Carregar variáveis de ambiente
# =========================
if not ENV_PATH.exists():
    raise FileNotFoundError("❌ Arquivo .env não encontrado na raiz do projeto.")

load_dotenv(dotenv_path=ENV_PATH)

DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")
DB_NAME = os.getenv("DB_NAME")

# Validar variáveis
if not all([DB_USER, DB_PASSWORD, DB_HOST, DB_PORT, DB_NAME]):
    raise ValueError("❌ Alguma variável de ambiente não foi carregada. Verifique o arquivo .env.")

# =========================
# 3. Criar conexão com o banco
# =========================
connection_string = (
    f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
)

engine = create_engine(connection_string)

# =========================
# 4. Definir consultas SQL
# =========================
queries = {
    "clientes": "SELECT * FROM analytics.dim_cliente;",
    "produtos": "SELECT * FROM analytics.dim_produto;",
    "tempo": "SELECT * FROM analytics.dim_tempo;",
    "fato_vendas": "SELECT * FROM analytics.fato_vendas;"
}

# =========================
# 5. Função de extração
# =========================
def extract_and_save(table_name: str, query: str) -> None:
    """
    Extrai dados do banco e salva em CSV na camada RAW.

    :param table_name: Nome da tabela/arquivo.
    :param query: Consulta SQL a ser executada.
    """
    try:
        print(f"📥 Extraindo dados da tabela: {table_name}")

        # Executar consulta
        df = pd.read_sql(text(query), engine)

        # Nome do arquivo com timestamp
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        file_path = RAW_DATA_DIR / f"{table_name}_{timestamp}.csv"

        # Salvar arquivo CSV
        df.to_csv(file_path, index=False, sep=";")

        print(f"✅ {table_name}: {len(df)} registros extraídos.")
        print(f"💾 Arquivo salvo em: {file_path}\n")

    except Exception as e:
        print(f"❌ Erro ao extrair {table_name}: {e}")

# =========================
# 6. Execução principal
# =========================
def main():
    print("🚀 Iniciando processo de extração de dados...\n")

    for table_name, query in queries.items():
        extract_and_save(table_name, query)

    print("🎉 Processo de extração finalizado com sucesso!")

# =========================
# 7. Ponto de entrada
# =========================
if __name__ == "__main__":
    main()