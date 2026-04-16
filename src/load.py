from sqlalchemy import create_engine
import pandas as pd
import os
from dotenv import load_dotenv
from pathlib import Path

# Diretórios
BASE_DIR = Path(__file__).resolve().parent.parent
TRUSTED_DIR = BASE_DIR / "data" / "trusted"
ENV_PATH = BASE_DIR / ".env"

# Carregar variáveis de ambiente
load_dotenv(dotenv_path=ENV_PATH)

DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")
DB_NAME = os.getenv("DB_NAME")

# Conexão com o banco
connection_string = (
    f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
)
engine = create_engine(connection_string)

SCHEMA = "analytics"

def load_table(file_name, table_name):
    file_path = TRUSTED_DIR / file_name
    df = pd.read_parquet(file_path)

    print(f"📤 Carregando dados para {SCHEMA}.{table_name}...")
    df.to_sql(
        name=table_name,
        con=engine,
        schema=SCHEMA,
        if_exists="append",  # Pode ser alterado para 'replace' em cargas completas
        index=False,
        method="multi"
    )
    print(f"✅ Dados carregados em {SCHEMA}.{table_name}.")

def main():
    print("🚀 Iniciando processo de carga...\n")

    load_table("clientes.parquet", "dim_cliente")
    load_table("produtos.parquet", "dim_produto")
    load_table("tempo.parquet", "dim_tempo")
    load_table("fato_vendas.parquet", "fato_vendas")

    print("\n🎉 Processo de carga finalizado com sucesso!")

if __name__ == "__main__":
    main()