import pandas as pd
from pathlib import Path

# Diretórios
BASE_DIR = Path(__file__).resolve().parent.parent
RAW_DIR = BASE_DIR / "data" / "raw"
TRUSTED_DIR = BASE_DIR / "data" / "trusted"
TRUSTED_DIR.mkdir(parents=True, exist_ok=True)

def get_latest_file(prefix):
    files = list(RAW_DIR.glob(f"{prefix}_*.csv"))
    if not files:
        raise FileNotFoundError(f"Nenhum arquivo encontrado para {prefix}")
    return max(files, key=lambda x: x.stat().st_mtime)

def standardize_columns(df):
    df.columns = (
        df.columns
        .str.strip()
        .str.lower()
        .str.replace(" ", "_")
    )
    return df

# -------------------------
# Transformação: Clientes
# -------------------------
def transform_clientes():
    df = pd.read_csv(get_latest_file("clientes"), sep=";")
    df = standardize_columns(df)

    if "data_cadastro" in df.columns:
        df["data_cadastro"] = pd.to_datetime(
            df["data_cadastro"], errors="coerce", dayfirst=True
        )

    df = df.drop_duplicates()
    df.to_parquet(TRUSTED_DIR / "clientes.parquet", index=False, engine="pyarrow")
    print("✅ Clientes transformados.")

# -------------------------
# Transformação: Produtos
# -------------------------
def transform_produtos():
    df = pd.read_csv(get_latest_file("produtos"), sep=";")
    df = standardize_columns(df)

    if "preco" in df.columns:
        df["preco"] = pd.to_numeric(df["preco"], errors="coerce")

    df = df.drop_duplicates()
    df.to_parquet(TRUSTED_DIR / "produtos.parquet", index=False, engine="pyarrow")
    print("✅ Produtos transformados.")

# -------------------------
# Transformação: Tempo
# -------------------------
def transform_tempo():
    df = pd.read_csv(get_latest_file("tempo"), sep=";")
    df = standardize_columns(df)

    if "data_venda" in df.columns:
        df["data_venda"] = pd.to_datetime(
            df["data_venda"], errors="coerce", dayfirst=True
        )

    if "ano" in df.columns:
        df["ano"] = pd.to_numeric(df["ano"], errors="coerce").astype("Int64")

    if "mes" in df.columns:
        df["mes"] = pd.to_numeric(df["mes"], errors="coerce").astype("Int64")

    df = df.drop_duplicates()
    df.to_parquet(TRUSTED_DIR / "tempo.parquet", index=False, engine="pyarrow")
    print("✅ Tempo transformado.")

# -------------------------
# Transformação: Fato Vendas
# -------------------------
def transform_fato_vendas():
    df = pd.read_csv(get_latest_file("fato_vendas"), sep=";")
    df = standardize_columns(df)

    if "data_venda" in df.columns:
        df["data_venda"] = pd.to_datetime(
            df["data_venda"], errors="coerce", dayfirst=True
        )

    numeric_cols = ["quantidade", "valor_unit", "valor_total"]
    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    df = df.drop_duplicates()
    df.to_parquet(TRUSTED_DIR / "fato_vendas.parquet", index=False, engine="pyarrow")
    print("✅ Fato Vendas transformado.")

# -------------------------
# Execução Principal
# -------------------------
def main():
    print("🚀 Iniciando transformação dos dados...\n")
    transform_clientes()
    transform_produtos()
    transform_tempo()
    transform_fato_vendas()
    print("\n🎉 Processo de transformação finalizado com sucesso!")

if __name__ == "__main__":
    main()