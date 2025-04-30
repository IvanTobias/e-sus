import pandas as pd
import os
from sqlalchemy import create_engine, text
import glob

# Caminho para os arquivos extraídos do DNE
CAMINHO_BASE = r"C:\Users\ivantp\Downloads\cep_dne_extraido\Delimitado"

# Conexão com seu banco PostgreSQL
engine = create_engine("postgresql+psycopg2://postgres:esus@localhost:5432/esus")

print("📄 Lendo todos os arquivos de logradouro...")
arquivos_logradouro = glob.glob(os.path.join(CAMINHO_BASE, "LOG_LOGRADOURO_*.TXT"))
print(f"📁 Total de arquivos lidos: {len(arquivos_logradouro)}")
frames = []
for arquivo in arquivos_logradouro:
    print(f"➕ Lendo {os.path.basename(arquivo)}")
    df = pd.read_csv(arquivo, sep="@", encoding="latin1", header=None)
    frames.append(df)
logradouro_df = pd.concat(frames, ignore_index=True)
print(f"📊 logradouro_df.columns count: {logradouro_df.shape[1]}")

print("🔁 Lendo arquivos complementares...")
localidade_df = pd.read_csv(os.path.join(CAMINHO_BASE, "LOG_LOCALIDADE.TXT"), sep="@", encoding="latin1", header=None)
print(localidade_df.head(3))
print(f"📊 localidade_df.columns count: {localidade_df.shape[1]}")

bairro_df = pd.read_csv(os.path.join(CAMINHO_BASE, "LOG_BAIRRO.TXT"), sep="@", encoding="latin1", header=None)
grande_usuario_df = pd.read_csv(os.path.join(CAMINHO_BASE, "LOG_GRANDE_USUARIO.TXT"), sep="@", encoding="latin1", header=None)
cpc_df = pd.read_csv(os.path.join(CAMINHO_BASE, "LOG_CPC.TXT"), sep="@", encoding="latin1", header=None)

# Nomeando colunas
logradouro_df.columns = ["id_logradouro", "tipo_logradouro", "titulo_logradouro", "nome_logradouro", "complemento", "bairro_ini", "bairro_fim", "id_localidade_ini", "id_localidade_fim", "cep", "id_tipo_logradouro"]
localidade_df.columns = [
    "id_localidade", "uf", "nome_municipio",
    "cep_localidade", "situacao", "tipo_localidade",
    "id_localidade_sub", "nome_abreviado", "codigo_ibge"]
bairro_df.columns = [
    "id_bairro", "uf", "id_localidade", "nome_bairro", "nome_abreviado"]
grande_usuario_df.columns = [
    "id_grande_usuario", "uf", "id_localidade", "id_bairro", "id_logradouro",
    "nome_grande_usuario", "endereco", "cep", "nome_abreviado"]
cpc_df.columns = ["id_cpc", "nome_cpc", "endereco", "complemento", "cep", "id_localidade"]

# Merge logradouro principal
print("🧰 Unificando dados principais...")

cep_df = logradouro_df.merge(localidade_df, left_on="id_localidade_ini", right_on="id_localidade", suffixes=("", "_loc"))
print(f"🔗 Registros após merge com localidade: {len(cep_df)}")
cep_df = cep_df.merge(bairro_df, left_on="bairro_ini", right_on="id_bairro", how="left")
cep_df["logradouro"] = (
    cep_df["titulo_logradouro"].astype(str).fillna("") + " " +
    cep_df["nome_logradouro"].astype(str).fillna("")
).str.strip()
print("🧪 Colunas disponíveis no cep_df:")
print(cep_df.columns.tolist())
print("📌 Preview do DataFrame final:")
print(cep_df.head(5))
print(f"📊 Total final no DataFrame: {len(cep_df)}")
cep_df = cep_df[["cep", "logradouro", "nome_bairro", "nome_municipio", "uf_x", "codigo_ibge"]]
cep_df.columns = ["cep", "logradouro", "bairro", "cidade", "uf", "ibge"]
cep_df["origem"] = "LOGRADOURO"
print("🔍 Nulos no CEP:", cep_df["cep"].isna().sum())
print("📉 Duplicatas:", cep_df.duplicated(subset=["cep", "logradouro"]).sum())

# Inclui grandes usuários
print("👥 Incluindo grandes usuários...")
if not grande_usuario_df.empty:
    grandes_df = grande_usuario_df.merge(localidade_df, on="id_localidade")
    grandes_df = grandes_df.merge(bairro_df, on=["id_localidade", "id_bairro"], how="left")
    grandes_df["logradouro"] = grandes_df["nome_grande_usuario"].fillna("")
    grandes_df = grandes_df[["cep", "logradouro", "nome_bairro", "nome_municipio", "uf", "codigo_ibge"]]
    grandes_df.columns = ["cep", "logradouro", "bairro", "cidade", "uf", "ibge"]
    grandes_df["origem"] = "GRANDE_USUARIO"
    cep_df = pd.concat([cep_df, grandes_df], ignore_index=True)

# Inclui CPC
print("🛋️ Incluindo CPCs...")
if not cpc_df.empty:
    cpc_merge = cpc_df.merge(localidade_df, on="id_localidade")
    cpc_merge["logradouro"] = cpc_merge["nome_cpc"].fillna("")
    cpc_merge = cpc_merge[["cep", "logradouro", "nome_municipio", "uf", "codigo_ibge"]]
    cpc_merge.insert(2, "bairro", "")
    cpc_merge.columns = ["cep", "logradouro", "bairro", "cidade", "uf", "ibge"]
    cpc_merge["origem"] = "CPC"
    cep_df = pd.concat([cep_df, cpc_merge], ignore_index=True)

# Limpa e ajusta
cep_df = cep_df.dropna(subset=["cep"])
cep_df["cep"] = cep_df["cep"].astype(str).str.zfill(8)
cep_df = cep_df.drop_duplicates(subset=["cep", "logradouro"])

# Cria tabela no banco de dados
print("📂 Criando tabela 'correios_ceps' no banco PostgreSQL...")
with engine.connect() as conn:
    conn.execute(text("""
        DROP TABLE IF EXISTS correios_ceps;
        CREATE TABLE correios_ceps (
            cep VARCHAR(8) PRIMARY KEY,
            logradouro TEXT,
            bairro TEXT,
            cidade TEXT,
            uf CHAR(2),
            ibge VARCHAR(10),
            origem TEXT
        );
    """))

# Insere os dados no banco
print(f"📊 Inserindo {len(cep_df)} registros...")
cep_df.to_sql("correios_ceps", engine, if_exists="append", index=False)
print("\u2705 Importação finalizada com sucesso!")