import pandas as pd
import os
from sqlalchemy import create_engine, text

def importar_ceps_fixo():
    CAMINHO_FIXO = r"C:\Users\ivantp\Downloads\cep_dne_extraido\Fixo"

    localidades_widths = [1, 2, 2, 6, 8, 72, 8, 36, 1, 1, 6, 8, 3, 7]
    localidades_cols = [
        "tipo_reg", "pais", "uf", "sep1", "chave_localidade",
        "nome_localidade", "cep_localidade", "abreviatura_localidade",
        "tipo_localidade", "situacao", "sep2", "chave_municipio",
        "dr_ect", "codigo_ibge"
    ]

    localidades_df = pd.read_fwf(
        os.path.join(CAMINHO_FIXO, "DNE_GU_LOCALIDADES.TXT"),
        widths=localidades_widths,
        names=localidades_cols,
        encoding="cp1252"
    )
    localidades_df = localidades_df[localidades_df["tipo_reg"] == "D"]
    ibge_map = {row["chave_localidade"]: row["codigo_ibge"] for _, row in localidades_df.iterrows()}

    ufs = ["AC","AL","AM","AP","BA","CE","DF","ES","GO","MA","MG","MS","MT",
           "PA","PB","PE","PI","PR","RJ","RN","RO","RR","RS","SC","SE","SP","TO"]

    registros = []

    def limpa_texto(campo):
        return campo.strip() if campo else ""

    def formata_cep(cep):
        cep = cep.strip()
        return cep.zfill(8) if cep else ""

    for uf in ufs:
        file_name = f"DNE_GU_{uf}_LOGRADOUROS.TXT"
        path = os.path.join(CAMINHO_FIXO, file_name)
        if not os.path.exists(path):
            print(f"Arquivo não encontrado: {path}")
            continue

        with open(path, "r", encoding="cp1252") as f:
            current_log = None
            for line in f:
                line = line.rstrip("\r\n")
                if not line or line[0] == "C":
                    continue
                tipo = line[0]
                uf_code = line[1:3]
                chave_localidade = line[9:17]
                nome_localidade = line[17:89]
                chave_bairro_ini = line[94:102]
                bairro_ini = line[102:174]
                chave_bairro_fim = line[179:187]
                bairro_fim = line[187:259]
                tipo_logr = line[259:285]
                prep_logr = line[285:288]
                titulo_logr = line[288:360]
                nome_logradouro = line[374:446]
                cep_field = line[518:526]
                paridade = line[549:550].strip()

                tipo_logr = limpa_texto(tipo_logr)
                prep_logr = limpa_texto(prep_logr)
                titulo_logr = limpa_texto(titulo_logr)
                nome_logradouro = limpa_texto(nome_logradouro)

                if titulo_logr:
                    nome_completo_log = f"{tipo_logr} {titulo_logr} {nome_logradouro}"
                else:
                    nome_completo_log = f"{tipo_logr} {nome_logradouro}"
                if prep_logr:
                    nome_completo_log = f"{tipo_logr} {prep_logr} {titulo_logr or ''} {nome_logradouro}".replace("  ", " ")

                bairro_ini = limpa_texto(bairro_ini)
                bairro_fim = limpa_texto(bairro_fim)
                bairro_nome = f"{bairro_ini} / {bairro_fim}" if bairro_ini and bairro_fim and bairro_ini != bairro_fim else bairro_ini or bairro_fim
                cidade_nome = limpa_texto(nome_localidade)
                codigo_ibge = ibge_map.get(chave_localidade, "")

                if tipo == "D":
                    current_log = {
                        "uf": uf_code.strip().upper(),
                        "cidade": cidade_nome,
                        "bairro": bairro_nome,
                        "logradouro": nome_completo_log.strip(),
                        "cep": formata_cep(cep_field),
                        "ibge": codigo_ibge
                    }
                    if current_log["cep"]:
                        registros.append(current_log)
                        current_log = None
                elif tipo == "S" and current_log:
                    cep_especifico = formata_cep(line[518:526])
                    num_ini = limpa_texto(line[527:538])
                    num_fim = limpa_texto(line[538:549])
                    complemento_faixa = ""
                    if num_ini:
                        complemento_faixa = f"Numeração {num_ini}"
                        if num_fim:
                            complemento_faixa += f" até {num_fim}"
                        if paridade:
                            par_dict = {"P": "lado Par", "I": "lado Ímpar", "A": "ambos os lados"}
                            complemento_faixa += f" ({par_dict.get(paridade, paridade)})"
                    registro = {
                        "uf": current_log["uf"],
                        "cidade": current_log["cidade"],
                        "bairro": current_log["bairro"],
                        "logradouro": current_log["logradouro"] + (f" {complemento_faixa}" if complemento_faixa else ""),
                        "cep": cep_especifico,
                        "ibge": current_log["ibge"]
                    }
                    registros.append(registro)

    df_ceps = pd.DataFrame(registros)
    df_ceps["cep"] = df_ceps["cep"].astype(str).str.zfill(8)
    df_ceps["uf"] = df_ceps["uf"].str.upper().str.strip()
    df_ceps["cidade"] = df_ceps["cidade"].str.title().str.strip()
    df_ceps["bairro"] = df_ceps["bairro"].str.title().str.strip()
    df_ceps["logradouro"] = df_ceps["logradouro"].str.strip()
    df_ceps["origem"] = "FIXO"
    df_ceps = df_ceps.drop_duplicates(subset=["cep", "logradouro"])

    engine = create_engine("postgresql+psycopg2://postgres:esus@localhost:5432/esus")

    # Cria a tabela com tipagem adequada
    with engine.connect() as conn:
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS correios_ceps (
                cep VARCHAR(8),
                logradouro TEXT,
                bairro TEXT,
                cidade TEXT,
                uf CHAR(2),
                ibge VARCHAR(10),
                origem TEXT,
                PRIMARY KEY (cep, logradouro)
            );
        """))

    # Insere incrementalmente, evitando duplicatas
    with engine.connect() as conn:
        for _, row in df_ceps.iterrows():
            insert_query = text("""
                INSERT INTO correios_ceps (cep, logradouro, bairro, cidade, uf, ibge, origem)
                VALUES (:cep, :logradouro, :bairro, :cidade, :uf, :ibge, :origem)
                ON CONFLICT (cep, logradouro) DO NOTHING
            """)
            conn.execute(insert_query, row.to_dict())

    print("✅ Importação Fixo incremental finalizada. Total registros processados:", len(df_ceps))
