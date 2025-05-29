import pandas as pd
import os
import glob
import logging
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s', encoding='utf-8')

def populate_correios_ceps_if_empty(db_uri, dne_data_path):
    """
    Populates the 'correios_ceps' table with data from DNE files if the table is empty.
    The 'correios_ceps' table is expected to be created by database_setup.py.
    """
    logging.info(f"Initiating CEP data population. DNE Path: {dne_data_path}")

    try:
        engine = create_engine(db_uri)
        with engine.connect() as connection:
            # This is Step 5: Check if Table is Empty
            try:
                result = connection.execute(text("SELECT COUNT(*) FROM correios_ceps")).scalar_one_or_none()
                if result is not None and result > 0:
                    logging.info("Tabela correios_ceps já populada. Pulando importação.")
                    return
                logging.info("Tabela correios_ceps está vazia ou não contém dados. Prosseguindo com a importação.")
            except SQLAlchemyError as e:
                logging.warning(f"Erro ao verificar contagem da tabela correios_ceps (pode não existir ainda, será criada por database_setup.py): {e}. Prosseguindo com a tentativa de importação.")

            # Step 6: Data Path (already used as dne_data_path parameter)
            # CAMINHO_BASE = dne_data_path (implicit)

            # Step 7: Table Creation (removed - handled by database_setup.py)
            # The DROP TABLE and CREATE TABLE block is intentionally omitted here.

            # The rest of the original script's logic (file reading, processing, insertion)
            # will be added in subsequent steps, wrapped in try-except blocks.
            # For now, this function only sets up the connection and checks table emptiness.

            logging.info("📄 Lendo todos os arquivos de logradouro...")
            arquivos_logradouro = glob.glob(os.path.join(dne_data_path, "LOG_LOGRADOURO_*.TXT"))
            if not arquivos_logradouro:
                logging.error(f"Nenhum arquivo LOG_LOGRADOURO_*.TXT encontrado em {dne_data_path}. Abortando importação de CEPs.")
                return

            logging.info(f"📁 Total de arquivos de logradouro lidos: {len(arquivos_logradouro)}")
            frames = []
            for arquivo in arquivos_logradouro:
                logging.info(f"➕ Lendo {os.path.basename(arquivo)}")
                try:
                    df = pd.read_csv(arquivo, sep="@", encoding="latin1", header=None, dtype=str)
                    frames.append(df)
                except FileNotFoundError:
                    logging.error(f"Arquivo não encontrado: {arquivo}")
                    return
                except pd.errors.EmptyDataError:
                    logging.warning(f"Arquivo vazio: {arquivo}")
                    continue
                except Exception as e:
                    logging.error(f"Erro ao ler o arquivo {arquivo}: {e}")
                    return
            
            if not frames:
                logging.error("Nenhum dado de logradouro lido. Abortando.")
                return
                
            logradouro_df = pd.concat(frames, ignore_index=True)
            
            # Column mapping based on DNE documentation structure
            # LOG_LOGRADOURO: LOG_NU, UFE_SG, LOC_NU, LOG_NO, LOG_COMPLEMENTO, CEP, TLO_TX, LOG_STA_TLO, LOG_NO_ABREV, BAI_NU_INI, BAI_NU_FIM, TEMP
            if logradouro_df.shape[1] == 12:
                 logradouro_df.columns = ["LOG_NU", "UFE_SG", "LOC_NU", "LOG_NOME", "LOG_COMPLEMENTO", "CEP", "TLO_TX", "LOG_STATUS_TIPO_LOG", "LOG_NOME_ABREVIADO", "BAI_NU_INI", "BAI_NU_FIM", "TEMP"]
            else:
                logging.error(f"Número inesperado de colunas ({logradouro_df.shape[1]}) em arquivos LOG_LOGRADOURO. Esperado 12.")
                return

            logging.info("🔁 Lendo arquivos complementares...")
            try:
                localidade_df = pd.read_csv(os.path.join(dne_data_path, "LOG_LOCALIDADE.TXT"), sep="@", encoding="latin1", header=None, dtype=str)
                # LOG_LOCALIDADE: LOC_NU, UFE_SG, LOC_NO, MUN_NU (CEP in old DNE, not IBGE), LOC_IN_SIT, LOC_IN_TIPO, LOC_NU_SUB, LOC_NO_ABREV, MUN_NU (IBGE Code in newer DNE)
                # Assuming 9 columns as per DNE: LOC_NU, UFE_SG, LOC_NO, LOC_CEP, LOC_IN_SIT, LOC_IN_TIPO, LOC_NU_SUB, LOC_NO_ABREV, MUN_NU
                if localidade_df.shape[1] == 9:
                     localidade_df.columns = ["LOC_NU_loc", "UFE_SG_loc", "LOC_NO_loc", "CEP_loc", "LOC_IN_SIT_loc", "LOC_IN_TIPO_loc", "LOC_NU_SUB_loc", "LOC_NO_ABREV_loc", "MUN_NU_IBGE_loc"]
                else:
                    logging.error(f"Número inesperado de colunas ({localidade_df.shape[1]}) em LOG_LOCALIDADE.TXT. Esperado 9.")
                    return

                bairro_df = pd.read_csv(os.path.join(dne_data_path, "LOG_BAIRRO.TXT"), sep="@", encoding="latin1", header=None, dtype=str)
                # LOG_BAIRRO: BAI_NU, UFE_SG, LOC_NU, BAI_NO, BAI_NO_ABREV
                if bairro_df.shape[1] == 5:
                    bairro_df.columns = ["BAI_NU_bai", "UFE_SG_bai", "LOC_NU_bai", "BAI_NO_bai", "BAI_NO_ABREV_bai"]
                else:
                    logging.error(f"Número inesperado de colunas ({bairro_df.shape[1]}) em LOG_BAIRRO.TXT. Esperado 5.")
                    return
                    
                grande_usuario_df = pd.read_csv(os.path.join(dne_data_path, "LOG_GRANDE_USUARIO.TXT"), sep="@", encoding="latin1", header=None, dtype=str, keep_default_na=False, na_values=[''])
                # LOG_GRANDE_USUARIO: GRU_NU, UFE_SG, LOC_NU, BAI_NU, LOG_NU, GRU_NO, GRU_ENDERECO, CEP, GRU_NO_ABREV
                if not grande_usuario_df.empty and grande_usuario_df.shape[1] == 9:
                     grande_usuario_df.columns = ["GRU_NU_gu", "UFE_SG_gu", "LOC_NU_gu", "BAI_NU_gu", "LOG_NU_gu", "GRU_NO_gu", "GRU_ENDERECO_gu", "CEP_gu", "GRU_NO_ABREV_gu"]
                elif not grande_usuario_df.empty:
                    logging.error(f"Número inesperado de colunas ({grande_usuario_df.shape[1]}) em LOG_GRANDE_USUARIO.TXT. Esperado 9.")
                    return

                cpc_df = pd.read_csv(os.path.join(dne_data_path, "LOG_CPC.TXT"), sep="@", encoding="latin1", header=None, dtype=str, keep_default_na=False, na_values=[''])
                # LOG_CPC: CPC_NU, UFE_SG, LOC_NU, CPC_NO, CPC_ENDERECO, CEP
                if not cpc_df.empty and cpc_df.shape[1] == 6:
                    cpc_df.columns = ["CPC_NU_cpc", "UFE_SG_cpc", "LOC_NU_cpc", "CPC_NO_cpc", "CPC_ENDERECO_cpc", "CEP_cpc"]
                elif not cpc_df.empty:
                    logging.error(f"Número inesperado de colunas ({cpc_df.shape[1]}) em LOG_CPC.TXT. Esperado 6.")
                    return

            except FileNotFoundError as e:
                logging.error(f"Arquivo complementar não encontrado: {e}. Abortando.")
                return
            except pd.errors.EmptyDataError as e:
                logging.warning(f"Arquivo complementar vazio: {e}.") # Continue if possible
            except Exception as e:
                logging.error(f"Erro ao ler arquivos complementares: {e}")
                return

            # --- Data Processing (to be added in next step) ---
            logging.info("🧰 Unificando dados principais (LOGRADOURO)...")
            # Merge logradouro with localidade
            cep_df = logradouro_df.merge(localidade_df, left_on="LOC_NU", right_on="LOC_NU_loc", suffixes=("_log", "_loc"))
            # Merge with bairro
            cep_df = cep_df.merge(bairro_df, left_on=["LOC_NU", "BAI_NU_INI"], right_on=["LOC_NU_bai", "BAI_NU_bai"], how="left", suffixes=("_cep", "_bairro"))
            
            cep_df["logradouro"] = cep_df["TLO_TX"].fillna('') + " " + cep_df["LOG_NOME"].fillna('')
            cep_df["logradouro"] = cep_df["logradouro"].str.strip()

            # Select and rename columns for the final DataFrame structure
            cep_df_final_log = cep_df[["CEP", "logradouro", "BAI_NO_bai", "LOC_NO_loc", "UFE_SG", "MUN_NU_IBGE_loc"]]
            cep_df_final_log.columns = ["cep", "logradouro", "bairro", "cidade", "uf", "ibge"]
            cep_df_final_log.loc[:, "origem"] = "LOGRADOURO" # Use .loc to avoid SettingWithCopyWarning
            
            all_dfs = [cep_df_final_log]

            if not grande_usuario_df.empty:
                logging.info("👥 Incluindo grandes usuários...")
                gu_df = grande_usuario_df.merge(localidade_df, left_on="LOC_NU_gu", right_on="LOC_NU_loc", suffixes=("_gu", "_loc"))
                gu_df = gu_df.merge(bairro_df, left_on=["LOC_NU_gu", "BAI_NU_gu"], right_on=["LOC_NU_bai", "BAI_NU_bai"], how="left", suffixes=("_gu", "_bairro"))
                gu_df["logradouro"] = gu_df["GRU_NO_gu"].fillna('')
                gu_df_final = gu_df[["CEP_gu", "logradouro", "BAI_NO_bai", "LOC_NO_loc", "UFE_SG_gu", "MUN_NU_IBGE_loc"]]
                gu_df_final.columns = ["cep", "logradouro", "bairro", "cidade", "uf", "ibge"]
                gu_df_final.loc[:, "origem"] = "GRANDE_USUARIO"
                all_dfs.append(gu_df_final)

            if not cpc_df.empty:
                logging.info("🛋️ Incluindo CPCs...")
                cpc_m_df = cpc_df.merge(localidade_df, left_on="LOC_NU_cpc", right_on="LOC_NU_loc", suffixes=("_cpc", "_loc"))
                cpc_m_df["logradouro"] = cpc_m_df["CPC_NO_cpc"].fillna('')
                cpc_m_df["bairro"] = None # CPCs don't have bairro in DNE files
                cpc_df_final = cpc_m_df[["CEP_cpc", "logradouro", "bairro", "LOC_NO_loc", "UFE_SG_cpc", "MUN_NU_IBGE_loc"]]
                cpc_df_final.columns = ["cep", "logradouro", "bairro", "cidade", "uf", "ibge"]
                cpc_df_final.loc[:, "origem"] = "CPC"
                all_dfs.append(cpc_df_final)
            
            final_cep_df_to_insert = pd.concat(all_dfs, ignore_index=True)

            logging.info("🧹 Limpando e ajustando dados finais...")
            final_cep_df_to_insert = final_cep_df_to_insert.dropna(subset=["cep"])
            final_cep_df_to_insert["cep"] = final_cep_df_to_insert["cep"].astype(str).str.replace(r'\D', '', regex=True).str.zfill(8)
            final_cep_df_to_insert["ibge"] = final_cep_df_to_insert["ibge"].astype(str).str.slice(0, 7)
            final_cep_df_to_insert = final_cep_df_to_insert.drop_duplicates(subset=["cep", "logradouro", "bairro", "cidade", "uf"], keep='first')
            
            final_cep_df_to_insert = final_cep_df_to_insert[final_cep_df_to_insert["cep"].notna() & (final_cep_df_to_insert["cep"].str.len() == 8)]

            if not final_cep_df_to_insert.empty:
                logging.info(f"📊 Inserindo {len(final_cep_df_to_insert)} registros na tabela correios_ceps...")
                try:
                    final_cep_df_to_insert.to_sql("correios_ceps", engine, if_exists="append", index=False, chunksize=10000)
                    connection.commit()
                    logging.info("\u2705 Importação para correios_ceps finalizada com sucesso!")
                except Exception as e:
                    logging.error(f"Erro ao inserir dados na tabela correios_ceps: {e}")
                    try:
                        connection.rollback()
                    except Exception as rb_err:
                        logging.error(f"Erro ao tentar rollback: {rb_err}")
            else:
                logging.info("Nenhum dado de CEP para inserir após processamento.")

    except FileNotFoundError as e: # For dne_data_path itself or critical structure
        logging.error(f"Erro de arquivo não encontrado (nível superior): {e}")
    except SQLAlchemyError as e: # For engine creation or initial connection
        logging.error(f"Erro de banco de dados (nível superior): {e}")
    except Exception as e: # Catch-all for other unexpected errors
        logging.error(f"Erro inesperado (nível superior): {e}")
        import traceback
        logging.error(traceback.format_exc())


if __name__ == "__main__":
    logging.info("Executando importar_ceps_local.py como script autônomo para teste.")
    
    script_dir = os.path.dirname(os.path.abspath(__file__))
    # For testing, expect 'dne_data_example' to be inside 'backend'
    DNE_FILES_DIR_TEST = os.path.join(script_dir, "dne_data_example")
    DB_URI_TEST = f"sqlite:///{os.path.join(script_dir, 'test_cep_db.sqlite')}"

    if not os.path.exists(DNE_FILES_DIR_TEST):
        os.makedirs(DNE_FILES_DIR_TEST, exist_ok=True)
        logging.warning(f"Pasta de exemplo DNE criada em: {DNE_FILES_DIR_TEST}.")
        logging.warning("Adicione arquivos TXT do DNE (LOG_LOGRADOURO_*.TXT, etc.) nesta pasta para testar.")

    logging.info(f"Usando DNE Path para teste: {DNE_FILES_DIR_TEST}")
    logging.info(f"Usando DB URI para teste: {DB_URI_TEST}")

    try:
        temp_engine = create_engine(DB_URI_TEST)
        with temp_engine.connect() as conn:
            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS correios_ceps (
                    cep VARCHAR(8) PRIMARY KEY,
                    logradouro TEXT,
                    bairro TEXT,
                    cidade TEXT,
                    uf CHAR(2),
                    ibge VARCHAR(10),
                    origem TEXT
                );
            """))
            conn.commit()
        logging.info("Tabela 'correios_ceps' de teste verificada/criada.")
        
        populate_correios_ceps_if_empty(DB_URI_TEST, DNE_FILES_DIR_TEST)

    except Exception as e:
        logging.error(f"Erro durante a execução de teste de importar_ceps_local.py: {e}")
        import traceback
        logging.error(traceback.format_exc())
    finally:
        logging.info("Fim da execução de teste de importar_ceps_local.py.")
        # test_db_file = os.path.join(script_dir, 'test_cep_db.sqlite')
        # if os.path.exists(test_db_file):
        #     os.remove(test_db_file)
        #     logging.info(f"Arquivo de banco de dados de teste removido: {test_db_file}")