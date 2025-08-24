from pyBibliotecas import *

# Configs
load_dotenv()

# Configurações para ambiente de postgres
DB_HOST_PROD = os.getenv("DB_HOST_PROD")
DB_NAME_PROD = os.getenv("DB_NAME_PROD")
DB_USER_PROD = os.getenv("DB_USER_PROD")
DB_PASS_PROD = os.getenv("DB_PASS_PROD")

# Configurações para ambiente de postgres
DB_HOST_BK = os.getenv("DB_HOST_BK")
DB_NAME_BK = os.getenv("DB_NAME_BK")
DB_USER_BK = os.getenv("DB_USER_BK")
DB_PASS_BK = os.getenv("DB_PASS_BK")

def fetch_batches_call(sql, batch_size=5000):
    with conectBDPostgresProd(DB_HOST_PROD, DB_NAME_PROD, DB_USER_PROD, DB_PASS_PROD) as con:
        db = con.cursor(name="cursor_call_batch")
        db.execute(sql)
        while True:
            rows = db.fetchmany(batch_size)
            if not rows:
                break
            yield rows
        db.close()


def insert_batch_destino(rows):
    with conectBDPostgresBk(DB_HOST_BK, DB_NAME_BK, DB_USER_BK, DB_PASS_BK) as con:
        db = con.cursor()
        try:
            # Inserção na tabela arquivo
            insert_arquivo = """
                INSERT INTO leitores.tb_whatszap_arquivo
                (ar_id, ar_dtcadastro, ar_arquivo, ar_tipo, ar_status, ar_dtgerado,
                 telefone, linh_id, ar_email_addresses, ar_json)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                ON CONFLICT (ar_id) DO NOTHING
            """

            # Inserção na tabela call_log
            insert_call = """
                INSERT INTO leitores.tb_whatszap_call_log
                (cal_id, call_id, call_creator, call_type, call_timestamp,
                 call_from, call_to, call_from_ip, call_from_port,
                 call_media_type, call_phone_number, call_state, call_platform,
                 telefone, ar_id, linh_id, sentido, json_analise, obs_analise)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                ON CONFLICT DO NOTHING
            """

            dados_arquivo = [
                (r[0], r[1], r[2], r[3], r[4], r[5], r[6], r[7], r[8], r[9]) for r in rows
            ]
            dados_call = [
                (r[10], r[11], r[12], r[13], r[14], r[15], r[16], r[17], r[18], r[19],
                 r[20], r[21], r[22], r[23], r[24], r[25], r[26], r[27], r[28]) for r in rows
            ]

            db.executemany(insert_arquivo, dados_arquivo)
            db.executemany(insert_call, dados_call)
            con.commit()

            return {
                "cal_ids": [r[10] for r in rows],
                "ar_ids": [r[0] for r in rows],
            }
        except Exception as e:
            con.rollback()
            print_color(f"Erro ao inserir no destino (call_log): {e}", 31)
            return {"cal_ids": [], "ar_ids": []}
        finally:
            db.close()

def delete_origem_arquivos(ar_ids):
    """Remove registros da tabela de arquivos no Prod"""
    if not ar_ids:
        return
    with conectBDPostgresProd(DB_HOST_PROD, DB_NAME_PROD, DB_USER_PROD, DB_PASS_PROD) as con:
        db = con.cursor()
        try:
            sql = "DELETE FROM leitores.tb_whatszap_arquivo WHERE ar_id IN %s"
            db.execute(sql, (tuple(ar_ids),))
            print_color(f"🗑️ Deletados {db.rowcount} arquivos", 35)

            con.commit()
        except Exception as e:
            con.rollback()
            print_color(f"Erro ao deletar arquivos (arquivo): {e}", 31)
        finally:
            db.close()


def delete_origem(ids):
    if not ids:
        return
    with conectBDPostgresProd(DB_HOST_PROD, DB_NAME_PROD, DB_USER_PROD, DB_PASS_PROD) as con:
        db = con.cursor()
        try:
            sql = "DELETE FROM leitores.tb_whatszap_call_log WHERE cal_id IN %s"
            db.execute(sql, (tuple(ids),))  # precisa ser tupla para o psycopg2 entender
            print_color(f"Deletados {db.rowcount} registros", 32)

            con.commit()
        except Exception as e:
            con.rollback()
            print_color(f"Erro ao deletar origem (call_log): {e}", 31)
        finally:
            db.close()


def mainCallLogs():
    sql = """
        SELECT 
            a.ar_id, a.ar_dtcadastro, a.ar_arquivo, a.ar_tipo, a.ar_status, 
            a.ar_dtgerado, a.telefone, a.linh_id, a.ar_email_addresses, a.ar_json,
            c.cal_id, c.call_id, c.call_creator, c.call_type, c.call_timestamp, 
            c.call_from, c.call_to, c.call_from_ip, c.call_from_port, 
            c.call_media_type, c.call_phone_number, c.call_state, c.call_platform, 
            c.telefone, c.ar_id, c.linh_id, c.sentido, c.json_analise, c.obs_analise
        FROM leitores.tb_whatszap_call_log c
        JOIN leitores.tb_whatszap_arquivo a ON a.ar_id = c.ar_id
        WHERE c.call_timestamp < '2024-01-01'
        ORDER BY c.call_timestamp ASC
    """

    for lote in fetch_batches_call(sql):
        print(f"🔄 Processando Calllogs lote de {len(lote)} registros...")
        result = insert_batch_destino(lote)

        if result["cal_ids"]:
            delete_origem(result["cal_ids"])
            # delete_origem_arquivos(result["ar_ids"])
            print(f"✅ Inseridos e removidos {len(result['cal_ids'])} registros")
        else:
            print("⚠️ Nenhum registro inserido, nada foi apagado")