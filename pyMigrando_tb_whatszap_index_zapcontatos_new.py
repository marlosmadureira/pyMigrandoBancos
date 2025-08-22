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

def fetch_batches(sql, batch_size=5000):
    """Lê em lotes do banco origem"""
    with conectBDPostgresProd(DB_HOST_PROD, DB_NAME_PROD, DB_USER_PROD, DB_PASS_PROD) as con:
        db = con.cursor(name="cursor_batch")  # server-side cursor
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
            # Inserir tb_whatszap_arquivo
            insert_arquivo = """
                INSERT INTO leitores.tb_whatszap_arquivo
                (ar_id, ar_dtcadastro, ar_arquivo, ar_tipo, ar_status, ar_dtgerado,
                 telefone, linh_id, ar_email_addresses, ar_json)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                ON CONFLICT (ar_id) DO NOTHING
            """

            # Inserir tb_whatszap_index_zapcontatos_new
            insert_index = """
                INSERT INTO leitores.tb_whatszap_index_zapcontatos_new
                (indn_id, datahora, messageid, sentido, alvo, interlocutor,
                 groupid, senderip, senderport, senderdevice, messagesize, typemsg,
                 messagestyle, ar_id, telefone, linh_id, json_analise, obs_analise)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                ON CONFLICT (indn_id) DO NOTHING
            """

            # separar os dados para cada tabela
            dados_arquivo = [
                (r[0], r[1], r[2], r[3], r[4], r[5], r[6], r[7], r[8], r[9]) for r in rows
            ]
            dados_index = [
                (r[10], r[11], r[12], r[13], r[14], r[15], r[16], r[17], r[18],
                 r[19], r[20], r[21], r[22], r[23], r[24], r[25], r[26], r[27]) for r in rows
            ]

            db.executemany(insert_arquivo, dados_arquivo)
            db.executemany(insert_index, dados_index)

            con.commit()
            return [r[10] for r in rows]  # retorna lista de indn_id inseridos
        except Exception as e:
            con.rollback()
            print_color(f"Erro ao inserir no destino: {e}", 31)
            return []
        finally:
            db.close()

def delete_origem(ids):
    if not ids:
        return
    with conectBDPostgresProd(DB_HOST_PROD, DB_NAME_PROD, DB_USER_PROD, DB_PASS_PROD) as con:
        db = con.cursor()
        try:
            sql = "DELETE FROM leitores.tb_whatszap_index_zapcontatos_new WHERE indn_id = ANY(%s)"
            db.execute(sql, (ids,))
            con.commit()
        except Exception as e:
            con.rollback()
            print_color(f"Erro ao deletar origem: {e}", 31)
        finally:
            db.close()

def mainNewLogs():
    sql = """
        SELECT 
            a.ar_id, a.ar_dtcadastro, a.ar_arquivo, a.ar_tipo, a.ar_status, 
            a.ar_dtgerado, a.telefone, a.linh_id, a.ar_email_addresses, a.ar_json,
            i.indn_id, i.datahora, i.messageid, i.sentido, i.alvo, i.interlocutor, 
            i.groupid, i.senderip, i.senderport, i.senderdevice, i.messagesize, 
            i.typemsg, i.messagestyle, i.ar_id, i.telefone, i.linh_id, 
            i.json_analise, i.obs_analise
        FROM leitores.tb_whatszap_index_zapcontatos_new i
        JOIN leitores.tb_whatszap_arquivo a ON a.ar_id = i.ar_id
        WHERE i.datahora < '2024-01-01'
        ORDER BY i.datahora ASC
    """

    for lote in fetch_batches(sql):
        print(f"🔄 Processando Newlogs lote de {len(lote)} registros...")
        ids_inseridos = insert_batch_destino(lote)
        if ids_inseridos:
            delete_origem(ids_inseridos)
            print(f"✅ Inseridos e removidos {len(ids_inseridos)} registros")
        else:
            print("⚠️ Nenhum registro inserido, nada foi apagado")