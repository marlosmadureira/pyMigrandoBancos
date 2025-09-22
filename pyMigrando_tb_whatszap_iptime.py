from pyBibliotecas import *
from datetime import datetime

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

# Configurações para ambiente de postgres
DB_HOST_BK_2 = os.getenv("DB_HOST_BK_2")
DB_NAME_BK_2= os.getenv("DB_NAME_BK_2")
DB_USER_BK_2 = os.getenv("DB_USER_BK_2")
DB_PASS_BK_2 = os.getenv("DB_PASS_BK_2")

def fetch_batches_call(sql, batch_size=3000):
    with conectBDPostgresProd(DB_HOST_PROD, DB_NAME_PROD, DB_USER_PROD, DB_PASS_PROD) as con:
        db = con.cursor(name="cursor_call_batch")
        db.execute(sql)
        while True:
            rows = db.fetchmany(batch_size)
            if not rows:
                break
            yield rows
        db.close()


def insert_batch_destino_83(rows):
    with conectBDPostgresBk(DB_HOST_BK, DB_NAME_BK, DB_USER_BK, DB_PASS_BK) as con:
        db = con.cursor()
        try:
            # Inserção na tabela arquivo (já existe, reaproveita)
            insert_arquivo = """
                             INSERT INTO leitores.tb_whatszap_arquivo
                             (ar_id, ar_dtcadastro, ar_arquivo, ar_tipo, ar_status, ar_dtgerado,
                              telefone, linh_id, ar_email_addresses, ar_json)
                             VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                             ON CONFLICT (ar_id) DO NOTHING"""

            # Inserção na tabela iptime
            insert_iptime = """
                            INSERT INTO leitores.tb_whatszap_iptime
                            (ip_id, ip_ip, ip_tempo, ar_id, ip_lat, ip_long, ip_operadora,
                             telefone, linh_id)
                            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                            ON CONFLICT DO NOTHING"""

            # dados para tb_whatszap_arquivo (iguais ao que você já usa)
            dados_arquivo = [
                (r[0], r[1], r[2], r[3], r[4], r[5], r[6], r[7], r[8], r[9]) for r in rows
            ]

            # dados para tb_whatszap_iptime
            dados_iptime = [
                (r[10], r[11], r[12], r[0], r[13], r[14], r[15], r[16], r[17]) for r in rows
            ]
            # aqui: r[0] = ar_id (já vem da tabela arquivo, chave estrangeira)

            db.executemany(insert_arquivo, dados_arquivo)
            db.executemany(insert_iptime, dados_iptime)

            for r in rows:
                grava_log(r[0], "iptimes.txt")  # ar_id

            con.commit()

            return {
                "ip_ids": [r[10] for r in rows],
                "ar_ids": [r[0] for r in rows],
            }
        except Exception as e:
            con.rollback()
            print_color(f"Erro ao inserir no destino (ip_id): {e}", 31)
            return {"ip_ids": [], "ar_ids": []}
        finally:
            db.close()

def insert_batch_destino_132(rows):
    with conectBDPostgresBk(DB_HOST_BK_2, DB_NAME_BK_2, DB_USER_BK_2, DB_PASS_BK_2) as con:
        db = con.cursor()
        try:
            # Inserção na tabela arquivo (já existe, reaproveita)
            insert_arquivo = """
                             INSERT INTO leitores.tb_whatszap_arquivo
                             (ar_id, ar_dtcadastro, ar_arquivo, ar_tipo, ar_status, ar_dtgerado,
                              telefone, linh_id, ar_email_addresses, ar_json)
                             VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                             ON CONFLICT (ar_id) DO NOTHING \
                             """

            # Inserção na tabela iptime
            insert_iptime = """
                            INSERT INTO leitores.tb_whatszap_iptime
                            (ip_id, ip_ip, ip_tempo, ar_id, ip_lat, ip_long, ip_operadora,
                             telefone, linh_id)
                            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                            ON CONFLICT DO NOTHING \
                            """

            # dados para tb_whatszap_arquivo (iguais ao que você já usa)
            dados_arquivo = [
                (r[0], r[1], r[2], r[3], r[4], r[5], r[6], r[7], r[8], r[9]) for r in rows
            ]

            # dados para tb_whatszap_iptime
            dados_iptime = [
                (r[10], r[11], r[12], r[0], r[13], r[14], r[15], r[16], r[17]) for r in rows
            ]
            # aqui: r[0] = ar_id (já vem da tabela arquivo, chave estrangeira)

            db.executemany(insert_arquivo, dados_arquivo)
            db.executemany(insert_iptime, dados_iptime)

            for r in rows:
                grava_log(r[0], "iptimes.txt")  # ar_id

            con.commit()
        except Exception as e:
            con.rollback()
            print_color(f"Erro ao inserir no destino (ip_id): {e}", 31)
        finally:
            db.close()

# def delete_origem_arquivos(ar_ids):
#     """Remove registros da tabela de arquivos no Prod"""
#     if not ar_ids:
#         return
#     with conectBDPostgresProd(DB_HOST_PROD, DB_NAME_PROD, DB_USER_PROD, DB_PASS_PROD) as con:
#         db = con.cursor()
#         try:
#             sql = "DELETE FROM leitores.tb_whatszap_arquivo WHERE ar_id IN %s"
#             db.execute(sql, (tuple(ar_ids),))
#             print_color(f"🗑️ Deletados {db.rowcount} arquivos", 35)
#             con.commit()
#         except Exception as e:
#             con.rollback()
#             print_color(f"Erro ao deletar arquivos (arquivo): {e}", 31)
#         finally:
#             db.close()


def delete_origem(ids):
    if not ids:
        return
    with conectBDPostgresProd(DB_HOST_PROD, DB_NAME_PROD, DB_USER_PROD, DB_PASS_PROD) as con:
        db = con.cursor()
        try:
            sql = "DELETE FROM leitores.tb_whatszap_iptime WHERE ip_id IN %s"
            db.execute(sql, (tuple(ids),))  # precisa ser tupla para o psycopg2 entender
            print_color(f"Deletados {db.rowcount} registros", 32)
            con.commit()
        except Exception as e:
            con.rollback()
            print_color(f"Erro ao deletar origem (ip_id): {e}", 31)
        finally:
            db.close()


def mainipTimes():
    ultimo_id = get_last_id("iptimes.txt")
    if ultimo_id:
        ultimo_id = 0
        print("Último ID gravado:", ultimo_id)
    else:
        print("Nenhum ID encontrado ainda.")

    sql = f"""
          SELECT a.ar_id, a.ar_dtcadastro, a.ar_arquivo, a.ar_tipo, a.ar_status, a.ar_dtgerado, a.telefone, a.linh_id, a.ar_email_addresses, a.ar_json, i.ip_id, i.ip_ip, i.ip_tempo, i.ip_lat, i.ip_long, i.ip_operadora, i.telefone AS ip_telefone, i.linh_id  AS ip_linh_id, i.ar_id    AS ip_ar_id FROM leitores.tb_whatszap_iptime i JOIN leitores.tb_whatszap_arquivo a ON a.ar_id = i.ar_id WHERE a.ar_id > {ultimo_id} AND i.ip_tempo < '2025-01-01' ORDER BY a.ar_id ASC"""

    total = 0

    for lote in fetch_batches_call(sql):
        agora = datetime.now()
        print(f"🔄 Processando Calllogs lote de {len(lote)} registros... {agora.strftime('%d/%m/%Y %H:%M:%S')} ")
        result = insert_batch_destino_83(lote)
        insert_batch_destino_132(lote)

        total = total + len(lote)

        if result["ip_ids"]:
            delete_origem(result["ip_ids"])
            # delete_origem_arquivos(result["ar_ids"])
            agora = datetime.now()
            print(f"✅ Inseridos e removidos {len(result['ip_ids'])} registros {agora.strftime('%d/%m/%Y %H:%M:%S')} ")
        else:
            print("⚠️ Nenhum registro inserido, nada foi apagado")

        beep()

        print_color(f"\nTotal Processado {total}\n", 34)