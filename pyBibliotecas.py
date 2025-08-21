import psycopg2
import os
from dotenv import load_dotenv
from psycopg2 import OperationalError

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

def conectBDPostgresProd(DB_HOST, DB_NAME, DB_USER, DB_PASS):
    con = psycopg2.connect(host=DB_HOST, database=DB_NAME, user=DB_USER, password=DB_PASS)
    return con

def conectBDPostgresBk(DB_HOST, DB_NAME, DB_USER, DB_PASS):
    con = psycopg2.connect(host=DB_HOST, database=DB_NAME, user=DB_USER, password=DB_PASS)
    return con

def print_color(text, color_code):
    print(f"\033[{color_code}m{text}\033[0m")

def testar_conexao_postgres_prdo():
    """
    Função para testar a conexão com PostgreSQL usando psycopg2.
    """
    try:
        print_color("\n=== Testando conexão com PostgreSQL (psycopg2) ===", 35)

        # Verificar variáveis de ambiente
        if not all([DB_HOST_PROD, DB_NAME_PROD, DB_USER_PROD, DB_PASS_PROD]):
            print_color("❌ Variáveis de ambiente para produção não estão configuradas", 31)
            print_color("Configure as seguintes variáveis no arquivo .env:", 33)
            print_color("DB_HOST, DB_NAME, DB_USER, DB_PASS", 33)
            return False

        print_color(f"✅ Host: {DB_HOST_PROD}", 32)
        print_color(f"✅ Database: {DB_NAME_PROD}", 32)
        print_color(f"✅ Usuário: {DB_USER_PROD}", 32)

        # Tentar conexão usando psycopg2
        con = conectBDPostgresProd(DB_HOST_PROD, DB_NAME_PROD, DB_USER_PROD, DB_PASS_PROD)
        if con is None:
            print_color("❌ Falha na conexão psycopg2", 31)
            return False

        # Testar query simples
        with con:
            with con.cursor() as cur:
                cur.execute("SELECT version();")
                version = cur.fetchone()
                print_color(f"✅ Versão do PostgreSQL: {version[0]}", 32)

        print_color("✅ Conexão com PostgreSQL funcionando perfeitamente!", 32)
        return True
    except OperationalError as e:
        print_color(f"❌ Erro operacional ao conectar ao PostgreSQL: {e}", 31)
        return False
    except Exception as e:
        print_color(f"❌ Erro no teste de conexão PostgreSQL: {e}", 31)
        return False


def testar_conexao_postgres_bk():
    """
    Função para testar a conexão com PostgreSQL usando psycopg2.
    """
    try:
        print_color("\n=== Testando conexão com PostgreSQL (psycopg2) ===", 35)

        # Verificar variáveis de ambiente
        if not all([DB_HOST_BK, DB_NAME_BK, DB_USER_BK, DB_PASS_BK]):
            print_color("❌ Variáveis de ambiente para produção não estão configuradas", 31)
            print_color("Configure as seguintes variáveis no arquivo .env:", 33)
            print_color("DB_HOST, DB_NAME, DB_USER, DB_PASS", 33)
            return False

        print_color(f"✅ Host: {DB_HOST_BK}", 32)
        print_color(f"✅ Database: {DB_NAME_BK}", 32)
        print_color(f"✅ Usuário: {DB_USER_BK}", 32)

        # Tentar conexão usando psycopg2
        con = conectBDPostgresBk(DB_HOST_BK, DB_NAME_BK, DB_USER_BK, DB_PASS_BK)
        if con is None:
            print_color("❌ Falha na conexão psycopg2", 31)
            return False

        # Testar query simples
        with con:
            with con.cursor() as cur:
                cur.execute("SELECT version();")
                version = cur.fetchone()
                print_color(f"✅ Versão do PostgreSQL: {version[0]}", 32)

        print_color("✅ Conexão com PostgreSQL funcionando perfeitamente!", 32)
        return True
    except OperationalError as e:
        print_color(f"❌ Erro operacional ao conectar ao PostgreSQL: {e}", 31)
        return False
    except Exception as e:
        print_color(f"❌ Erro no teste de conexão PostgreSQL: {e}", 31)
        return False