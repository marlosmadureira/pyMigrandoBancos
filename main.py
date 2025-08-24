from pyMigrando_tb_whatszap_call_log import *
from pyMigrando_tb_whatszap_index_zapcontatos_new import *
from pyBibliotecas import *
import threading

def run_parallel():
    print_color("\nInicializando....\n", 32)

    # Cria as threads para rodar as funções
    t1 = threading.Thread(target=mainNewLogs, name="Thread-NewLogs")
    t2 = threading.Thread(target=mainCallLogs, name="Thread-CallLogs")

    # Inicia as threads
    t1.start()
    t2.start()

    # Aguarda ambas terminarem
    t1.join()
    t2.join()

if __name__ == '__main__':

    print_color(f"\nInicializando....\n", 32)

    run_parallel()