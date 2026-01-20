from airflow.sdk  import dag, task
from airflow.providers.sftp.operators.sftp import SFTPOperator
from airflow.providers.sftp.hooks.sftp import SFTPHook
from datetime import datetime, timedelta
import os
import shutil

# Configurações
LOCAL_FILE_PATH = '/home/timeware/airflow/temp_airflow' #caminho pasta airflow
REMOTE_SOURCE_PATH = '/home/timeware/airflow_script' #caminho pasta origem
REMOTE_DEST_PATH = '/home/timeware/recebe_airflow' #caminhho pasta destino

default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(seconds=3),
}

@dag(
    dag_id="transferencia_sftp",
    default_args=default_args,
    start_date=datetime(2026, 1, 20),
    schedule=timedelta(seconds=3),
    catchup=False, 
    max_active_runs=1,
 tags=['sftp', 'transferencia']
)
def transferencia_sftp_dag():

    @task
    def preparar_diretorio_local():
        if os.path.exists(LOCAL_FILE_PATH):
            shutil.rmtree(LOCAL_FILE_PATH)
        os.makedirs(LOCAL_FILE_PATH, exist_ok=True)
        print(f"Diretório local preparado: {LOCAL_FILE_PATH}")

    @task
    def listar_arquivos():
        hook = SFTPHook(ftp_conn_id='copiar')
        return [f for f in arquivos if not f.startswith('.')]

    @task
    def gerar_caminhos(arquivos):
        return [
            {
                "local": os.path.join(LOCAL_FILE_PATH, f),
                "origem": os.path.join(REMOTE_SOURCE_PATH, f),
                "destino": os.path.join(REMOTE_DEST_PATH, f)
            }
            for f in arquivos
        ]

    @task
    def log_arquivos_transferidos(arquivos: list):
        for f in arquivos:
            print(f"Arquivo transferido com sucesso: {f}")

     @task
    def deletar_arquivos_origem_sftp(arquivos):
        sftp_hook = SFTPHook(ssh_conn_id='copiar')
        for f in arquivos:
            remote_f_path = os.path.join(REMOTE_SOURCE_PATH, f)
            sftp_hook.delete_file(remote_f_path)
            print(f"Removido da origem: {f}")

    @task
    def deletar_arquivos_locais():
        if os.path.exists(LOCAL_FILE_PATH):
            shutil.rmtree(LOCAL_FILE_PATH)
            print(f"Diretório local removido.")

    # --- Fluxo de Execução ---
    
    setup_local = preparar_diretorio_local()
    lista_nomes = listar_arquivos()
    
    # Geramos o mapeamento de caminhos
    caminhos = gerar_caminhos(lista_nomes)

    # Baixar arquivos
    baixar = SFTPOperator.partial(
        task_id='baixar_arquivos',
        ssh_conn_id='copiar', #inserir ID da conexão origem
        operation='get',
        create_intermediate_dirs=True
    ).expand(
        remote_filepath=caminhos.map(lambda c: c["origem"]),
        local_filepath=caminhos.map(lambda c: c["local"])
    )

    # Enviar arquivos
    enviar = SFTPOperator.partial(
        task_id='enviar_arquivos',
        ssh_conn_id='colar', #inserir ID da conexão destino 
        operation='put',
        create_intermediate_dirs=True
    ).expand(
        local_filepath=caminhos.map(lambda c: c["local"]),
        remote_filepath=caminhos.map(lambda c: c["destino"])
    )

    # Ordem de dependências
    setup_local >> lista_nomes >> caminhos >> baixar >> enviar
    
    # Finalização
    enviar >> log_arquivos_transferidos(lista_nomes)
    enviar >> deletar_arquivos_locais()
    enviar >> >> deletar_arquivos_origem_sftp(lista_arquivos)

# Instancia a DAG
transferencia_sftp_dag()
