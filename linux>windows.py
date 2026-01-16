from airflow.decorators import dag, task
from airflow.providers.sftp.hooks.sftp import SFTPHook
from airflow.providers.samba.hooks.samba import SambaHook
from datetime import datetime, timedelta
import os
import shutil
import smbclient

# --- CONFIGURAÇÕES ---
LOCAL_TEMP_PATH = '/home/timeware/airflow/temp_airflow'
REMOTE_SOURCE_PATH = '/home/timeware/airflow_script'  
REMOTE_DEST_PATH = 'profarma'                      

default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

@dag(
    dag_id="transferencia_linux_para_windows",
    default_args=default_args,
    start_date=datetime(2026, 1, 15),
    schedule=timedelta(minutes=1),
    catchup=False,
    max_active_runs=1,
)
def transferencia_sftp_smb_dag():

    @task
    def preparar_diretorio_local():
        if os.path.exists(LOCAL_TEMP_PATH):
            shutil.rmtree(LOCAL_TEMP_PATH)
        os.makedirs(LOCAL_TEMP_PATH, exist_ok=True)
        return LOCAL_TEMP_PATH

    @task
    def baixar_arquivos_sftp(local_path):
        sftp_hook = SFTPHook(ssh_conn_id='copiar')
        
        arquivos = sftp_hook.list_directory(REMOTE_SOURCE_PATH)
        arquivos_baixados = []

        for f in arquivos:
            remote_f_path = os.path.join(REMOTE_SOURCE_PATH, f)
            local_f_path = os.path.join(local_path, f)
            
            sftp_hook.retrieve_file(remote_f_path, local_f_path)
            arquivos_baixados.append(f)
            print(f"Baixado via SFTP: {f}")
            
        return arquivos_baixados

    @task
    def enviar_arquivos_smb(arquivos, local_path):
        if not arquivos:
            print("Nenhum arquivo para transferir.")
            return
            
        smb_hook = SambaHook(samba_conn_id='copiarwindows') 
        conn_data = smb_hook.get_connection('copiarwindows')
        smbclient.register_session(
            conn_data.host, 
            username=conn_data.login, 
            password=conn_data.password
        )
        
        base_dest_path = f"\\\\{conn_data.host}\\{conn_data.schema}\\{REMOTE_DEST_PATH}"
        
        try:
            for f in arquivos:
                caminho_local = os.path.join(local_path, f)
                caminho_windows = os.path.join(base_dest_path, f)
                
                with open(caminho_local, "rb") as local_f:
                    with smbclient.open_file(caminho_windows, mode="wb") as remote_f:
                        remote_f.write(local_f.read())
                print(f"Enviado para Windows: {f}")
        finally:
            smbclient.reset_connection_cache()

    @task
    def deletar_arquivos_origem_sftp(arquivos):
        sftp_hook = SFTPHook(ssh_conn_id='copiar')
        for f in arquivos:
            remote_f_path = os.path.join(REMOTE_SOURCE_PATH, f)
            sftp_hook.delete_file(remote_f_path)
            print(f"Removido da origem (Linux): {f}")

    # --- FLUXO ---
    dir_local = preparar_diretorio_local()
    lista_arquivos = baixar_arquivos_sftp(dir_local)
    
    # Executa envio e depois a limpeza
    envio = enviar_arquivos_smb(lista_arquivos, dir_local)
    envio >> deletar_arquivos_origem_sftp(lista_arquivos)

transferencia_sftp_smb_dag()
