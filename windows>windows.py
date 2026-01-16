from airflow.sdk import dag, task
from airflow.providers.samba.hooks.samba import SambaHook
from datetime import datetime, timedelta
import os
import shutil
import smbclient

# --- CONFIGURAÇÕES ---
LOCAL_FILE_PATH = '/home/timeware/airflow/temp_airflow'
REMOTE_SOURCE_PATH = 'profarma' 
REMOTE_DEST_PATH = 'colarwindows' 

default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

@dag(
    dag_id="transferencia_windows_windows",
    default_args=default_args,
    start_date=datetime(2026, 1, 15),
    schedule=timedelta(minutes=1),
    catchup=False,
    max_active_runs=1,
    tags=['smb', 'windows', 'sdk3']
)
def transferencia_smb_dag():

    @task
    def preparar_diretorio_local():
        if os.path.exists(LOCAL_FILE_PATH):
            shutil.rmtree(LOCAL_FILE_PATH)
        os.makedirs(LOCAL_FILE_PATH, exist_ok=True)
        return LOCAL_FILE_PATH

    @task
    def baixar_arquivos_origem(local_path):
        hook = SambaHook(samba_conn_id='copiarwindows')
        conn = hook.get_connection('copiarwindows')
        smbclient.register_session(conn.host, username=conn.login, password=conn.password)
        base_path = f"\\\\{conn.host}\\{conn.schema}\\{REMOTE_SOURCE_PATH}"
        
        arquivos_baixados = []
        try:
            for f in smbclient.listdir(base_path):
                if f and not f.startswith('.'):
                    caminho_remoto = os.path.join(base_path, f)
                    caminho_local = os.path.join(local_path, f)
                    
                    with smbclient.open_file(caminho_remoto, mode="rb") as r, open(caminho_local, "wb") as l:
                        l.write(r.read())
                    
                    arquivos_baixados.append(f)
            return arquivos_baixados
        finally:
            smbclient.reset_connection_cache()

    @task
    def enviar_arquivos_destino(arquivos: list):
        if not arquivos:
            return "Nenhum arquivo para enviar"

        hook = SambaHook(samba_conn_id='colarwindows')
        conn = hook.get_connection('colarwindows')
        smbclient.register_session(conn.host, username=conn.login, password=conn.password)
        base_path_dest = f"\\\\{conn.host}\\{conn.schema}\\{REMOTE_DEST_PATH}"

        if not smbclient.path.exists(base_path_dest):
            smbclient.makedirs(base_path_dest)

        sucessos = []
        try:
            for f in arquivos:
                caminho_local = os.path.join(LOCAL_FILE_PATH, f)
                caminho_remoto = os.path.join(base_path_dest, f)
                
                with open(caminho_local, "rb") as l, smbclient.open_file(caminho_remoto, mode="wb") as r:
                    r.write(l.read())
                
                sucessos.append(f)
                print(f"Enviado para destino SMB: {f}")
            return sucessos
        finally:
            smbclient.reset_connection_cache()

    @task
    def deletar_arquivos_origem(arquivos_confirmados: list):
        if not arquivos_confirmados:
            return
            
        hook = SambaHook(samba_conn_id='copiarwindows')
        conn = hook.get_connection('copiarwindows')
        
        smbclient.register_session(conn.host, username=conn.login, password=conn.password)
        base_path_origem = f"\\\\{conn.host}\\{conn.schema}\\{REMOTE_SOURCE_PATH}"
        
        try:
            for f in arquivos_confirmados:
                smbclient.remove(os.path.join(base_path_origem, f))
                print(f"Removido da origem Windows: {f}")
        finally:
            smbclient.reset_connection_cache()

    # --- FLUXO ---
    dir_t = preparar_diretorio_local()
    baixados = baixar_arquivos_origem(dir_t)
    enviados = enviar_arquivos_destino(baixados)
    
    # Só deleta da origem o que foi confirmado como enviado ao destino
    deletar_arquivos_origem(enviados)

transferencia_smb_dag()
