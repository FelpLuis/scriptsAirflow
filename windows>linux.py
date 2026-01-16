from airflow.sdk import dag, task # Mantendo o padrão que sua versão pede
from airflow.providers.samba.hooks.samba import SambaHook
from airflow.providers.sftp.operators.sftp import SFTPOperator
from datetime import datetime, timedelta
import os
import shutil

# --- CONFIGURAÇÕES ---
LOCAL_FILE_PATH = '/home/timeware/airflow/temp_airflow' #inserir caminho da pasta da instancia airflow
REMOTE_SOURCE_PATH = 'profarma'  #inserir caminho da pasta server origem
REMOTE_DEST_PATH = '/home/timeware/recebe_airflow' #caminho da pasta server destino

default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

@dag(
    dag_id="script_trasnferir_arquivos_linuxXwindows", 
    default_args=default_args,
    start_date=datetime(2026, 1, 15),
    schedule=timedelta(minutes=1),
    catchup=False,
    max_active_runs=1,
)
def transferencia_smb_dag():

    @task
    def preparar_diretorio_local():
        if os.path.exists(LOCAL_FILE_PATH):
            shutil.rmtree(LOCAL_FILE_PATH)
        os.makedirs(LOCAL_FILE_PATH, exist_ok=True)
        return LOCAL_FILE_PATH

    @task
    def baixar_arquivos_smb(local_path):
        import smbclient 
        from airflow.providers.samba.hooks.samba import SambaHook
        
        hook = SambaHook(samba_conn_id='copiarwindows') #inserir ID da Conexão de origem
        conn_data = hook.get_connection('copiarwindows')
        username = conn_data.login
        password = conn_data.password
        host = conn_data.host
        share = conn_data.schema  
        

        smbclient.register_session(host, username=username, password=password)
        
        base_path = f"\\\\{host}\\{share}\\{REMOTE_SOURCE_PATH}"
        
        arquivos_baixados = []
        try:
            arquivos = smbclient.listdir(base_path)
            
            for f in arquivos:
                if f and not f.startswith('.'):
                    caminho_remoto = os.path.join(base_path, f)
                    caminho_local = os.path.join(local_path, f)
                    
                    with smbclient.open_file(caminho_remoto, mode="rb") as remote_f:
                        with open(caminho_local, "wb") as local_f:
                            local_f.write(remote_f.read())
                    
                    arquivos_baixados.append(f)
                    print(f"Baixado com sucesso: {f}")
        finally:
            smbclient.reset_connection_cache() 
            
        return arquivos_baixados

    @task
    def gerar_mapeamento_envio(arquivos):
        return [
            {
                "local_filepath": os.path.join(LOCAL_FILE_PATH, f),
                "remote_filepath": os.path.join(REMOTE_DEST_PATH, f),
            }
            for f in arquivos
        ] if arquivos else []

    @task
    def deletar_arquivos_origem_smb(arquivos: list):
        import smbclient
        hook = SambaHook(samba_conn_id='copiarwindows') #inserir ID de conexão da origem
        conn_data = hook.get_connection('copiarwindows') 
        
        smbclient.register_session(conn_data.host, username=conn_data.login, password=conn_data.password)
        base_path = f"\\\\{conn_data.host}\\{conn_data.schema}\\{REMOTE_SOURCE_PATH}"
        
        for f in arquivos:
            try:
                smbclient.remove(os.path.join(base_path, f))
                print(f"Removido da origem: {f}")
            except Exception as e:
                print(f"Erro ao deletar {f}: {e}")

    # Fluxo
    dir_t = preparar_diretorio_local()
    baixados = baixar_arquivos_smb(dir_t)
    map_envio = gerar_mapeamento_envio(baixados)
    
    enviar = SFTPOperator.partial(
        task_id='enviar_arquivos_sftp',
        ssh_conn_id='colar', #Inseri ID do server destino 
        operation='put',
        create_intermediate_dirs=True
    ).expand_kwargs(map_envio)

    enviar >> deletar_arquivos_origem_smb(baixados)

transferencia_smb_dag()
