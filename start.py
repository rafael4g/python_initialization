# Init File
import os
import nbformat as nbf

def criar_pasta_template(base_path):
    """ criar_pasta_template """

    estrutura_pastas = [
        "src",        
        "src/utils",
        "src/database",
        ".vscode",
        "src/bucket/bronze",
        "src/bucket/silver",
        "src/bucket/gold"
    ]

    for pasta in estrutura_pastas:
        caminho_completo = os.path.join(base_path, pasta)
        os.makedirs(caminho_completo, exist_ok=True)
        print(f'Criado: {caminho_completo}')        

    # Criar e adicionar conteúdo ao arquivo src/bucket/bronze/.gitkeep
    init_file_bucket_silver = os.path.join(base_path, "src/bucket/bronze/.gitkeep")
    if not os.path.exists(init_file_bucket_silver):
        with open(init_file_bucket_silver, 'w') as init_file:
            init_file.write("""""")  
            
    # Criar e adicionar conteúdo ao arquivo src/bucket/silver/.gitkeep
    init_file_bucket_silver = os.path.join(base_path, "src/bucket/silver/.gitkeep")
    if not os.path.exists(init_file_bucket_silver):
        with open(init_file_bucket_silver, 'w') as init_file:
            init_file.write("""""")      

    # Criar e adicionar conteúdo ao arquivo src/bucket/gold/.gitkeep
    init_file_bucket_gold = os.path.join(base_path, "src/bucket/gold/.gitkeep")
    if not os.path.exists(init_file_bucket_gold):
        with open(init_file_bucket_gold, 'w') as init_file:
            init_file.write("""""") 

    # Criar e adicionar conteúdo ao arquivo src/database/.gitkeep
    init_file_database = os.path.join(base_path, "src/database/.gitkeep")
    if not os.path.exists(init_file_database):
        with open(init_file_database, 'w') as init_file:
            init_file.write("""""")             

    # Criar e adicionar conteúdo ao arquivo src/__init__.py
    init_file_src = os.path.join(base_path, "src/__init__.py")
    if not os.path.exists(init_file_src):
        with open(init_file_src, 'w') as init_file:
            init_file.write("""""")

    # Criar e adicionar conteúdo ao arquivo src/utils/__init__.py
    init_file_path = os.path.join(base_path, "src/utils/__init__.py")
    if not os.path.exists(init_file_path):
        with open(init_file_path, 'w') as init_file:
            init_file.write("""\
import os
import re
from typing import List, Dict
from zoneinfo import ZoneInfo # Python 3.9+
from sqlalchemy import create_engine, text
from unicodedata import normalize
from datetime import datetime
from pathlib import Path  
import xml.etree.ElementTree as ET
import pandas as pd
import hashlib
import mysql.connector
from decouple import config

MYSQL_USER = config('MYSQL_USER')
MYSQL_PASS = config('MYSQL_PASS')
MYSQL_HOST = config('MYSQL_HOST')
MYSQL_PORT = config('MYSQL_PORT')
HOST_LOC = config('HOST_LOC')
USER_LOC = config('USER_LOC')
PASS_LOC = config('PASS_LOC')
PATH_BUCKET = config('PATH_BUCKET')
ENV_BRONZE = config('ENV_BRONZE')
ENV_SILVER = config('ENV_SILVER')
ENV_GOLD = config('ENV_GOLD')
DUCKDB_DATABASE = config('DUCKDB_DATABASE')
                            
DATETIME_HOUR_MINUTES = datetime.strftime(datetime.now(), "%Y-%m-%d %H:%M:%S")
                            
dict_env_default = {
    'usr': USER_LOC
    , 'pwd': PASS_LOC
    , 'hst': HOST_LOC
    , 'prt': 3306
}


def connect_to_mysql(_db_name: str, _dict_env: Dict | None = dict_env_default) -> mysql.connector:
    # conectando ao mysql/mariadb lib: mysql.connector

    config_mysql = {
    'user': _dict_env['usr'],
    'password': _dict_env['pwd'],
    'host': _dict_env['hst'],
    'database': _db_name,   
    'port': _dict_env['prt'], 
    'allow_local_infile': True,
    }
    
    conn = mysql.connector.connect(**config_mysql)
        
    return conn

def load_data_to_mysql(_file_path: str, _table_name: str, _columns_default: list[str] | None) -> str:
    # Criar padrao para load data infile

    colunas_str = ', '.join(_columns_default)
    table_name_correct =  _table_name
    
    load_data_sql = f'''
    LOAD DATA LOCAL INFILE '{_file_path}'
    INTO TABLE {table_name_correct}
    CHARACTER SET utf8mb4
    FIELDS TERMINATED BY ';' ENCLOSED BY '"'
    LINES TERMINATED BY '\\n'
    IGNORE 1 LINES
    ({colunas_str});
    '''    

    return load_data_sql


def handle_mysql_local_delete( _db_destino: str, _table_name: str, _name_col_delete: str = '', _period: int = 0, _dict_credentials= Dict ) -> str:
    ### Deleta a tabela com base na variavel _name_col_delete.
    # Parametros
    #---------
    # _db_destino: str          Nome do banco de dados destino
    # _table_name: str          Nome da tabela destino
    # _name_col_delete: str     Nome da coluna utilizada para o criterio de delete, caso seja necessario
    # _period: int              Periodo para filtrar os registros a serem excluidos
    # _dict_credentials: Dict   Dicionario com as credenciais de acesso ao banco de dados
    #---------
    # Retorna: Apenas mensagem de sucesso ou error!

    if not(_dict_credentials):
        _dict_credentials = dict_env_default
        
    if _period == 0:
        query_delete__carga = f'truncate table {_table_name}' 
    else:        
        query_delete__carga = f'DELETE FROM {_table_name} WHERE {_name_col_delete} = {_period}' 
    
    conn = connect_to_mysql(_db_destino, _dict_env=_dict_credentials)
    cursor = conn.cursor(buffered=True)
    try:    
        # Executar o comando TRUNCATE TABLE  
        cursor.execute('SET FOREIGN_KEY_CHECKS = 0;')     
        cursor.execute(query_delete__carga) 
        cursor.execute('SET FOREIGN_KEY_CHECKS = 1;')
        # print(f'Delete removido da função, utilizada para critério incremetal')   
        conn.commit()
        output_str = f'Tabela {_table_name} limpa com sucesso!'
        cursor.close()
        conn.close()

    except mysql.connector.Error as err:
        output_str = f'Erro: {err}'
        print(output_str)
    finally:
        # Fechar o cursor e a conexao
        cursor.close()
        conn.close()
        
    return output_str


def handle_update_csv_to_mysql(_db_destino: str, _table_name: str, _load_data_sql: str, _dict_credentials= Dict) -> str:
    # realiza update do arquivo recebido com load_data_sql ja configurado
    # -- print(query_delete_to_mysql)
    print(f'Conectando ao banco de dados: {_db_destino} ')

    if not(_dict_credentials):
        _dict_credentials = dict_env_default

    conn = connect_to_mysql(_db_destino, _dict_env=_dict_credentials)
    cursor = conn.cursor(buffered=True)    

    try:           
        print(f"Iniciando nova carga em {_table_name}.")   
        # Executar o comando LOAD DATA INFILE
        cursor.execute("SET FOREIGN_KEY_CHECKS = 0;")
        cursor.execute("SET unique_checks = 0;")           
        cursor.execute(_load_data_sql)      
        cursor.execute("SET FOREIGN_KEY_CHECKS = 1;")      
        cursor.execute("SET unique_checks = 1;")   
            
        # Confirmar a transação
        conn.commit()
        output_str = "Dados importados com sucesso!"
   
        cursor.close()
        conn.close()
        
    except mysql.connector.Error as err:
        output_str = f"Erro: {err}"
        print(output_str)
    finally:
        # Fechar o cursor e a conexão
        cursor.close()
        conn.close()
        
    return output_str

def handle_load_csv_to_mysql(_file_path: str, _table_name: str, _db_destino: str, _dict_credentials: Dict, _columns_default= list[str] ) -> str:

    # Criando string para load data infile
    load_data_sql = load_data_to_mysql(_file_path=_file_path
											, _table_name=_table_name
                                            , _columns_default=_columns_default
                                    		)
	# Executando load data infile
    lines_updated = handle_update_csv_to_mysql( _db_destino=_db_destino
                                                      , _table_name=_table_name
													  , _load_data_sql=load_data_sql      
                                                      , _dict_credentials=_dict_credentials                                         
                                               	)
    
    return lines_updated

# =========================================================================== #    
# verifica se exite pasta, do contrario cria a pasta
def check_folder_exists(path_check: str) -> str:
    # -- Criar pasta se nao existe

    if not os.path.exists(path_check):
        os.makedirs(path_check)
        response = 'Pasta criada!'    
    else:
        response = 'Pasta ja existe.'
    return response

# funcao para conexao com mysql( usada para desenvolvimento ) 
def handle_conect_db(_mysql_db_name: str) -> create_engine:
    # -- handle_conect_db
    engine = create_engine(f'mysql+pymysql://{MYSQL_USER}:{MYSQL_PASS}@{MYSQL_HOST}:{MYSQL_PORT}/{_mysql_db_name}')
    return engine

# remove objetos invisiveis no texto, e cria saida em formato md5
def handle_strip_string_md5(str1_in: str) -> str:
    # -- Funcao para remover objetos de strings
    # -- str1_in: string de entrada
 
    convert_string = str(str1_in)
    clear_obj = re.sub(r"^\s+|\s+$", "", convert_string)
    strip_string = clear_obj.strip().replace(' ', '').upper()  # equal TRIM
    hash_string_stripped = hashlib.md5(strip_string.encode())
    return hash_string_stripped.hexdigest()

# normaliza cabecalhos de dataframe, padrao, snake_case
def handle_normalize_strings(in_string: str) -> str:
    # -- Remove caracteres especiais, e replace nos caracteres .()/|-,
                            
    target = normalize('NFKD', in_string).encode('ASCII','ignore').decode('ASCII')
    target = target.replace('.','')
    target = target.replace('(','')
    target = target.replace(')','')
    target = target.replace('/','')
    target = target.replace('|','_')
    target = target.replace('-','')
    target = target.replace('.','_')
    target = target.replace(' ','_')
    return target

# convert formato de data, em datetime padrao americano: YYYY-MM-DD HH:MM:SS
def handle_parse_dt(value, tipo_tz="America/Sao_Paulo"):
	# -- Converte para datetime (naive, ja no fuso local).
    # -- funcao existente no processo de Airflow
                            
	if value is None:
		return None
	if isinstance(value, datetime):
		return value.astimezone(ZoneInfo(tipo_tz)).replace(tzinfo=None) if value.tzinfo else value
	s = str(value).strip()
	if not s:
		return None
	for fmt in (
		"%Y%m%d",
		"%Y-%m-%d %H:%M:%S",
		"%Y-%m-%dT%H:%M:%S",
		"%Y-%m-%d",
		"%d/%m/%Y %H:%M:%S",
		"%d/%m/%Y %H:%M",
		"%d/%m/%Y",
	):
		try:
			return datetime.strptime(s, fmt)
		except Exception:
			pass
	return None

                      
# comparando e adcionando colunas faltantes ao dataset original
def handle_headers_comparation(_header_list: List[str], _header_original: List[str]) -> List[str]:
    # Funcao que adciona coluna faltante 
    #---
    #`variables`:

    #_header_list: lista com as nomenclatura padronizadas \n
    #_header_original: lista das colunas do DataFrame analizado
    
    header_fit_output = []
    for i in _header_list:
        if i in _header_original:
            pass
        else:
            header_fit_output.append(i)
    return header_fit_output

# criacao do formato YYYYMM
def handle_ymonth(_dt: datetime) -> int:
    # -- handle_ymonth  
    # com campo data cria-se formato de anomes, exemplo: 12/05/2022: 202205
    s_year = _dt.year
    s_month = _dt.month
    s_ymonth = (s_year * 100 + s_month)
    return s_ymonth                                                        

# conversao de arquivo xml para pandas dataframe
def parse_xml_records(xml_path: Path, record_tag: str) -> pd.DataFrame:
    # Le um XML com estrutura de <record_tag> contendo multiplos <Field name="...">valor</Field>.
    # Retorna DataFrame com colunas = 'name' e valores = text.
    # Alguns arquivos tem raiz adicional; usamos findall diretamente.
    
    tree = ET.parse(xml_path)
    records = tree.findall(record_tag)
    data = []
    for rec in records:
        row = {}
        for field in rec.findall("Field"):
            name = field.attrib.get("name")
            row[name] = field.text
        if row:
            data.append(row)
    return pd.DataFrame(data)

# salva o pandas dataframe em .parquet, por isso a necessidade da lib pyarrow
def save_to_parquet(df: pd.DataFrame, out_path: str) -> None:
    # Salva DataFrame em parquet.
    df.to_parquet(out_path, compression='snappy', engine='pyarrow')                         

if __name__ == '__main__':
    print('Teste - ok!')


""")
    print(f'Criado: {init_file_path}')

    # Criar e adicionar conteúdo ao arquivo .env
    init_file_env = ''.join(".env")
    if not os.path.exists(init_file_env):
        with open(init_file_env, 'w') as init_file_env:
            init_file_env.write("""\
PATH_ROOT=./src
PATH_BUCKET=./src/bucket
PATH_EXTENSIONS=./src/extensions
ENV_BRONZE=./src/bucket/bronze
ENV_SILVER=./src/bucket/silver
ENV_GOLD=./src/bucket/gold
MYSQL_USER=usuario
MYSQL_PASS=password
MYSQL_HOST=localhost
MYSQL_PORT=3306
HOST_LOC=localhost
USER_LOC=usuario01
PASS_LOC=usuario01
PORT_LOC=3306
DUCKDB_DATABASE=./src/database/db_local.duckdb

""")
    print(f'Criado: {init_file_env}')

    # Criar e adicionar conteúdo ao arquivo .vscode/settings.json
    init_file_vscode = os.path.join(base_path, ".vscode/settings.json")
    if not os.path.exists(init_file_vscode):
        with open(init_file_vscode, 'w') as init_file_vscode:
            init_file_vscode.write("""\
{
    "files.exclude": {
    "**/.pyc": {
        "when": "$(basename).py"
    },
    "**/__pycache__": true,
    "**/.pytest_cache": true
    },
    // editor
    "editor.wordWrap": "off",
    "editor.fontSize": 15,
    "editor.lineHeight": 24,
    "editor.tabSize": 4,
    "editor.formatOnPaste": true,
    "files.autoSave": "afterDelay",
    "editor.tabCompletion": "on",
    // explorer
    "explorer.compactFolders": false,
    // workbench
    "workbench.editor.enablePreview": false,
    "workbench.sideBar.location": "left",
    "workbench.startupEditor": "none", // <== Não abrir o arquivo de boas vindas 
    "workbench.iconTheme": "material-icon-theme",   
    "workbench.colorTheme": "Dracula Theme",
    "workbench.tree.indent": 18,
    // referente a jupyter notebook, localizar .venv para o projeto
    "python.terminal.activateEnvironment": true,
    "jupyter.jupyterServerType": "local",
    "jupyter.askForKernelRestart": false,
    // =================== CONFIG: arquivos e pastas ===================
    "files.autoSaveDelay": 50000, // <== Salvar automaticamente
    // terminal
    "terminal.integrated.fontSize":14,
    "terminal.integrated.profiles.windows": {
        "Git Bash": {
        "source": "Git Bash"
        }
    },
    "terminal.integrated.defaultProfile.windows": "Git Bash",
    // zen mode
    "zenMode.hideActivityBar": true,
    "zenMode.silentNotifications": true,
    "zenMode.fullScreen": false,
    "zenMode.centerLayout": false,
    "zenMode.hideLineNumbers": false,
    // =================== CONFIG: extensão material icon file ===================
    "material-icon-theme.folders.associations": {
        "*.parquet": "regedit",
        "*.xlsb": "table",                           
        "*.sql": "parcel",                
        "*.jpg": "luau",            
    },
    // =================== CONFIG: extensão material icon folder ===================
    "material-icon-theme.folders.associations": {
        // bucket-folder
        "bucket": "scala",
        "bronze": "components",
        "silver": "constant",
        "gold": "aws",
        // models-folder
        "models": "unity",
        "marts": "plugin",
        "sources": "keys",
        "staging": "tasks",
       // utils-folder
        "utils": "tools",
    },
}


""")
    print(f'Criado: {init_file_vscode}')

        
    # Cria um novo notebook
    create_notebook = nbf.v4.new_notebook()

    # Cria células no notebook
    cells = [
        nbf.v4.new_markdown_cell("### Start notebook with DuckDb"),
        nbf.v4.new_code_cell("""
# Init Notebook
#-----------------------------------------------------------------------------------------------------------------------------------------
# Aprendizado Extra: Convenções de Nomenclatura ( Naming Conventions )
# snake_case: Todas as letras são minúsculas e as palavras são separadas por underline (_). Muito comum em Python e bancos de dados.
# camelCase: A primeira letra é minúscula, mas a primeira letra de cada palavra subsequente é maiúscula (exemploCamelCase).
# PascalCase: Semelhante ao camelCase, mas a primeira letra de todas as palavras é maiúscula (ExemploPascalCase). Muito usado para classes.
# kebab-case: Todas as letras são minúsculas e as palavras são separadas por hífen (exemplo-kebab-case). Comum em URLs e CSS.
# SCREAMING_SNAKE_CASE (ou Macro Case): Todas as letras maiúsculas, separadas por underline (EXEMPLO_SCREAMING). Comum para constantes. 
#-----------------------------------------------------------------------------------------------------------------------------------------         
# import magic_duckdb                         # lib para queries duckdb , adcional
import os                                   # lib sistema operacional              
import duckdb                               # lib duckdb, manipulação até 50gb
import src.utils as utils                   # configs and functions
import pandas as pd                         # lib para manipulação de dados até 10gb
import warnings                             # lib para controle de warnings
                        
warnings.filterwarnings('ignore')           # ignore warnings ( avisos ignorados )
pd.set_option('display.max_columns',None)   # show para todas as colunas do dataframe/dataset
pd.set_option('display.max_colwidth', None) # show para todos os textos na coluna do dataframe/dataset
pd.set_option('display.max_rows',None)      # show para todas as linhas

# TODO: criação de conexão duckdb.local, caso não exista cria-se um novo database
con = duckdb.connect(utils.DUCKDB_DATABASE) # type: ignore

# TODO: modelo de variavel para arquivos .parquet
file_parquet = ''.join(f'{utils.ENV_BRONZE}/files*.parquet') 
%load_ext magic_duckdb                                         
"""),
    nbf.v4.new_code_cell(f"""
# modelo de drop e create table, baseado no arquivo .parquet
con.execute('drop table if exists tbl_file_parquet')
con.execute(f"create table tbl_file_parquet as select * from '{{file_parquet}}' ")
    """),
    nbf.v4.new_code_cell(f"""
%%dql -co con 
--# Utilizando Magic DuckDb
select 
    a.database_name
    , a.table_name
    , a.estimated_size
    , a.column_count
    , a.index_count
from duckdb_tables() a
    """),
    nbf.v4.new_code_cell("print('Continue....')")
    ]

    # Adiciona as células ao notebook
    create_notebook['cells'] = cells

    # Salva o notebook no formato .ipynb
    init_file_notebook = os.path.join(base_path, "./duckdb_local.ipynb")
    if not os.path.exists(init_file_notebook):
        with open(init_file_notebook, 'w', encoding='utf-8') as f:
            nbf.write(create_notebook, f)

    print(f"Notebook -- duckdb_local.ipynb -- Created Successfully!")


# Exemplo de uso
criar_pasta_template(".")

