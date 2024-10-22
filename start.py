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
        "src/bucket/bronze/csv",
        "src/bucket/bronze/excel",
        "src/bucket/silver",
        "src/bucket/gold",
        "src/models/marts",
        "src/models/sources",
        "src/models/staging"
    ]

    for pasta in estrutura_pastas:
        caminho_completo = os.path.join(base_path, pasta)
        os.makedirs(caminho_completo, exist_ok=True)
        print(f'Criado: {caminho_completo}') 

    # Criar e adicionar conteúdo ao arquivo src/bucket/bronze/csv/.gitkeep
    init_file_bucket_bronze_csv = os.path.join(base_path, "src/bucket/bronze/csv/.gitkeep")
    if not os.path.exists(init_file_bucket_bronze_csv):
        with open(init_file_bucket_bronze_csv, 'w') as init_file:
            init_file.write("""""")   

    # Criar e adicionar conteúdo ao arquivo src/bucket/bronze/excel/.gitkeep
    init_file_bucket_bronze_excel = os.path.join(base_path, "src/bucket/bronze/excel/.gitkeep")
    if not os.path.exists(init_file_bucket_bronze_excel):
        with open(init_file_bucket_bronze_excel, 'w') as init_file:
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

    # Criar e adicionar conteúdo ao arquivo src/models/marts/__init__.py
    init_file_marts = os.path.join(base_path, "src/models/marts/__init__.py")
    if not os.path.exists(init_file_marts):
        with open(init_file_marts, 'w') as init_file:
            init_file.write("""""")
    # Criar e adicionar conteúdo ao arquivo src/models/sources/__init__.py
    init_file_sources = os.path.join(base_path, "src/models/sources/__init__.py")
    if not os.path.exists(init_file_sources):
        with open(init_file_sources, 'w') as init_file:
            init_file.write("""""")
    # Criar e adicionar conteúdo ao arquivo src/models/staging/__init__.py
    init_file_staging = os.path.join(base_path, "src/models/staging/__init__.py")
    if not os.path.exists(init_file_staging):
        with open(init_file_staging, 'w') as init_file:
            init_file.write("""""")

    # Criar e adicionar conteúdo ao arquivo src/models/staging/query.py
    init_file_staging_query = os.path.join(base_path, "src/models/staging/query.py")
    if not os.path.exists(init_file_staging_query):
        with open(init_file_staging_query, 'w') as init_file:
            init_file.write("""\
def query_select(_name_table: str, _limit: int =10) -> str:
	# -- query_select.sql
    query = f\"\"\"
	with
		cte_query_select as (
            select *
            from s1.{_name_table}
            limit {_limit}
        )
            select * from cte_query_select
    \"\"\"
    return query
""")

    # Criar e adicionar conteúdo ao arquivo src/models/staging/create.py
    init_file_staging_create = os.path.join(base_path, "src/models/staging/create.py")
    if not os.path.exists(init_file_staging_create):
        with open(init_file_staging_create, 'w') as init_file:
            init_file.write("""\
import pandas as pd
                                                        
def tbl_canais(path_filename: str) -> pd.DataFrame:
    df_canais = pd.read_excel(path_filename, engine='openpyxl', sheet_name='Planilha1')
    df_canais.rename(columns=lambda x: x.lower(),inplace=True)
    df_canais.fillna(value='', inplace=True)
    
    return df_canais

def tbl_expansao(path_filename: str) -> pd.DataFrame:
    df_expansao = pd.read_excel(path_filename
                                , engine='openpyxl'
                                , sheet_name='default_1'
                                , dtype={'COD_IBGE': int, 'QTD': int})
    df_expansao.rename(columns=lambda x: x.lower(),inplace=True)
    df_expansao.fillna(value='', inplace=True)
    
    return df_expansao 
""")            

    # Criar e adicionar conteúdo ao arquivo src/models/staging/update.py
    init_file_staging_update = os.path.join(base_path, "src/models/staging/update.py")
    if not os.path.exists(init_file_staging_update):
        with open(init_file_staging_update, 'w') as init_file:
            init_file.write("""
def update_table(_name_table: str, _limit: int =10) -> str: ...
""")

    # Criar e adicionar conteúdo ao arquivo src/utils/__init__.py
    init_file_path = os.path.join(base_path, "src/utils/__init__.py")
    if not os.path.exists(init_file_path):
        with open(init_file_path, 'w') as init_file:
            init_file.write("""\
from sqlalchemy import create_engine
from unicodedata import normalize
from datetime import datetime
import re
import hashlib
from decouple import config

MYSQL_USER = config('MYSQL_USER')
MYSQL_PASS = config('MYSQL_PASS')
MYSQL_HOST = config('MYSQL_HOST')
MYSQL_PORT = config('MYSQL_PORT')
ENV_BRONZE = config('ENV_BRONZE')
DUCKDB_DATABASE = config('DUCKDB_DATABASE')

def handle_conect_db(_mysql_db_name: str):
    # -- handle_conect_db
    engine = create_engine(f'mysql+pymysql://{MYSQL_USER}:{MYSQL_PASS}@{MYSQL_HOST}:{MYSQL_PORT}/{_mysql_db_name}')
    return engine

def handle_strip_string(str1_in: str) -> str:
    # -- Função para remover objetos de strings
    # -- str1_in: string de entrada
 
    convert_string = str(str1_in)
    clear_obj = re.sub(r"^\s+|\s+$", "", convert_string)
    strip_string = clear_obj.strip().replace(' ', '').upper()  # equal TRIM
    hash_string_stripped = hashlib.md5(strip_string.encode())
    return hash_string_stripped.hexdigest()

def handle_normalize_strings(in_string: str) -> str:
    # -- handle_normalize_strings
    target = normalize('NFKD', in_string).encode('ASCII','ignore').decode('ASCII')
    target = target.replace('.','')
    target = target.replace('(','')
    target = target.replace(')','')
    target = target.replace('/','')
    target = target.replace('-','')
    return target

# comparando e adcionando colunas faltantes ao dataset original
def handle_headers_comparation(header_list, header_original) -> list:
    # -- handle_headers_comparation
    new_list = []
    for i in header_list:
        if i in header_original:
            pass
        else:
            new_list.append(i)
    return new_list

def handle_without_zero(in_string: str) -> str:
    # -- handle_without_zero
    _str_in = str(in_string)
    target = _str_in.replace('.0', '')
    target = target.strip()
    if target == '-3':
        str_output = 0
    else:
        str_output = target
    return str_output

def handle_ymonth(_dt: datetime) -> int:
    # -- handle_ymonth  
    s_year = _dt.year
    s_month = _dt.month
    s_ymonth = (s_year * 100 + s_month)
    return s_ymonth                                                        

if __name__ == '__main__':
    print('Tested!')


""")
    print(f'Criado: {init_file_path}')

    # Criar e adicionar conteúdo ao arquivo .env
    init_file_env = ''.join(".env")
    if not os.path.exists(init_file_env):
        with open(init_file_env, 'w') as init_file_env:
            init_file_env.write("""\
PATH_ROOT=./src
ENV_BRONZE=./src/bucket/bronze
MYSQL_USER=usuario
MYSQL_PASS=password
MYSQL_HOST=localhost
MYSQL_PORT=3306
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
    // =================== CONFIG: extensão material icon theme ===================
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
import src.utils as utils                  # configs and functions
import src.models.marts as mql             # marts options
import src.models.sources as soql          # sources options
import src.models.staging.query as qsql    # staging query
import src.models.staging.update as update # staging update
import src.models.staging.create as create # staging create
import duckdb
import pandas as pd
import warnings
                        
warnings.filterwarnings('ignore')
pd.set_option('display.max_columns',None)
pd.set_option('display.max_rows',None)

con = duckdb.connect(utils.DUCKDB_DATABASE) # type: ignore
con.execute('CREATE SCHEMA IF NOT EXISTS s1')

file_parquet = ''.join(f'{utils.ENV_BRONZE}/files*.parquet')                                        
"""),
    nbf.v4.new_code_cell(f"""
con.execute('drop table if exists s1.tbl_file_parquet')
con.execute(f"create table s1.tbl_file_parquet as select * from '{{file_parquet}}' ")
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

