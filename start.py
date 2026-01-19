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
        "src/bucket/gold"
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

    # Criar e adicionar conteúdo ao arquivo src/utils/__init__.py
    init_file_path = os.path.join(base_path, "src/utils/__init__.py")
    if not os.path.exists(init_file_path):
        with open(init_file_path, 'w') as init_file:
            init_file.write("""\
from typing import List
from sqlalchemy import create_engine, text
from unicodedata import normalize
from datetime import datetime
import re
import os
import hashlib
from decouple import config

MYSQL_USER = config('MYSQL_USER')
PATH_API = config('PATH_API')
MYSQL_PASS = config('MYSQL_PASS')
MYSQL_HOST = config('MYSQL_HOST')
MYSQL_PORT = config('MYSQL_PORT')
MYSQL_DEFAULT_DB = config('MYSQL_DEFAULT_DB')
PATH_BUCKET = config('PATH_BUCKET')
PATH_EXTENSIONS = config('PATH_EXTENSIONS')
ENV_BRONZE = config('ENV_BRONZE')
DUCKDB_DATABASE = config('DUCKDB_DATABASE')
                            
DATETIME_HOUR_MINUTES = datetime.strftime(datetime.now(), "%Y-%m-%d %H:%M:%S")

def check_folder_exists(path_check: str) -> str:
    # -- Criar pasta se não existe

    if not os.path.exists(path_check):
        os.makedirs(path_check)
        response = 'Pasta criada!'    
    else:
        response = 'Pasta já existe.'
    return response
     
def handle_conect_db(_mysql_db_name: str) -> create_engine:
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
    target = target.replace('|','_')
    target = target.replace('-','')
    target = target.replace('.','_')
    target = target.replace(' ','_')
    return target

def handle_parse_dt(value, tipo_tz="America/Sao_Paulo"):
	#Tenta converter para datetime (naive, ja no fuso local).
    # função existente no processo de Airflow
	if value is None:
		return None
	if isinstance(value, datetime):
		return value.astimezone(ZoneInfo(tipo_tz)).replace(tzinfo=None) if value.tzinfo else value
	s = str(value).strip()
	if not s:
		return None
	for fmt in (
		'%Y%m%d',
		'%Y-%m-%d %H:%M:%S',
		'%Y-%m-%dT%H:%M:%S',
		'%Y-%m-%d',
		'%d/%m/%Y %H:%M:%S',
		'%d/%m/%Y %H:%M',
		'%d/%m/%Y',
	):
		try:
			return datetime.strptime(s, fmt)
		except Exception:
			pass
	return None

                      
# comparando e adcionando colunas faltantes ao dataset original
def handle_headers_comparation(_header_list: List[str], _header_original: List[str]) -> List[str]:
    ''' Função que adciona coluna faltante 

    ---
    `variables`:

    _header_list: lista com as nomenclatura padronizadas \n
    _header_original: lista das colunas do DataFrame analizado
    '''
    header_fit_output = []
    for i in _header_list:
        if i in _header_original:
            pass
        else:
            header_fit_output.append(i)
    return header_fit_output

def handle_ymonth(_dt: datetime) -> int:
    # -- handle_ymonth  
    s_year = _dt.year
    s_month = _dt.month
    s_ymonth = (s_year * 100 + s_month)
    return s_ymonth                                                        


def parse_xml_records(xml_path: Path, record_tag: str) -> pd.DataFrame:
    '''
    Lê um XML com estrutura de <record_tag> contendo múltiplos <Field name="...">valor</Field>.
    Retorna DataFrame com colunas = 'name' e valores = text.
    '''
    tree = ET.parse(xml_path)
    # Alguns arquivos têm raiz adicional; usamos findall diretamente.
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

def save_to_parquet(df: pd.DataFrame, out_path: str):
    """
    Salva DataFrame em parquet.
    """    
    df.to_parquet(out_path, compression='snappy', engine='pyarrow')	                                                    

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
PATH_BUCKET=./src/bucket
ENV_BRONZE=./src/bucket/bronze
ENV_SILVER=./src/bucket/silver
ENV_GOLD=./src/bucket/gold
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
import os                             
import duckdb
import src.utils as utils                  # configs and functions
import magic_duckdb 
import pandas as pd
import warnings
                        
warnings.filterwarnings('ignore')
pd.set_option('display.max_columns',None)
pd.set_option('display.max_colwidth', None) 
pd.set_option('display.max_rows',None)

con = duckdb.connect(utils.DUCKDB_DATABASE) # type: ignore


file_parquet = ''.join(f'{utils.ENV_BRONZE}/files*.parquet') 
%load_ext magic_duckdb                                         
"""),
    nbf.v4.new_code_cell(f"""
con.execute('drop table if exists tbl_file_parquet')
con.execute(f"create table tbl_file_parquet as select * from '{{file_parquet}}' ")
    """),
    nbf.v4.new_code_cell(f"""
#-- Utilizando Magic DuckDb
%%dql -co con 
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

