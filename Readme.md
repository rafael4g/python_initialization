# Projeto ENV_INIT

Este projeto contém um script para a criação automática de uma estrutura de diretórios e arquivos pré-definidos para um ambiente de dados. Além disso, configura um notebook `DuckDB` para manipulação de dados.

## Funcionalidades

- Cria uma estrutura de diretórios padronizada para o projeto, incluindo pastas para `src`, `utils`, `models`, `bucket` (com níveis de bronze, silver e gold), entre outras.
- Cria arquivos `.gitkeep` e `__init__.py` nos diretórios necessários para manter a estrutura de diretórios em sistemas de controle de versão.
- Gera um arquivo `.env` com variáveis de ambiente, como credenciais do MySQL e caminho do banco de dados DuckDB.
- Configura o editor com preferências no arquivo `.vscode/settings.json`.
- Como indicação, instale a extensão do VSCODE Material Icon Theme `EXTENSÃO VSCODE`.
- Cria um notebook (`duckdb_local.ipynb`) para interação com um banco de dados local DuckDB.

## Como utilizar

### 1. Estrutura de diretórios

O script cria a seguinte estrutura de diretórios:

├── .vscode/ \
│ └── settings.json \
├── src/ \
│ ├── __init__.py \
│ ├── bucket/ \
│ │ ├── bronze/ \
│ │ │ └── .gitkeep \
│ │ ├── silver/ \
│ │ │ └── .gitkeep \
│ │ └── gold/ \
│ │ └── .gitkeep \
│ ├── database/ \
│ │ └── .gitkeep \
│ ├── utils/ \
│ └ └── __init__.py \
├── .env \
└── duckdb_local.ipynb

```python
# Para modularização de pastas
__init__.py

# Funções em ./utils, breve descrição
def check_folder_exists(path_check: str) -> str: ... # criar pasta se nao existe
def handle_conect_db(_mysql_db_name: str) -> create_engine: ... # conexao com mysql/mariadb( usada para desenvolvimento )
def handle_strip_string(str1_in: str) -> str: ... # remove objetos não visiveis no texto, e cria saida em formato md5
def handle_normalize_strings(in_string: str) -> str: ... # normaliza cabecalhos de dataframe, padrao, snake_case
def handle_parse_dt(value, tipo_tz="America/Sao_Paulo"): ... # padroniza datas no formato padrao: YYYY-MM-DD HH:MM:SS
def handle_headers_comparation(_header_list: List[str], _header_original: List[str]) -> List[str]: ... # compara e adcionando colunas faltantes ao dataset original
def handle_ymonth(_dt: datetime) -> int: ... # cria formato YYYYMM ( ANOMES )
def parse_xml_records(xml_path: Path, record_tag: str) -> pd.DataFrame: ... # conversão de arquivo xml para pandas dataframe com record_tag especifico
def save_to_parquet(df: pd.DataFrame, out_path: str) -> None: ... # convert pandas dataframe em .parquet
```

### 2. Variáveis de ambiente

Um arquivo `.env` é gerado automaticamente com as seguintes variáveis:

PATH_ROOT=./src \
PATH_BUCKET=./src/bucket \
PATH_EXTENSIONS=./src/extensions \
ENV_BRONZE=./src/bucket/bronze \
ENV_SILVER=./src/bucket/silver \
ENV_GOLD=./src/bucket/gold \
MYSQL_USER=usuario \
MYSQL_PASS=password \
MYSQL_HOST=localhost \
MYSQL_PORT=3306 \
DUCKDB_DATABASE=./src/database/db_local.duckdb


Você pode ajustar as variáveis de acordo com seu ambiente.

### 3. Arquivo de configuração do VSCode

O arquivo `.vscode/settings.json` é gerado com as seguintes configurações:

- Salvamento automático após 50 segundos de inatividade.
- Preferências de tema, ícones e fonte.
- Configurações de exclusão de arquivos temporários como `.pyc` e `__pycache__`.

### 4. Notebook DuckDB

O script também cria um notebook `duckdb_local.ipynb`, que inicia uma conexão com o banco de dados DuckDB e permite a execução de queries em arquivos `.parquet`.

## Como executar o script

1. Clone este repositório.
2. Crie um ambiente virtual.
   ```python
   # linux
   ./pasta_do_projeto:~$ python -m venv .venv

   # Windows
   C:\pasta_do_projeto> python -m venv .venv

   ```
3. Ative o ambiente   
   ```python
   # linux
   ./pasta_do_projeto:~$ source .venv/bin/activate

   # Windows
   ./pasta_do_projeto:~$ . .venv/scripts/activate.ps1
   ```
4. Instale as dependências (caso necessário):
   ```bash
   pip install -r requirements.txt
   ```
   ##### LIBs
   - ipykernel ( conexão de notebooks com kernel python )
   - nbformat ( manipular notebook )
   - openpyxl ( manipular excel )
   - magic_duckdb ( para queries amigaveis em duckdb )
   - mysql-connector ( para conexão com mysql/mariaDb )
   - numpy ( para funções matemáticas )
   - sqlalchemy ( para conexão com diversos bancos de dados como mysql, postgresql, sql server, etc... )
   - python-decouple ( para setar variaveis de ambiente )
   - pandas ( manipulação de dados )
   - pyarrow ( para estrutura parquet )
   - duckdb ( criação de banco de dados local, e manipulação de dados até 50gb )

5. Execute o script Python:
   ```bash
   # Isso criará a estrutura de pastas, arquivos e o notebook( ``inicial`` ).
    python start.py
    ```

6. Magic DuckDB ( Adcional )
   - Extenção para manipulação de sql utilizando a expressão %%dql no inicio da celula do notebook
   - Utilizamos no próprio banco local com a flag `%%dql -co -con`
      - `-co` para conexão em variavel de conexão ao banco de dados.
      - `con` nossa variavel de conexão ao banco de dados local.

   ```python
   %%dql -co con 
   select 
      a.database_name
      , a.table_name
      , a.estimated_size
      , a.column_count
      , a.index_count
   from duckdb_tables() a
   ```

7. Extensões para Duckdb ( Adcional )
   - baixe conforme sua versão, neste projeto v1.4.3
   ```bash
   # baixe o zip no site abaixo
   http://extensions.duckdb.org/v1.4.3/windows_amd64/spatial.duckdb_extension.gz

   # Ou Instale diretamente com o comando abaixo, direto da celula do notebook
   con.execute('INSTALL spatial')
   con.execute('LOAD spatial')
   ```

8. Abra o notebook duckdb_local.ipynb no Jupyter e execute as células para manipulação do banco de dados DuckDB.

## Requisitos
- Python 3.10+
- Pacotes: nbformat, openpyxl, magic_duckdb, sqlalchemy, decouple, pandas, duckdb, pyarrow, ipykernel
- versões no arquivo requirements.txt

## Contribuição
Sinta-se à vontade para abrir um Pull Request ou sugerir melhorias para o projeto. <br>
Para amantes de SQL, utilizar a extenxão [Pretty SQL Alias](https://marketplace.visualstudio.com/items?itemName=LinZhang.pretty-sql-alias) ( para formatação dos scripts alinhados com seus ALIAS )

## Licença
Este projeto está sob a licença MIT.

