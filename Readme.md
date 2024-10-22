# Projeto ENV_INIT

Este projeto contém um script para a criação automática de uma estrutura de diretórios e arquivos pré-definidos para um ambiente de dados. Além disso, configura um notebook `DuckDB` para manipulação de dados.

## Funcionalidades

- Cria uma estrutura de diretórios padronizada para o projeto, incluindo pastas para `src`, `utils`, `models`, `bucket` (com níveis de bronze, silver e gold), entre outras.
- Cria arquivos `.gitkeep` e `__init__.py` nos diretórios necessários para manter a estrutura de diretórios em sistemas de controle de versão.
- Gera um arquivo `.env` com variáveis de ambiente, como credenciais do MySQL e caminho do banco de dados DuckDB.
- Configura o editor com preferências no arquivo `.vscode/settings.json`.
- Cria um notebook (`duckdb_local.ipynb`) para interação com um banco de dados local DuckDB.

## Como utilizar

### 1. Estrutura de diretórios

O script cria a seguinte estrutura de diretórios:

├── .vscode/ \
│ └── settings.json \
├── src/ \
│ ├── init.py \
│ ├── bucket/ \
│ │ ├── bronze/ \
│ │ │ ├── csv/ \
│ │ │ │ └── .gitkeep \
│ │ │ ├── excel/ \
│ │ │ │ └── .gitkeep \
│ │ ├── silver/ \
│ │ │ └── .gitkeep \
│ │ └── gold/ \
│ │ └── .gitkeep \
│ ├── database/ \
│ │ └── .gitkeep \
│ ├── models/ \
│ │ ├── marts/ \
│ │ │ └── init.py \
│ │ ├── sources/ \
│ │ │ └── init.py \
│ │ ├── staging/ \
│ │ │ ├── init.py \
│ │ │ ├── query.py \
│ │ │ ├── create.py \
│ │ │ └── update.py \
│ ├── utils/ \
│ └── init.py \
├── .env \
└── duckdb_local.ipynb


### 2. Variáveis de ambiente

Um arquivo `.env` é gerado automaticamente com as seguintes variáveis:

PATH_ROOT=./src \
ENV_BRONZE=./src/bucket/bronze \
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
2. Instale as dependências (caso necessário):
   ```bash
   pip install nbformat openpyxl sqlalchemy python-decouple pandas duckdb
   ```

3. Execute o script Python:
   ```bash
    python start.py
    ```

Isso criará a estrutura de pastas, arquivos e o notebook.

4. Abra o notebook duckdb_local.ipynb no Jupyter e execute as células para manipulação do banco de dados DuckDB.

## Requisitos
- Python 3.7+
- Pacotes: nbformat, openpyxl, sqlalchemy, decouple, pandas, duckdb

## Contribuição
Sinta-se à vontade para abrir um Pull Request ou sugerir melhorias para o projeto.

## Licença
Este projeto está sob a licença MIT.

