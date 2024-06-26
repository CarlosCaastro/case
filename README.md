﻿# Projeto de Extração e Transformação de Dados do GitHub

## Execução do Projeto

### Requisitos
 - Docker
 - Conta no GitHub e token de acesso

### Clonar o repositório

```
git clone https://github.com/CarlosCaastro/case.git
```

### Configurar credenciais.

Atualizar o arquivo [constantes.py](/credentials/constantes.py) com o usuário GitHub e token de acesso.

### Imagem Docker
#### Build
```
docker build -t pyspark-app .
```

#### Run 
Caso queira o arquivo csv para posterior análise.
```
docker run -d -v <DIRETORIO_DO_REPOSITORIO>:/app pyspark-app:latest
```

Caso queira apenas executar o arquivo main.py, os arquivos irão persistir apenas no container.
```
docker run pyspark-app:latest
```

## Estrutura do Projeto
```
Case
├── credentials
│   └── constantes.py
├── modules
│   ├── extract.py
│   ├── loader.py
│   ├── test.py
│   └── transform.py
├── output
├── dockerfile
├── main.py
└── Ifood - People Analytics - Modularization.ipynb
```

### Constantes.py
No arquivo [constantes.py](/credentials/constantes.py) contém as definições das constantes que serão utilizada na execução do projeto, seja credenciais de token, caminho ou usuário que deseja ter as informações dos seguidores.

### Extract.py

[Este módulo](/modules/extract.py) contém classes e funções para extração de dados da API do GitHub.

#### get_github_user_info(login, token):
Função que retorna informações do usuário do GitHub como nome, empresa, blog, e-mail, biografia, repositórios públicos, seguidores, seguindo e data de criação.

> get_followers(self)   
Método que extrai a lista de seguidores do usuário.

> enrich_dataframe_with_github_info(self, df)  
Método que enriquece o DataFrame com informações adicionais dos seguidores.

> execute_extract_api(self)  
Método que executa a extração de dados da API do GitHub e retorna um DataFrame enriquecido.

> extract_csv(self, path)  
Método que lê um arquivo CSV e retorna um DataFrame.

> get_count_followers(self)  
Método que retorna a contagem de seguidores do usuário no GitHub.

### Loader.py
[Este módulo](/modules/loader.py) contém a classe para carregar e salvar os dados transformados.

> save_to_csv(self, df: DataFrame)  
Método que salva o DataFrame em um arquivo CSV no caminho especificado.

### Test.py
[Este módulo](/modules/test.py) contém a classe para execução de testes e validação dos dados.

> clean_company_check(self, df)  
Método que verifica se a coluna "company" ainda contém o caractere '@' ou não.

> date_format_check(self, df)  
Método que verifica se a coluna "created_at" está no formato de data dd/mm/yyyy.

> volumetry_check(self, df)   
Método que verifica se a contagem de seguidores no DataFrame corresponde à contagem atual de seguidores no GitHub.

### Transform.py
Este módulo contém a classe para transformação dos dados extraídos, com base nos requerementos do case.

> transform(df: DataFrame)   
Método estático que transforma o DataFrame, removendo o caractere '@' da coluna "company" e formatando a coluna "created_at" para o formato dd/MM/yyyy.

## Output
O output desse projeto é um arquivo CSV conténdo as informações dos seguidores do usuário, como nome, companhia, blog, email. bio, quantidade de repositórios publicos, quantidade de seguidores, quantidade de usuários que está seguindo e quando foi criado.

A pasta [output](/output/), contém um exemplo de saída.


## Dockerfile
O arquivo dockerfile é responsável por criar uma imagem do Docker utilizando uma imagem do Python3.8 - Slim e instalar as dependencias necessárias como pyspark e requests para executar o arquivo [main.py](/main.py)

## Main.py
Arquivo principal que orquestra a execução de todo o processo ETL e a validação dos dados.



