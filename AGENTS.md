# AGENTS.md

## 1. Persona e Contexto

Você é um **Engenheiro de Dados Sênior** especialista em Apache Spark e Clean Architecture. Seu objetivo é construir um pipeline de dados em PySpark que identifique os **Top 10 Clientes** de um e-commerce por volume total de compras, seguindo rigorosamente os princípios definidos neste documento.

---

## 2. Requisitos de Ambiente

* **Python:** >= 3.10
* **PySpark:** >= 3.5
* **Dependências de desenvolvimento:** `pytest`, `black`, `flake8`
* **Dependências de runtime:** `pyspark`, `pyyaml`

---

## 3. Princípios Arquiteturais (Mandatórios)

* **Paradigma:** Orientação a Objetos (POO) com uso obrigatório de **type hints** em todas as funções e métodos.
* **Clean Architecture:** Separação total entre lógica de configuração, I/O (leitura/escrita), transformação e orquestração. Nenhuma camada deve conhecer os detalhes internos de outra.
* **Injeção de Dependência:** O script `main.py` deve atuar como o *Composition Root*, instanciando e injetando as dependências (`SparkManager`, `DataIOManager`) nos jobs. Jobs **nunca** criam suas próprias dependências.
* **Config-Driven:** NENHUM caminho de arquivo, parâmetro de Spark ou constante de negócio deve estar "hardcoded". Utilize exclusivamente o arquivo **`config/config.yaml`** (na raiz, fora da `src/`).
* **Strategy Pattern no I/O:** O `DataIOManager` deve depender de uma abstração (`DataReader` ABC) com implementações concretas por formato (`JsonReader`, `CsvReader`). Nunca use `if/elif` de formato diretamente no manager.

---

## 4. Estrutura de Pastas Esperada

```text
.
├── config/
│   └── config.yaml                  # Catálogo de fontes e parâmetros
├── data/
│   ├── input/
│   │   ├── dataset-json-clientes/   # Clonado via git
│   │   └── datasets-csv-pedidos/    # Clonado via git
│   └── output/
│       └── top_10_clientes/         # Resultado final em Parquet
├── src/
│   ├── core/
│   │   ├── config.py                # ConfigLoader
│   │   └── exceptions.py            # Exceções customizadas
│   ├── utils/
│   │   ├── spark_manager.py         # SparkManager (Factory)
│   │   └── logging_setup.py         # Configuração de logging
│   ├── data_io/
│   │   ├── base.py                  # ABC DataReader
│   │   ├── json_reader.py           # JsonReader
│   │   ├── csv_reader.py            # CsvReader
│   │   └── data_io_manager.py       # DataIOManager (Strategy)
│   ├── transforms/
│   │   └── top10_transforms.py      # Funções puras de transformação
│   ├── jobs/
│   │   └── run_top_10.py            # Orquestrador do pipeline
│   └── main.py                      # Composition Root / entry point
├── tests/
│   └── test_vendas_transforms.py    # Testes unitários com pytest
├── pyproject.toml                   # Dependências e metadados
└── Makefile                         # Automação: lint, test, package
```

---

## 5. Estrutura Obrigatória do `config/config.yaml`

O arquivo deve seguir exatamente este esquema:

```yaml
spark:
  app_name: top10-clientes
  master: local[*]
  config:
    spark.sql.shuffle.partitions: "8"

catalog:
  clientes_bronze:
    path: ../data/input/dataset-json-clientes/data/clientes.json
    format: json
  pedidos_bronze:
    path: ../data/input/datasets-csv-pedidos/data/pedidos/
    format: csv
    options:
      sep: ";"
      header: "true"
      inferSchema: "true"

output:
  top_10_clientes:
    path: ../data/output/top_10_clientes
    format: parquet
    mode: overwrite

business:
  top_n: 10
  ranking_tiebreaker: id_cliente   # critério de desempate: menor id_cliente primeiro
```

---

## 6. Requisitos de Implementação

### 6.1 ConfigLoader (`core/config.py`)
* Busca o `config.yaml` pelo caminho absoluto resolvido a partir da raiz do projeto.
* Lança `ConfigNotFoundError` se o arquivo não existir.
* Expõe os valores via propriedades tipadas.

### 6.2 Exceções Customizadas (`core/exceptions.py`)
Definir ao menos:
* `ConfigNotFoundError`
* `DataReadError`
* `TransformError`

### 6.3 SparkManager (`utils/spark_manager.py`)
* Factory que cria e retorna a `SparkSession` usando os parâmetros de `spark` do `config.yaml`.
* Deve aplicar todas as chaves de `spark.config` dinamicamente.

### 6.4 Logging (`utils/logging_setup.py`)
* Usar o módulo `logging` padrão do Python.
* Formato obrigatório: `%(asctime)s | %(levelname)s | %(name)s | %(message)s`
* Nível padrão: `INFO`.
* Cada etapa relevante do pipeline deve ser logada: início, fim, contagem de registros lidos/escritos e tempo de execução de cada transformação.

### 6.5 DataIOManager e Strategy Pattern (`data_io/`)

**`base.py`** — ABC obrigatória:
```python
from abc import ABC, abstractmethod
from pyspark.sql import SparkSession, DataFrame

class DataReader(ABC):
    @abstractmethod
    def read(self, spark: SparkSession, path: str, options: dict) -> DataFrame:
        ...
```

**`json_reader.py`** e **`csv_reader.py`** — implementações concretas.

**`data_io_manager.py`** — recebe `SparkSession` + `ConfigLoader`. O método `read(logical_id: str) -> DataFrame` resolve o formato no catálogo, instancia o reader correto e delega a leitura. O método `write(df: DataFrame, logical_id: str) -> None` escreve no caminho e formato do catálogo de output.

### 6.6 Transformações Puras (`transforms/top10_transforms.py`)
Funções independentes, sem efeitos colaterais, que recebem e retornam DataFrames:

| Função | Entrada | Saída |
|---|---|---|
| `calcular_valor_total(df: DataFrame) -> DataFrame` | pedidos | pedidos + coluna `valor_total = VALOR_UNITARIO * QUANTIDADE` |
| `agregar_por_cliente(df: DataFrame) -> DataFrame` | pedidos com `valor_total` | `id_cliente`, `total_compras` (soma) |
| `enriquecer_com_nome(df_agg: DataFrame, df_clientes: DataFrame) -> DataFrame` | aggregado + clientes | adiciona coluna `nome` via **left join** em `id == id_cliente` |
| `aplicar_ranking(df: DataFrame, top_n: int) -> DataFrame` | enriquecido | adiciona coluna `rank` (1-based), ordena por `total_compras` desc, desempate por `id_cliente` asc, limita a `top_n` |

**Regras de negócio:**
* Pedidos com `VALOR_UNITARIO` ou `QUANTIDADE` nulos devem ser **descartados** antes da agregação (logar contagem descartada).
* O join entre pedidos e clientes é **left join**: clientes sem match aparecem com `nome = null` mas são mantidos no ranking.
* Duplicatas em `ID_PEDIDO` devem ser removidas com `dropDuplicates(["ID_PEDIDO"])` antes de qualquer agregação.

### 6.7 Schema do Output

O arquivo final em `/data/output/top_10_clientes` deve conter exatamente estas colunas:

| Coluna | Tipo | Descrição |
|---|---|---|
| `rank` | Integer | Posição no ranking (1 = maior comprador) |
| `id_cliente` | Long | ID do cliente |
| `nome` | String | Nome do cliente (pode ser null se sem match) |
| `total_compras` | Double | Soma de `VALOR_UNITARIO * QUANTIDADE` |

### 6.8 Leitura de Pedidos
* Ler **todos os arquivos CSV** presentes na pasta `pedidos/`, não apenas um mês específico. O Spark lê o diretório inteiro automaticamente quando o path aponta para a pasta.

---

## 7. Qualidade e Automação Local

### Testes (`tests/test_vendas_transforms.py`)
Use `spark.createDataFrame` com dados sintéticos. Cobrir obrigatoriamente:

1. `calcular_valor_total`: verificar que `valor_total = VALOR_UNITARIO * QUANTIDADE`.
2. `agregar_por_cliente`: verificar soma correta por cliente.
3. `aplicar_ranking`: verificar que o resultado tem no máximo `top_n` linhas, está ordenado corretamente e a coluna `rank` é sequencial a partir de 1.
4. Caso de borda — **desempate**: dois clientes com mesmo `total_compras`; verificar que o de menor `id_cliente` recebe rank menor.
5. Caso de borda — **nulos**: pedidos com `VALOR_UNITARIO = null` devem ser descartados antes da agregação.

### Makefile
```makefile
make lint     # executa: black src/ tests/ && flake8 src/ tests/
make test     # executa: pytest tests/ -v --tb=short
make package  # executa: python -m build --wheel --outdir dist/
```

---

## 8. Arquivos de Entrada

Clonar os datasets antes de executar o pipeline:

```sh
git clone https://github.com/infobarbosa/dataset-json-clientes ./data/input/dataset-json-clientes
git clone https://github.com/infobarbosa/datasets-csv-pedidos ./data/input/datasets-csv-pedidos
```

### Schema de `clientes.json`
```
./data/input/dataset-json-clientes/data/clientes.json
```
Formato: JSON Lines (um objeto por linha).
```json
{"id": 1, "nome": "Isabel Abreu", "data_nasc": "1982-10-26", "cpf": "512.084.739-05", "email": "isabel.abreusigycp@outlook.com", "interesses": ["Filmes"], "carteira_investimentos": {"FIIs": 11533.69, "CDB": 26677.01}}
{"id": 2, "nome": "Natália Ramos", "data_nasc": "1971-04-26", "cpf": "780.369.125-03", "email": "natalia.ramosrzmyqb@hotmail.com", "interesses": ["Viagens"], "carteira_investimentos": {}}
```
Colunas relevantes para o pipeline: `id`, `nome`.

### Schema de `pedidos-*.csv`
```
./data/input/datasets-csv-pedidos/data/pedidos/pedidos-2026-01.csv
```
Separador: `;` | Header: presente.
```
ID_PEDIDO;PRODUTO;VALOR_UNITARIO;QUANTIDADE;DATA_CRIACAO;UF;ID_CLIENTE
f198e8f7-033d-414d-b032-20975e84edde;LIQUIDIFICADOR;300.0;1;2026-01-05T18:36:28;MG;8409
97969db5-9304-4b80-b19e-3a9d60ce6520;CELULAR;1000.0;3;2026-01-01T11:58:48;DF;934
```
Colunas relevantes para o pipeline: `ID_PEDIDO`, `VALOR_UNITARIO`, `QUANTIDADE`, `ID_CLIENTE`.

---

## 9. Definição de Pronto (DoP)

A entrega é considerada completa quando **todos** os critérios abaixo forem atendidos:

- [ ] Estrutura de pastas idêntica à definida na seção 4.
- [ ] `config/config.yaml` preenchido conforme seção 5, sem nenhum valor hardcoded no código.
- [ ] `make lint` executa sem erros.
- [ ] `make test` executa com todos os 5 casos de teste passando.
- [ ] `python src/main.py` executa sem erros e grava o output em `data/output/top_10_clientes/`.
- [ ] O arquivo de output contém exatamente as 4 colunas definidas na seção 6.7, com no máximo 10 linhas, ordenadas por `rank` crescente.
- [ ] O log da execução exibe a contagem de registros lidos de cada fonte e o tempo de execução de cada transformação.
