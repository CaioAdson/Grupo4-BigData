# Grupo 4 — Arquitetura de Big Data em Tempo Real para E-commerce

## Integrantes

* **Nome:** Caio Adson Oliveira Pimentel
* **Nome:** Maria Ingrid Silva Silveira

---

## 1. Sobre o projeto

Este projeto implementa uma arquitetura de **Big Data em tempo real** para monitoramento de um cenário de e-commerce.

A solução combina processamento **streaming** e **batch**, permitindo:

* geração contínua de eventos de e-commerce;
* ingestão dos eventos utilizando Apache Flume;
* armazenamento dos dados brutos no HDFS;
* processamento de eventos em tempo real com Apache Flink;
* identificação de produtos em alta;
* armazenamento dos alertas em Apache HBase;
* processamento batch com Apache Spark;
* consolidação das vendas utilizando Hive;
* manutenção de um histórico de vendas para comparação entre dias.

### Fluxo principal

```text
                 ┌──────────────────┐
                 │  Gerador Python  │
                 │  eventos JSON    │
                 └────────┬─────────┘
                          │
                          ▼
                 ┌──────────────────┐
                 │      Flume       │
                 │     TAILDIR      │
                 └────────┬─────────┘
                          │
                          ▼
                 ┌──────────────────┐
                 │       HDFS       │
                 │   dados brutos   │
                 └────────┬─────────┘
                          │
                          ▼
                 ┌──────────────────┐
                 │      Spark       │
                 │   ETL / Batch    │
                 └────────┬─────────┘
                          │
                          ▼
                 ┌──────────────────┐
                 │       Hive       │
                 │  Data Warehouse  │
                 └──────────────────┘


Eventos → Flink → janela deslizante → alertas → HBase
             │
             └── processamento em tempo real
```

---

# 2. Objetivo

O objetivo é demonstrar uma arquitetura distribuída capaz de processar dados de um e-commerce em dois cenários:

### Streaming

Processar eventos continuamente e identificar produtos que estão apresentando aumento de cliques em uma janela de tempo.

### Batch

Processar os dados históricos armazenados no HDFS, realizando uma etapa de ETL com Spark e disponibilizando os resultados em tabelas Hive.

Dessa forma, o projeto demonstra a integração entre processamento em tempo real e processamento histórico.

---

# 3. Tecnologias utilizadas

| Tecnologia     | Função                                  |
| -------------- | --------------------------------------- |
| Python         | Geração contínua dos eventos            |
| Apache Flume   | Ingestão dos eventos                    |
| HDFS           | Armazenamento dos dados brutos          |
| Apache Flink   | Processamento de streaming              |
| HBase          | Armazenamento dos alertas em tempo real |
| Apache Spark   | Processamento batch e ETL               |
| Hive           | Data Warehouse                          |
| Docker Compose | Orquestração dos serviços               |
| PyFlink        | Desenvolvimento do job de streaming     |
| PySpark        | Desenvolvimento do ETL                  |

---

# 4. Estrutura do projeto

```text
Grupo4-BigData/
│
├── data/
│   └── events.jsonl
│
├── generator/
│   └── gerador.py
│
├── flume/
│   ├── flume.conf
│   └── flume-test.conf
│
├── flink/
│   ├── job.py
│   └── rotator.py
│
├── spark/
│   └── etl.py
│
├── hive/
│   └── schema.sql
│
├── scripts/
│   ├── check-final.sh
│   ├── check-week1.sh
│   └── check-week1.ps1
│
├── docker-compose.yml
│
└── README.md
```

---

# 5. Geração dos eventos

O arquivo:

```text
generator/gerador.py
```

é responsável por gerar continuamente eventos simulando atividades de um e-commerce.

Os eventos são armazenados em:

```text
data/events.jsonl
```

Entre os eventos gerados estão:

* cliques em produtos;
* inclusão de produtos no carrinho;
* compras;
* atualizações relacionadas à entrega.

Cada evento contém informações utilizadas posteriormente pelos componentes de streaming e batch.

---

# 6. Ingestão com Apache Flume

O Apache Flume utiliza o source `TAILDIR` para acompanhar continuamente o arquivo:

```text
/data/events.jsonl
```

A configuração ativa está em:

```text
flume/flume.conf
```

O pipeline utilizado pelo Flume é:

```text
TAILDIR → File Channel → HDFS Sink
```

Os dados são armazenados no HDFS utilizando particionamento por data e hora:

```text
/ecommerce/raw/
└── year=AAAA/
    └── month=MM/
        └── day=DD/
            └── hour=HH/
                └── events-*
```

Esse particionamento facilita a organização e posterior leitura dos dados pelo Spark.

---

# 7. HDFS

O HDFS funciona como camada de armazenamento dos dados brutos.

O diretório principal utilizado pelo projeto é:

```text
/ecommerce/raw
```

Durante a validação do projeto foram identificados arquivos reais gravados pelo Flume, por exemplo:

```text
/ecommerce/raw/year=2026/month=09/day=27/hour=19/events-*
```

Isso comprova a integração:

```text
Python → Flume → HDFS
```

---

# 8. Processamento em tempo real com Apache Flink

O job de streaming está localizado em:

```text
flink/job.py
```

O Flink monitora continuamente o diretório:

```text
/ready
```

Os eventos são processados utilizando **tempo de evento**.

## Watermark

Foi utilizado um watermark com tolerância de:

```text
10 segundos
```

Essa estratégia permite lidar com eventos que chegam com pequeno atraso.

## Janela deslizante

O processamento utiliza:

```text
Janela: 1 minuto
Slide: 20 segundos
```

Assim, o Flink reavalia continuamente os eventos recentes.

## Identificação de produtos em alta

Os eventos do tipo:

```text
click
```

são agrupados por:

```text
product_id
```

Quando um produto alcança pelo menos:

```text
5 cliques
```

na janela considerada, é gerado um alerta.

---

# 9. Integração Flink → HBase

Os alertas produzidos pelo Flink são enviados para o Apache HBase utilizando sua API REST.

A tabela utilizada é:

```text
product_alerts
```

A família de colunas utilizada é:

```text
info
```

Durante a validação, a tabela foi consultada pela API REST e retornou seu schema corretamente.

O fluxo é:

```text
Evento click
     │
     ▼
   Flink
     │
     ▼
Janela 1 min
slide 20 s
watermark 10 s
     │
     ▼
≥ 5 cliques
     │
     ▼
  HBase
product_alerts
```

Também foi observada a geração de alertas no log do TaskManager, por exemplo:

```text
HBASE ALERTA: produto=product-4, clicks=5, status=200
```

O status HTTP `200` confirma que o envio do alerta para o HBase foi realizado com sucesso.

---

# 10. Processamento batch com Apache Spark

O processamento batch está implementado em:

```text
spark/etl.py
```

O Spark lê os dados armazenados no HDFS:

```text
hdfs://namenode:8020/ecommerce/raw
```

O job realiza transformação e agregação dos eventos.

Entre os campos utilizados estão:

* categoria;
* região;
* data do evento;
* receita;
* quantidade;
* identificador do evento.

---

# 11. Wide Dependency

O ETL utiliza operações de agrupamento como:

```python
groupBy("category", "region", "event_date")
```

Essa operação representa uma **wide dependency**, pois os dados podem precisar ser redistribuídos entre diferentes partições do Spark durante o processo de shuffle.

Esse comportamento é importante em arquiteturas distribuídas porque pode gerar custo de comunicação entre os executores.

O projeto utiliza essa operação para consolidar os dados de vendas por:

```text
categoria + região + data
```

---

# 12. Hive

O Spark utiliza suporte ao Hive para armazenar os resultados consolidados.

O banco utilizado é:

```text
ecommerce
```

Foram criadas as seguintes tabelas:

```text
ecommerce.daily_sales
ecommerce.daily_sales_history
```

## daily_sales

Representa a consolidação das vendas por categoria, região e data.

Exemplo de dados obtidos durante a validação:

```text
casa        PE   2026-09-27   41243.53   61   38
moda        BA   2026-09-27   55144.90   77   41
livros      SP   2026-09-27   87652.21   124  57
moda        PE   2026-09-27   75150.66   95   50
moda        RJ   2026-09-27   57082.84   77   41
```

## daily_sales_history

Armazena o histórico consolidado para comparação com outros dias.

Durante a validação foram encontrados registros de diferentes datas, por exemplo:

```text
livros       SP   2026-09-07
esportes     CE   2026-09-14
esportes     BA   2026-09-08
moda         CE   2026-09-11
eletronicos  BA   2026-09-08
```

---

# 13. Integração completa

A arquitetura final pode ser resumida da seguinte forma:

```text
                    ┌─────────────────┐
                    │ Python Generator│
                    └────────┬────────┘
                             │
                             ▼
                    ┌─────────────────┐
                    │      Flume      │
                    │     TAILDIR     │
                    └────────┬────────┘
                             │
                             ▼
                    ┌─────────────────┐
                    │      HDFS       │
                    │   Raw Events    │
                    └────────┬────────┘
                             │
                  ┌──────────┴──────────┐
                  │                     │
                  ▼                     ▼
          ┌──────────────┐       ┌──────────────┐
          │    Spark     │       │    Flink     │
          │     Batch    │       │   Streaming  │
          └──────┬───────┘       └──────┬───────┘
                 │                      │
                 ▼                      ▼
          ┌──────────────┐       ┌──────────────┐
          │     Hive     │       │    HBase     │
          │  DW/Histórico│       │    Alertas   │
          └──────────────┘       └──────────────┘
```

---

# 14. Rotator do Flink

O arquivo:

```text
flink/rotator.py
```

é utilizado como ponte para o processamento contínuo do Flink.

O gerador mantém o arquivo de eventos sendo atualizado continuamente. Porém, o `FileSource` do Flink trabalha melhor detectando novos arquivos.

Por isso, o rotator transforma os eventos em arquivos completos e imutáveis disponibilizados no diretório:

```text
flink-ready/ready
```

O Flink monitora esse diretório continuamente.

Assim, temos:

```text
events.jsonl
     │
     ▼
 rotator
     │
     ▼
arquivos finalizados
     │
     ▼
   Flink
```

---

# 15. Docker Compose

Todos os componentes da arquitetura são executados por meio do Docker Compose.

Principais serviços:

```text
generator
flume
namenode
datanode
hbase
hbase-init
flink-jobmanager
flink-taskmanager
flink-rotator
spark-master
spark-worker
hive-metastore
```

Isso permite reproduzir o ambiente de forma padronizada.

---

# 16. Como executar

## Pré-requisitos

* Docker Desktop
* Docker Compose
* Máquina com memória suficiente para executar os serviços simultaneamente

## Subir o projeto

No diretório raiz:

```bash
docker compose build
docker compose up -d
```

Verificar os containers:

```bash
docker compose ps
```

---

# 17. Interfaces

As principais interfaces disponibilizadas pelo ambiente são:

| Serviço       | Endereço              |
| ------------- | --------------------- |
| Flink Web UI  | http://localhost:8081 |
| Spark Web UI  | http://localhost:8080 |
| HDFS NameNode | http://localhost:9870 |
| HBase REST    | http://localhost:8090 |

---

# 18. Comandos de validação

## Verificar Flink

```bash
docker compose exec flink-jobmanager flink list
```

O job esperado é:

```text
ecommerce-trending-products (RUNNING)
```

## Verificar HBase

```bash
curl http://localhost:8090/product_alerts/schema
```

A tabela esperada é:

```text
product_alerts
```

com a família:

```text
info
```

## Verificar HDFS

```bash
docker compose exec namenode hdfs dfs -ls -R /ecommerce/raw
```

Devem aparecer arquivos dentro das partições:

```text
year=...
month=...
day=...
hour=...
```

## Executar o ETL

```bash
docker compose exec spark-master \
/opt/spark/bin/spark-submit \
--master spark://spark-master:7077 \
/opt/spark/jobs/etl.py
```

## Verificar tabelas Hive

```bash
docker compose exec spark-master \
/opt/spark/bin/spark-sql \
--master spark://spark-master:7077 \
-e "SHOW TABLES IN ecommerce;"
```

Resultado esperado:

```text
daily_sales
daily_sales_history
```

---

# 19. Evidências da execução

Durante a validação final foram obtidas as seguintes evidências:

### Flink

```text
ecommerce-trending-products (RUNNING)
```

### HBase

A API REST retornou:

```text
NAME => 'product_alerts'
COLUMNS => [
  {
    NAME => 'info'
  }
]
```

### Spark

O ETL terminou com:

```text
SparkContext is stopping with exitCode 0
```

### Hive

Foram encontradas:

```text
daily_sales
daily_sales_history
```

e ambas retornaram dados reais durante consultas SQL.

### HDFS

Foram encontrados arquivos em:

```text
/ecommerce/raw/year=2026/month=09/day=27/hour=19/
```

Isso demonstra que os principais componentes da arquitetura foram executados e integrados.

---

# 20. Dificuldades encontradas e soluções

## 20.1 HBase e ZooKeeper

Durante a configuração inicial, houve problemas de comunicação entre os componentes do HBase e ZooKeeper.

A configuração foi ajustada para que o serviço de inicialização da tabela utilizasse corretamente o endereço do HBase no ambiente Docker.

Após a correção, a tabela:

```text
product_alerts
```

foi criada automaticamente.

---

## 20.2 Permissões do warehouse do Hive

O Spark inicialmente encontrou problemas ao criar o diretório utilizado pelo warehouse do Hive.

Foi necessário garantir a existência do diretório:

```text
/user/hive/warehouse
```

no HDFS e ajustar suas permissões.

Depois disso, o Spark conseguiu criar as tabelas Hive.

---

## 20.3 Arquivos temporários do Flume

Durante a execução do ETL, o Spark encontrou um arquivo temporário que estava sendo alterado pelo Flume:

```text
events-*.tmp
```

Isso causava erro de arquivo inexistente durante a leitura.

Para executar o processamento batch de forma estável, o Flume foi temporariamente parado enquanto o Spark realizava o processamento dos arquivos já finalizados.

Depois da conclusão do ETL, o Flume foi iniciado novamente.

Essa situação demonstra um desafio real de integração entre ingestão contínua e processamento batch sobre arquivos que ainda estão sendo escritos.

---

# 21. Trade-offs da arquitetura

A arquitetura utiliza tecnologias diferentes porque cada uma possui uma função específica.

### Flume + HDFS

Permite ingestão contínua e armazenamento dos dados brutos para processamento posterior.

### Flink + HBase

Permite identificar eventos relevantes em tempo real e armazenar os alertas para consulta rápida.

### Spark + Hive

Permite realizar processamento batch sobre o histórico, agregando e consolidando os dados para análises posteriores.

### Wide dependency

O `groupBy` permite realizar a agregação necessária, mas exige shuffle e redistribuição de dados entre partições.

### Arquivos intermediários do Flink

O uso do rotator adiciona uma etapa ao pipeline, mas resolve a limitação prática de acompanhar continuamente um único arquivo que está sendo alterado.

---

# 22. Critérios atendidos

| Critério                 | Status |
| ------------------------ | ------ |
| Gerador Python contínuo  | ✅      |
| Eventos em JSON          | ✅      |
| Flume com TAILDIR        | ✅      |
| Flume → HDFS             | ✅      |
| HDFS com particionamento | ✅      |
| Flink Streaming          | ✅      |
| Watermark                | ✅      |
| Janela deslizante        | ✅      |
| Alertas de produto       | ✅      |
| Flink → HBase            | ✅      |
| Tabela `product_alerts`  | ✅      |
| Spark ETL                | ✅      |
| Wide dependency          | ✅      |
| Spark → Hive             | ✅      |
| `daily_sales`            | ✅      |
| `daily_sales_history`    | ✅      |
| Histórico de vendas      | ✅      |
| Docker Compose           | ✅      |
| Evidências de execução   | ✅      |

---


# 23 Conclusão

O projeto implementa uma arquitetura integrada de Big Data para um cenário de e-commerce, combinando processamento em tempo real e processamento batch.

O fluxo de streaming permite detectar produtos em alta e armazenar os alertas no HBase.

O fluxo batch utiliza os dados armazenados no HDFS para realizar transformações com Spark e consolidar informações no Hive.

A solução demonstra a integração entre:

```text
Python
   ↓
Flume
   ↓
HDFS
   ├──────────────→ Spark → Hive
   │
   └──────────────→ Flink → HBase
```

Dessa forma, o projeto atende aos requisitos de ingestão, armazenamento, processamento streaming, processamento batch, geração de alertas e consolidação histórica.
