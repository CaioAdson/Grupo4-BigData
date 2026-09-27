import os

from pyspark.sql import SparkSession, functions as F


spark = (
    SparkSession.builder
    .appName("ecommerce-daily-etl")
    .enableHiveSupport()
    .getOrCreate()
)

spark.sql("CREATE DATABASE IF NOT EXISTS ecommerce")

# CORREÇÃO: o Flume grava em subpastas particionadas
# (.../raw/year=%Y/month=%m/day=%d/hour=%H/events-*), não em um arquivo
# único chamado "events.jsonl". Apontar para a pasta-base faz o Spark
# descobrir os arquivos recursivamente (e, de brinde, expõe year/month/
# day/hour como colunas de partição via Hive-style partition discovery).
input_path = os.getenv(
    "INPUT_PATH",
    "hdfs://namenode:8020/ecommerce/raw"
)

events = (
    spark.read.json(input_path)
    .withColumn("event_date", F.to_date("event_time"))
)

# Identifica a data mais recente disponível nos eventos.
current_date = events.select(F.max("event_date")).first()[0]

print(f"=== DATA ATUAL PROCESSADA: {current_date} ===")

# Compras somente do dia atual.
purchases = (
    events
    .filter(
        (F.col("event_type") == "purchase")
        & (F.col("event_date") == F.lit(current_date))
    )
    .withColumn(
        "revenue",
        F.col("price") * F.col("quantity")
    )
)

# RDD para demonstrar o processamento com RDD.
purchase_rdd = purchases.rdd.map(
    lambda row: (
        row["category"],
        row["region"],
        float(row["revenue"] or 0),
        int(row["quantity"] or 0),
        row["event_id"],
        row["event_date"],
    )
)

purchase_df = spark.createDataFrame(
    purchase_rdd,
    [
        "category",
        "region",
        "revenue",
        "quantity",
        "event_id",
        "event_date",
    ],
)

# groupBy gera shuffle, demonstrando uma wide dependency.
daily_sales = (
    purchase_df
    .groupBy("category", "region", "event_date")
    .agg(
        F.round(F.sum("revenue"), 2).alias("total_revenue"),
        F.sum("quantity").alias("items_sold"),
        F.countDistinct("event_id").alias("purchases"),
    )
)

print("=== VENDAS DO DIA ===")
daily_sales.show(truncate=False)

# Atualiza a tabela diária.
daily_sales.write.mode("overwrite").saveAsTable(
    "ecommerce.daily_sales"
)

# ==========================================================
# COMPARAÇÃO COM O DIA ANTERIOR
# ==========================================================

previous_date = (
    current_date
    - __import__("datetime").timedelta(days=1)
)

print(f"=== DIA ANTERIOR ESPERADO: {previous_date} ===")

try:
    historical = spark.table("ecommerce.daily_sales_history")

    previous_sales = historical.filter(
        F.col("event_date") == F.lit(previous_date)
    )

    if previous_sales.limit(1).count() > 0:

        comparison = (
            daily_sales.alias("current")
            .join(
                previous_sales.alias("history"),
                on=["category", "region"],
                how="left",
            )
            .select(
                "category",
                "region",
                "current.event_date",
                F.col("current.total_revenue").alias(
                    "current_revenue"
                ),
                F.col("history.total_revenue").alias(
                    "previous_revenue"
                ),
            )
            .withColumn(
                "revenue_difference",
                F.round(
                    F.col("current_revenue")
                    - F.coalesce(
                        F.col("previous_revenue"),
                        F.lit(0),
                    ),
                    2,
                ),
            )
        )

        print(
            f"=== COMPARAÇÃO: {current_date} "
            f"X {previous_date} ==="
        )

        comparison.show(truncate=False)

    else:
        print(
            f"Nenhum histórico encontrado para "
            f"{previous_date}."
        )

except Exception:
    print("Tabela de histórico ainda não existe.")

# ==========================================================
# HISTÓRICO DOS DIAS ANTERIORES
# ==========================================================

# Reprocessa todas as compras do arquivo bruto para manter
# o histórico de todos os dias disponíveis.
all_purchases = (
    events
    .filter(F.col("event_type") == "purchase")
    .withColumn(
        "revenue",
        F.col("price") * F.col("quantity")
    )
)

all_purchase_rdd = all_purchases.rdd.map(
    lambda row: (
        row["category"],
        row["region"],
        float(row["revenue"] or 0),
        int(row["quantity"] or 0),
        row["event_id"],
        row["event_date"],
    )
)

all_purchase_df = spark.createDataFrame(
    all_purchase_rdd,
    [
        "category",
        "region",
        "revenue",
        "quantity",
        "event_id",
        "event_date",
    ],
)

updated_history = (
    all_purchase_df
    .groupBy("category", "region", "event_date")
    .agg(
        F.round(F.sum("revenue"), 2).alias("total_revenue"),
        F.sum("quantity").alias("items_sold"),
        F.countDistinct("event_id").alias("purchases"),
    )
)

updated_history.write.mode("overwrite").saveAsTable(
    "ecommerce.daily_sales_history"
)

print("=== HISTÓRICO ATUALIZADO ===")

spark.stop()