import os

from pyspark.sql import SparkSession, functions as F


spark = (
    SparkSession.builder
    .appName("ecommerce-daily-etl")
    .enableHiveSupport()
    .getOrCreate()
)

spark.sql("CREATE DATABASE IF NOT EXISTS ecommerce")

input_path = os.getenv(
    "INPUT_PATH",
    "hdfs://namenode:8020/ecommerce/raw/events.jsonl"
)

events = spark.read.json(input_path)

# Mantém somente compras e calcula a receita.
purchases = (
    events
    .filter(F.col("event_type") == "purchase")
    .withColumn(
        "revenue",
        F.col("price") * F.col("quantity")
    )
)

# RDD: transformação dos dados para demonstrar o uso de RDD.
purchase_rdd = purchases.rdd.map(
    lambda row: (
        row["category"],
        row["region"],
        float(row["revenue"] or 0),
        int(row["quantity"] or 0),
        row["event_id"],
    )
)

# Volta para DataFrame para continuar o ETL com Spark SQL/DataFrame.
purchase_df = spark.createDataFrame(
    purchase_rdd,
    [
        "category",
        "region",
        "revenue",
        "quantity",
        "event_id",
    ],
)

# groupBy provoca shuffle e demonstra uma wide dependency.
daily_sales = (
    purchase_df
    .groupBy("category", "region")
    .agg(
        F.round(F.sum("revenue"), 2).alias("total_revenue"),
        F.sum("quantity").alias("items_sold"),
        F.countDistinct("event_id").alias("purchases"),
    )
)

print("=== VENDAS DO DIA ===")
daily_sales.show(truncate=False)

# Salva o resultado consolidado no Hive.
daily_sales.write.mode("overwrite").saveAsTable(
    "ecommerce.daily_sales"
)

# Histórico: compara as vendas atuais com o histórico disponível.
try:
    historical = spark.table("ecommerce.daily_sales_history")

    comparison = (
        daily_sales.alias("current")
        .join(
            historical.alias("history"),
            on=["category", "region"],
            how="left",
        )
        .select(
            "category",
            "region",
            F.col("current.total_revenue").alias("current_revenue"),
            F.col("history.total_revenue").alias("previous_revenue"),
        )
        .withColumn(
            "revenue_difference",
            F.round(
                F.col("current_revenue")
                - F.coalesce(F.col("previous_revenue"), F.lit(0)),
                2,
            ),
        )
    )

    print("=== COMPARAÇÃO COM HISTÓRICO ===")
    comparison.show(truncate=False)

except Exception:
    print("Nenhum histórico anterior encontrado.")

# Atualiza o histórico com os dados processados.
daily_sales.write.mode("overwrite").saveAsTable(
    "ecommerce.daily_sales_history"
)

spark.stop()