# Couche de traitement de données pour la consommation de données à partir de Kafka avec Spark
import os
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, window, when, to_timestamp, date_format
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

# Configuration
KAFKA_BROKER = "localhost:9092"
TOPIC_NAME = "urban.traffic"
PARQUET_OUTPUT_PATH = "data/gold/traffic_aggregated"
CHECKPOINT_PATH = "data/checkpoints/traffic_aggregated"

spark_version = pyspark.__version__
os.environ['PYSPARK_SUBMIT_ARGS'] = f'--packages org.apache.spark:spark-sql-kafka-0-10_2.13:{spark_version} pyspark-shell'

def start_spark_streaming():
    # Initialisation de la session Spark
    print("[INFO] Initialisation de la session Apache Spark...")
    spark = SparkSession.builder \
        .appName("SmartCityTrafficAnalytics") \
        .master("local[*]") \
        .config("spark.sql.shuffle.partitions", "2") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")
    print("[OK] Session Spark active.")

    # Définition du schéma JSON correspondant au Producer YOLOv8
    traffic_schema = StructType([
        StructField("timestamp", StringType(), True),
        StructField("camera_id", StringType(), True),
        StructField("vehicle_count", IntegerType(), True)
    ])

    # Connexion au flux temps réel de Kafka (Couche Bronze)
    print(f"[INFO] Connexion au flux Kafka sur le topic '{TOPIC_NAME}'...")
    raw_stream = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BROKER) \
        .option("subscribe", TOPIC_NAME) \
        .option("startingOffsets", "latest") \
        .load()

    # Parsing et typage de la donnée (Passage en Couche Silver)
    # On extrait la valeur du message Kafka (qui est en binaire) et on la caste en string/JSON
    parsed_stream = raw_stream \
        .selectExpr("CAST(value AS STRING) as json_payload") \
        .select(from_json(col("json_payload"), traffic_schema).alias("data")) \
        .select("data.*") \
        .withColumn("event_time", to_timestamp(col("timestamp")))

    # Calculs et Agrégations (Passage en Couche Gold)
    # On calcule le nombre moyen de véhicules par fenêtre de 1 minute
    aggregated_stream = parsed_stream \
        .withWatermark("event_time", "2 minutes") \
        .groupBy(
            window(col("event_time"), "1 minute"),
            col("camera_id")
        ) \
        .avg("vehicle_count") \
        .withColumnRenamed("avg(vehicle_count)", "avg_vehicles") \
        .withColumn(
            "congestion_status",
            when(col("avg_vehicles") > 15, "CRITIQUE")
            .when(col("avg_vehicles") > 8, "EMBOUTEILLAGE")
            .otherwise("FLUIDE")
        ) \
        .select(
            col("window.start").alias("window_start"),
            col("window.end").alias("window_end"),
            "camera_id",
            "avg_vehicles",
            "congestion_status"
        )

    # Écriture Gold : Parquet partitionné + affichage console via foreachBatch
    def write_gold_batch(batch_df, batch_id):
        if batch_df.isEmpty():
            return

        # Affichage console
        print(f"\n--- Micro-batch #{batch_id} ---")
        batch_df.show(truncate=False)

        # Écriture Parquet partitionnée par date et caméra (Couche Gold)
        batch_df \
            .withColumn("date", date_format(col("window_start"), "yyyy-MM-dd")) \
            .write \
            .mode("append") \
            .partitionBy("date", "camera_id") \
            .parquet(PARQUET_OUTPUT_PATH)

    print("[OK] Pipeline de streaming prêt. En attente des données...")
    query = aggregated_stream.writeStream \
        .outputMode("update") \
        .foreachBatch(write_gold_batch) \
        .option("checkpointLocation", CHECKPOINT_PATH) \
        .start()

    query.awaitTermination()

if __name__ == "__main__":
    start_spark_streaming()