from pyspark.sql import SparkSession
from pyspark.sql.functions import split, col
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import RandomForestRegressor
from pyspark.ml.evaluation import RegressionEvaluator

import threading
import time


class StreamingData:
    def __init__(self, max_batches=10):
        self.batch_counter = 0
        self.max_batches = max_batches
        self.collected_data = None
        self.query = None

    def update_model(self, batch_df, batch_id):
        if batch_df.count() == 0:
            return

        if self.collected_data is None:
            self.collected_data = batch_df
        else:
            self.collected_data = self.collected_data.union(batch_df)

        assembler = VectorAssembler(inputCols=["feature1", "feature2"], outputCol="features")
        assembled_data = assembler.transform(self.collected_data).select("features", "y")

        rf = RandomForestRegressor(featuresCol="features", labelCol="y")
        model = rf.fit(assembled_data)

        predictions = model.transform(assembled_data)
        evaluator = RegressionEvaluator(labelCol="y", predictionCol="prediction", metricName="rmse")
        rmse = evaluator.evaluate(predictions)

        self.batch_counter += 1
        print(f"[Batch {batch_id}] Số cây: {len(model.trees)} | RMSE: {rmse} | Batches: {self.batch_counter}")
        print("---------------------------------------------------")

    def monitor_batches(self):
        while self.batch_counter < self.max_batches:
            time.sleep(1)
        print(f"Huấn luyện đủ {self.max_batches} batch. Dừng lại.")
        self.query.stop()

    def start_streaming(self):
        spark = SparkSession.builder \
                .appName("SparkML Random Forest Streaming") \
                .config("spark.hadoop.fs.file.impl", "org.apache.hadoop.fs.LocalFileSystem") \
                .config("spark.hadoop.fs.AbstractFileSystem.hdfs.impl", "org.apache.hadoop.fs.local.LocalFileSystem") \
                .config("spark.sql.shuffle.partitions", "4") \
                .config("spark.driver.memory", "2g") \
                .config("spark.executor.memory", "2g") \
                .config("spark.sql.warehouse.dir", "file:///C:/temp/spark-warehouse") \
                .getOrCreate()

        spark.sparkContext.setLogLevel("ERROR")

        raw_stream = spark.readStream.format("socket") \
            .option("host", "localhost") \
            .option("port", 9999) \
            .load()

        data_stream = raw_stream.select(
            split(col("value"), ",").getItem(0).cast("float").alias("feature1"),
            split(col("value"), ",").getItem(1).cast("float").alias("feature2"),
            split(col("value"), ",").getItem(2).cast("float").alias("y")
        )

        self.query = data_stream.writeStream \
            .foreachBatch(self.update_model) \
            .outputMode("update") \
            .start()

        threading.Thread(target=self.monitor_batches).start()

        self.query.awaitTermination()