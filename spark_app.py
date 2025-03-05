from pyspark.sql import SparkSession
from pyspark import SparkContext
from google.cloud import storage
from google.cloud import bigquery

def main():
    # Create a Spark session
    spark = SparkSession.builder.appName("XMLToBigQuery").getOrCreate()
    spark_context = SparkContext()

    # Configure GCS connector
    conf = {"spark.hadoop.fs.gcs.impl.disable.cache.validation": "true"}
    spark.sparkContext.setConf(conf)

    # Read XML from GCS
    gcs_path = "gs://gcptraining-350417/spark-repo/claim.xml" 
    xml_df = spark.read.format("xml").option("rowTag", "ROW").load(gcs_path)

    # Write to BigQuery
    xml_df.write.format("bigquery").mode("overwrite").option("temporaryGcsBucket", "gcptraining-350417").save("gcptraining-350417.medicare.claim_table")

if __name__ == "__main__":
    main()
