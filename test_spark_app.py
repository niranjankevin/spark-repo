from pyspark.sql import SparkSession
from pyspark import SparkContext
from pyspark.sql.functions import col

def test_xml_to_bigquery():
    """
    Tests the spark_app.py functionality by reading from the bigquery table.
    """
    # Create a Spark session
    spark = SparkSession.builder.appName("XMLToBigQueryTest").getOrCreate()
    spark_context = SparkContext()

    # Read data from bigquery table
    bq_df = spark.read.format("bigquery").load("gcptraining-350417.medicare.claim_table")

    # Expected data
    expected_data = [{"provider_id":"123"}, {"provider_id":"456"}]
    
    # Check if data matches with expected data
    assert bq_df.select("provider_id").rdd.collect() == expected_data

if __name__ == "__main__":
    test_xml_to_bigquery()
