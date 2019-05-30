
import os
import re
from pyspark.sql.types import *
from pyspark.sql import SQLContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf


class SparkContextSC:
    def __init__(self):
        self.spark = SparkSession.builder.appName("CheckPoint1").master("local[*]").config("spark.driver.memory", "6g")\
            .getOrCreate()
        self.sc = self.spark.sparkContext
        self.sql_context = SQLContext(self.sc)


class TopIP(object):
    def __init__(self, sql_context, input_file):
        self.sql_context = sql_context
        self.file_name = input_file
        self.input_file = os.path.join(os.path.abspath(os.path.dirname(self.file_name)), self.file_name)
        self.file_schema = StructType([StructField("log_data", StringType(), True)])

    def read_text_file(self):
        df = self.sql_context.read.format("com.databricks.spark.csv")\
            .options(header="false")\
            .load(self.input_file, schema=self.file_schema)

        def search_ip(row_):
            ip = re.search(r"\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}", row_).group(0)
            return ip

        get_ip_udf = udf(search_ip, StringType())
        df = df.withColumn("ip", get_ip_udf("log_data"))

        df = df.drop("log_data")
        df = df.cube("ip").count()
        df = df.filter(df.ip.isNotNull())

        count_data = df.select("count").collect()
        count_array = [int(row["count"]) for row in count_data]
        cutoff_value = sorted(set(count_array), reverse=True)[4]

        df = df.filter(df["count"] >= cutoff_value)
        print("List top five IP address in terms of number of hits :: ")
        df.show()


if __name__ == "__main__":
    sql_context_ = SparkContextSC().sql_context
    file_path = "CheckPoint1Input.txt"

    obj = TopIP(sql_context_, file_path)
    obj.read_text_file()
