
import os
from datetime import datetime
from pyspark.sql.types import *
from pyspark.sql import SQLContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import split
from pyspark.sql.functions import udf


class SparkContextSC:
    def __init__(self):
        self.spark = SparkSession.builder.appName("CheckPoint1").master("local[*]").config("spark.driver.memory", "6g").getOrCreate()
        self.sc = self.spark.sparkContext
        self.sql_context = SQLContext(self.sc)


class DataFrameOperation(object):
    def __init__(self, sql_context, input_file):
        self.sql_context = sql_context
        self.file_name = input_file
        self.input_file = os.path.join(os.path.abspath(os.path.dirname(self.file_name)), self.file_name)

    def read_text_file(self):
        df = self.sql_context.read.format("com.databricks.spark.csv")\
            .options(header="true")\
            .option("delimiter", "|")\
            .load(self.input_file)

        data_split = split(df["location"], "-")
        df_a = df.withColumn("state", data_split.getItem(0))
        df_a = df_a.withColumn("city", data_split.getItem(1))
        print("You need to parse the State and City into two different columns :: ")
        df_a.show()

        df.createOrReplaceTempView("result_df")
        query_b_i = "SELECT id,end_date,start_date,location,DATEDIFF(end_date,start_date) date_diff FROM result_df"
        query_b_i_df = self.sql_context.sql(query_b_i)
        print("SparkSQL functions to get this date difference :: ")
        query_b_i_df.show()

        def date_diff(end_date, start_date):
            date_format = "%Y-%m-%d %H:%M:%S"
            formatted_start_date = datetime.strptime(start_date, date_format)
            formatted_end_date = datetime.strptime(end_date, date_format)
            delta = formatted_end_date - formatted_start_date
            return delta.days

        get_date_udf = udf(date_diff, IntegerType())
        df_b_ii = df.withColumn("date_diff", get_date_udf("end_date", "start_date"))
        print("UDF that gets the number of days between the end date and the start date :: ")
        df_b_ii.show()


if __name__ == "__main__":
    sql_context_ = SparkContextSC().sql_context
    file_path = "CheckPoint4Input.txt"

    obj = DataFrameOperation(sql_context_, file_path)
    obj.read_text_file()
