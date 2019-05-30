
from pyspark.sql import Row
from pyspark.sql import SQLContext
from pyspark.sql import SparkSession


class SparkContextSC:
    def __init__(self):
        self.spark = SparkSession.builder.appName("CheckPoint1").master("local[*]").config("spark.driver.memory", "6g")\
            .getOrCreate()
        self.sc = self.spark.sparkContext
        self.sc = self.sc
        self.sql_context = SQLContext(self.sc)


class QueryEngine(object):
    def __init__(self, sql_context, df):
        self.sql_context = sql_context
        self.df = df

    def query(self):
        self.df.createOrReplaceTempView("result_df")

        query_1 = """
                    SELECT
                      location,
                      COUNT (DISTINCT product_id) as product_count
                    from
                      result_df
                    GROUP BY
                      location
                    HAVING
                      COUNT (DISTINCT product_id) = (
                        SELECT
                          MAX (count_table.product_count)
                        FROM
                          (
                            SELECT
                              COUNT (DISTINCT product_id) as product_count
                            from
                              result_df
                            GROUP BY
                              location
                          ) count_table
                      )
                    """
        query_1_df = self.sql_context.sql(query_1)
        print("Find locations in which sale of each product is max ::")
        query_1_df.show()

        query_2 = """
                    SELECT
                      customer_id,
                      COUNT (product_id) as product_count
                    from
                      result_df
                    GROUP BY
                      customer_id
                    HAVING
                      COUNT (product_id) = (
                        SELECT
                          MAX (count_table.product_count)
                        FROM
                          (
                            SELECT
                              COUNT (product_id) as product_count
                            from
                              result_df
                            GROUP BY
                              customer_id
                          ) count_table
                      )
                    """
        query_2_df = self.sql_context.sql(query_2)
        print("Find customer who has purchased max number of items :: ")
        query_2_df.show()

        query_3 = """
                    SELECT
                      customer_id,
                      SUM (sell_price) as sum_amount
                    from
                      result_df
                    GROUP BY
                      customer_id
                    HAVING
                      SUM (sell_price) = (
                        SELECT
                          MAX (sum_table.sum_amount)
                        FROM
                          (
                            SELECT
                              SUM (sell_price) as sum_amount
                            from
                              result_df
                            GROUP BY
                              customer_id
                          ) sum_table
                      )
                    """
        query_3_df = self.sql_context.sql(query_3)
        print("Find customer who has spent max money :: ")
        query_3_df.show()

        query_4 = """
                    SELECT
                      item_description,
                      SUM (sell_price) as sum_amount
                    from
                      result_df
                    GROUP BY
                      item_description
                    HAVING
                      SUM (sell_price) = (
                        SELECT
                          MIN (sum_table.sum_amount)
                        FROM
                          (
                            SELECT
                              SUM (sell_price) as sum_amount
                            from
                              result_df
                            GROUP BY
                              item_description
                          ) sum_table
                      )
                    """
        query_4_df = self.sql_context.sql(query_4)
        print("Find product which has min sale in terms of money :: ")
        query_4_df.show()

        query_5 = """
                    SELECT
                      item_description,
                      COUNT (item_description) as item_count
                    from
                      result_df
                    GROUP BY
                      item_description
                    HAVING
                      COUNT (item_description) = (
                        SELECT
                          MIN (count_table.item_count)
                        FROM
                          (
                            SELECT
                              COUNT (item_description) as item_count
                            from
                              result_df
                            GROUP BY
                              item_description
                          ) count_table
                      )
                    """
        query_5_df = self.sql_context.sql(query_5)
        print("Find product which has min sale in terms of number of unit sold :: ")
        query_5_df.show()


if __name__ == "__main__":
    sql_context_ = SparkContextSC().sql_context
    spark_context_ = SparkContextSC().sc

    custom_information_data = [[1001, 1002, 1003], ["abc@gmail.com", "def@gmail.com", "ghi@gmail.com"],
                               ["english", "english", "hindi"], ["mum", "mum", "del"]]
    custom_information_column = Row("customer_id", "email_id", "language", "location")

    purchase_information_data = [[101, 102, 103, 104, 105, 106], [201, 205, 202, 201, 203, 206],
                                 [1001, 1001, 1002, 1002, 1002, 1003], [200, 400, 50, 150, 100, 250],
                                 ["book", "pen", "pencil", "book", "colours", "a4sheets"]]
    purchase_information_column = Row("transaction_id", "product_id", "customer_id", "sell_price", "item_description")

    custom_information_df = spark_context_.parallelize([custom_information_column(*i) for i in
                                                       zip(*custom_information_data)]).toDF()

    purchase_information_df = spark_context_.parallelize([purchase_information_column(*i) for i in
                                                         zip(*purchase_information_data)]).toDF()

    merge_df = custom_information_df.alias("cus").join(purchase_information_df.alias("pur"), custom_information_df.customer_id == purchase_information_df.customer_id, how="left_outer").select("cus.customer_id", "cus.email_id", "cus.language", "cus.location", "pur.transaction_id", "pur.product_id", "pur.sell_price", "pur.item_description")
    merge_df.show()

    obj = QueryEngine(sql_context_, merge_df)
    obj.query()
