from pyspark.sql import SparkSession
from pyspark.sql.types import StructField
from pyspark.sql.types import StructType
from pyspark.sql.types import StringType


def read_mysql_method1():
    spark = SparkSession.builder.appName("Connect to MySQL").getOrCreate()
    url = "jdbc:mysql://localhost:3306/pyspark?useUnicode=true&characterEncoding=UTF-8&serverTimezone=UTC"
    prop = {"user": "root", "password": "root"}
    # 注意变量位置，properties=prop，
    user_df = spark.read.jdbc(url, "t_user", properties=prop)
    user_df.show()
    spark.stop()


def read_mysql_method2():
    spark = SparkSession.builder.appName("Connect to MySQL").getOrCreate()
    url = "jdbc:mysql://localhost:3306/pyspark?useUnicode=true&characterEncoding=UTF-8&serverTimezone=UTC"

    user_df2 = spark.read.format("jdbc") \
        .option("url", url) \
        .option("dbtable", "t_user") \
        .option("user", "root").option("password", "root").load()
    user_df2.show()
    spark.stop()


def write_mysql_method1():
    spark = SparkSession.builder.appName("write to mysql").getOrCreate()
    url = "jdbc:mysql://localhost:3306/pyspark?useUnicode=true&characterEncoding=UTF-8&serverTimezone=UTC"
    prop = {"user": "root", "password": "root"}

    df = spark.read.jdbc(url=url, table="t_user", properties=prop).where("id = 1")
    df.write.mode("append").jdbc(url=url, table="t_user1", properties=prop)


def write_mysql_method2():
    spark = SparkSession.builder.appName("write 2 mysql").getOrCreate()
    url = "jdbc:mysql://localhost:3306/pyspark?useUnicode=true&characterEncoding=UTF-8&serverTimezone=UTC"
    prop = {"user": "root", "password": "root"}

    user_df = spark.read.format("csv") \
        .option("sep", ";") \
        .option("inferSchema", True) \
        .option("header", True) \
        .load("/Users/risheng/dminer/spark/dminer_spark/resource/user.csv")

    # schema_str = "id email age"
    # fields = [StructField(field_name, StringType(), True) for field_name in schema_str.split(" ")]
    # schema = StructType(fields)

    user_df2 = user_df.select(['email', 'age'])
    user_df2.show()

    user_df2.write.mode("append").jdbc(url=url, table="t_user2", properties=prop)


def main():
    # 读取MySQL方式一
    # read_mysql_method1()

    # 读取MySQL方式二
    # read_mysql_method2()

    # 从MySQL查询数据，然后在写回MySQL
    # write_mysql_method1()

    write_mysql_method2()


if __name__ == '__main__':
    main()
