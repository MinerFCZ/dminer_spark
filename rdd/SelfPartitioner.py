from pyspark import SparkConf, SparkContext, RDD, TaskContext
from pyspark.rdd import Partitioner


def print_partition(rdd):
    print("------------------print_partition--------------")
    print(TaskContext.get().partitionId())
    for r in rdd:
        print(r)
    print("------------------print_partition--------------")


def partitioner(x):
    element = x.split('-')
    if int(element[1]) % 2 == 0:
        return 0
    else:
        return 1


def main():
    conf = SparkConf().setAppName('partitioner test').setMaster('local')
    sc = SparkContext(conf=conf)

    cities = ["beijing-1", "beijing-2", "beijing-3", "shanghai-1", "shanghai-2", "tianjing-1", "tianjing-2"]
    cities_rdd = sc.parallelize(cities)
    rdd = cities_rdd.map(lambda x: (x, 1))

    rdd.partitionBy(3, partitioner).foreachPartition(print_partition)


if __name__ == '__main__':
    main()
