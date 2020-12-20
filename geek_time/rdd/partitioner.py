from pyspark import SparkConf, SparkContext, TaskContext
from pyspark import RDD


def print_partition(rdd):
    print("------------------print_partition--------------")
    print(TaskContext.get().partitionId())
    for r in rdd:
        print(r)
    print("------------------print_partition--------------")



def main():
    conf = SparkConf().setAppName('partitioner test').setMaster('local')
    sc = SparkContext(conf=conf)

    data = [1, 2, 3, 4, 5]
    dist_data = sc.parallelize(data)
    rdd = dist_data.map(lambda x: (x, f'values_{x}'))
    rdd.foreach(lambda x: print(x))

    print(f'number of partition:{rdd.getNumPartitions()}')

    rdd.foreachPartition(print_partition)

    print("default Partitioner")
    rdd.partitionBy(3).foreachPartition(print_partition)


if __name__ == '__main__':
    main()
