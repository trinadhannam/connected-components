import time
import pyspark
from pyspark.sql import SparkSession
import sys
import gc
import warnings
warnings.filterwarnings("ignore")

# Number of mappers and reducers
NUM_MAPPERS = 3
NUM_REDUCERS = 4


# Initialize Spark session and context
spark_session = SparkSession.builder.appName("PySpark Connected Components")\
                .config("spark.eventLog.gcMetrics.youngGenerationGarbageCollectors", "G1 Young Generation") \
                .config("spark.eventLog.gcMetrics.oldGenerationGarbageCollectors", "G1 Old Generation") \
                .getOrCreate()
spark_context = spark_session.sparkContext
spark_context.setLogLevel("OFF")  

# Spark accumulator for counting new pairs, shared across all nodes
new_pairs = spark_context.accumulator(0)

# loads the dataset into spark
def load_dataset(path):
    return spark_context.textFile(path, minPartitions=NUM_MAPPERS)


def preprocess_rdd(rdd):
    return rdd.filter(lambda x: '#' not in x).map(lambda x: x.strip().split()).map(lambda x: (int(x[0]), int(x[1])))


def iterative_map(rdd , NUM_MAPPERS):
    return rdd.union(rdd.map(lambda x: (x[1], x[0]))).repartition(NUM_MAPPERS)


def count_new_pair(x):
    global new_pairs
    k, neighbours = x
    np = 0
    min_value = min(k, *neighbours)
    if min_value < k:
        yield (k, min_value)
        for v in neighbours:
            if min_value != v:
                np += 1
                yield (v, min_value)
    new_pairs += np

def iterative_reduce(rdd , NUM_REDUCERS):
    rdd = rdd.repartition(NUM_REDUCERS)
    return (
        rdd.groupByKey()
        .flatMap(lambda x: count_new_pair(x))
        .sortByKey()
        
    )


def connected_components(rdd):
    nb_iteration = 0
    global new_pairs
    while True:
        nb_iteration += 1
        start_pair_count = new_pairs.value
        rdd = iterative_map(rdd , NUM_MAPPERS)
        rdd = iterative_reduce(rdd , NUM_REDUCERS)
        rdd = rdd.distinct().repartition(NUM_REDUCERS)
        gc.collect()
        print(f"Iteration {nb_iteration}: total edges = {new_pairs.value}")
        
        if start_pair_count == new_pairs.value:
            break

    return rdd


def workflow(dataset_path):
    rdd_raw = load_dataset(dataset_path)
    rdd = preprocess_rdd(rdd_raw)
    start_time = time.time()
    gc.collect()
    rdd = connected_components(rdd)
    component_count = rdd.map(lambda x: x[1]).distinct().count()
    end_time = time.time()
    total_time_taken = end_time - start_time
    print(f"Number of connected components: {component_count}")
    print(f"Time taken: {total_time_taken} seconds")


if __name__ == "__main__":
    inp_path = sys.argv[1]
    workflow(inp_path)
