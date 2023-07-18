# ---------------------------------
#       INDEX OF THE SECTIONS      
# ---------------------------------
# 1. INTRODUCTION
#    Brief explaination of how the code works
# 2. IMPORTS & TRESHOLD
#    Import of the libraries and setting of the treshold for the stopping condition
# 3. HASH FUNCTIONS & COUNT SKETCH
#    Implementation of a) the count sketch algorithm and b) of the two two hash
#    functions used inside it
# 4. PROCESS_BATCH FUNCTION
#    Function which deals with the processing of the single RDD
# 5. MAIN
#    Spark configuration, creation of the discretized stream
# 6. STATISTICS COMPUTATION & PRINTS
#    Statistics computation and prints of the final outputs

# ------------------------
#       INTRODUCTION
# ------------------------
# The code contains a MapReduce implementation for data streams of the Count Sketch
# algorithm, which is used to estimate the absolute frequency of a set of items
# when working with prohibitively large datasets.
# Follows the link to the original 2002 paper describing the Count Sketch algorithm:
# https://www.cs.princeton.edu/courses/archive/spring04/cos598B/bib/CharikarCF.pdf
# The program receives as input a continuous stream of integers items, which is
# transformed into a Discretized Stream of batches of items through Spark's method
# "socketTextStream()". The single batch can be interpreted as a RDD, and thus accessed
# through Spark's RDD methods.
# Before any transformation, the batch is filtered to exclude the items outside the 
# input's specified interval [left,right].
# On a batch per time, the Count Sketch algorithm is performed; specifically, the
# function "count_sketch()" extracts the j-th update of the sketch from the RDD
# through a MapReduce approach. The hash functions "h_hash()" and "g_hash()" are used to
# map the items' count into a lower dimensional space, specified by the input parameters
# W and D, which values determine the performance/accuracy tradeoff.
# The stream processing's stop is invoked after approximately 10M items have been read.
# The threshold is harcoded at the beginning of the code.
# Finally, the following statistics are computed:
# - The true and approximated frequencies of all distinct filtered items;
# - The true and approximated second moment F2 of all distinct filtered items;
# - The average relative error of the frequency estimate of all distinct filtered items,
#   computed as follows: |true_freq - approx_freq| / true_freq

# ------------------------------
#       IMPORTS & TRESHOLD
# ------------------------------
from pyspark import SparkContext, SparkConf
from pyspark.streaming import StreamingContext
from pyspark import StorageLevel
import numpy as np
import threading
import sys

# After how many items should we stop?
THRESHOLD = 10000000

# -----------------------------------------
#       HASH FUNCTIONS & COUNT SKETCH
# -----------------------------------------
# Hash function h: U -> {0, ..., W-1}
def h_hash(x,a,b):
    return ((a*int(x) + b) % p) % W

# Hash function g: U -> {-1, 1}
def g_hash(x,a,b):
    hash_value = ((a*int(x) + b) % p) % 2
    return 2*hash_value - 1

# Count sketch algorithm
def count_sketch(f_batch):
    global sketch
    # i.e. repeat for all the hash functions h1, ..., hD and g1,...,gD
    for j in np.arange(0, D):

        # GOAL: extract the j-th update of the sketch from the RDD through a MapReduce approach
        # 1) Map: from x to (hj(x),gj(x))
        # 2) Reduce: from (hj(x),gj(x)) to (hj(x), sum gj(x))
        # 3) collectAsMap: from (hj(x), sum gj(x)) to a dictionary {hj: sum gj(x)}

        # Note that the count sketch algorithm requires D hash functions:
        # U -> {0, ..., W-1} and D hash functions: U -> {-1, +1}. To achieve this
        # result, we use only two hash functions, "h_hash" and "g_hash", which
        # in total take 1,...,j,...,D*2 different "a" and "b" parameters

        sketch_dict = f_batch.map(lambda x: (h_hash(x,a[j],b[j]), g_hash(x,a[j+D],b[j+D]))) \
        .reduceByKey(lambda x, y: x + y) \
        .collectAsMap()

        # Update the sketch with the j-th update
        for key in sketch_dict:
            sketch[j,key] += sketch_dict[key]

# ----------------------------------
#       PROCESS_BATCH FUNCTION
# ----------------------------------
# Operations to perform after receiving an RDD 'batch' at time 'time'
def process_batch(time, batch):
    # We are working on the batch at time `time`.
    global streamLength, histogram

    # a. "batch_size": size of the current batch
    # b. "streamLength[0]": size of the stream so far, i.e. sigma
    # c. "streamLength[1]": size of the stream so far after filtering, i.e. sigma_R

    batch_size = batch.count()
    # If we already have enough points (> THRESHOLD), skip this batch.
    if streamLength[0]>=THRESHOLD:
        return
    streamLength[0] += batch_size

    # 1) Converts the strings of the RDD into integers
    # 2) Filter the elements in the batch which are not in the interval [left, right]
    filtered_batch = batch.map(lambda x: int(x)).filter(lambda x: (x >= left) & (x <= right))
    streamLength[1] += filtered_batch.count()

    # Extract in a dictionary the distinct items from the batch
    batch_items = filtered_batch.map(lambda s: (int(s), 1)).reduceByKey(lambda x, y: x + y).collectAsMap()

    # Store the items of "batch_items" and their frequencies into "hisogram"
    # i.e., histogram contains the true absolute frequencies of the items seen so far
    for key in batch_items:
        if key not in histogram:
            histogram[key] = batch_items[key]
        else:
            histogram[key] += batch_items[key]

    # Run the count sketch algorithm
    count_sketch(filtered_batch)

    # Set the stopping condition
    if streamLength[0] >= THRESHOLD:
        stopping_condition.set()

# ----------------
#       MAIN
# ----------------
if __name__ == '__main__':

    # If the number of command-line arguments is different from 7 (name of the file + 6
    # parameters), an error arises explaining the correct usage.
    assert len(sys.argv) == 7, "ERROR - USAGE: D, W, left, right, K, portExp"

    # Set the Spark configuration
    # IMPORTANT: when running locally, it is *fundamental* that the
    # `master` setting is "local[*]" or "local[n]" with n > 1, otherwise
    # there will be no processor running the streaming computation and your
    # code will crash with an out of memory (because the input keeps accumulating).
    conf = SparkConf().setMaster("local[*]").setAppName("G026HW3")
    # If you get an OutOfMemory error in the heap consider to increase the
    # executor and drivers heap space with the following lines:
    conf = conf.set("spark.executor.memory", "4g").set("spark.driver.memory", "4g")

    # Set the Spark Context and the Streaming Context. The parameter 0.1 in Streaming
    # Context defines the batch duration, i.e. the interval at which the input data
    # stream is divided into discrete batches for processing. In simple terms, this means
    # that with the argument you can control how large to make your batches.
    sc = SparkContext(conf=conf)
    ssc = StreamingContext(sc, 1)
    ssc.sparkContext.setLogLevel("ERROR")

    # TECHNICAL DETAIL:
    # The streaming spark context and our code and the tasks that are spawned all
    # work concurrently. To ensure a clean shut down we use this semaphore.
    # The main thread will first acquire the only permit available and then try
    # to acquire another one right after spinning up the streaming computation.
    # The second tentative at acquiring the semaphore will make the main thread
    # wait on the call. Then, in the `foreachRDD` call, when the stopping condition
    # is met we release the semaphore, basically giving "green light" to the main
    # thread to shut down the computation.
    # We cannot call `ssc.stop()` directly in `foreachRDD` because it might lead
    # to deadlocks.

    stopping_condition = threading.Event()

    # Input reading
    D = int(sys.argv[1]) # Sketch's number of rows
    W = int(sys.argv[2]) # Sketch's number of columns
    left = int(sys.argv[3]) # left endpoint of the interval of interest
    right = int(sys.argv[4]) # right endpoint of the interval of interest
    K = int(sys.argv[5]) # number of top frequent items of interest
    portExp = int(sys.argv[6]) # port number

    # Required data structures to maintain the state of the stream
    streamLength = [0,0] # Stream length (an array to be passed by reference)
    histogram = {} # Hash Table for the distinct elements
    approx_histogram = {} # Approximated Hash Table for the distinct elements

    # Initialise the ingredients of the hash functions
    p = 8191
    a = np.random.randint(low=1, high=p-1, size = D*2)
    b = np.random.randint(low=0, high=p-1, size = D*2)

    # Initialise the sketch
    sketch = np.zeros((D, W))

    # Create the Discretized Stream
    stream = ssc.socketTextStream("algo.dei.unipd.it", portExp, StorageLevel.MEMORY_AND_DISK)
    # For each batch, to the following.
    # BEWARE: the `foreachRDD` method has "at least once semantics", meaning
    # that the same data might be processed multiple times in case of failure.

    # For each RDD of the DStream, applies the function "process_batch"
    stream.foreachRDD(lambda time, batch: process_batch(time, batch))

    # Start a Spark Streaming engine, wait for a specified shutdown condition, and then
    # stop the engine
    print("Starting streaming engine")
    ssc.start()
    print("Waiting for shutdown condition")
    stopping_condition.wait()
    print("Stopping the streaming engine")
    # NOTE: You will see some data being processed even after the
    # shutdown command has been issued. This is because we are asking
    # to stop "gracefully", meaning that any outstanding work
    # will be done.
    ssc.stop(False, True)
    print("Streaming engine stopped")

    # ----------------------------------------
    #       OUTPUTS COMPUTATION & PRINTS
    # ----------------------------------------
    # Compute the approximate estimates obtained through count sketch
    for u in histogram:
        approx_freq_list = []
        for j in np.arange(0,D):
            approx_freq_list.append(g_hash(u,a[j+D],b[j+D]) * sketch[j,h_hash(u,a[j],b[j])])
        approx_histogram[u] = np.median(approx_freq_list)

    # Initialise useful structures for outputs computation
    desc_histogram = sorted(histogram.items(), key = lambda item: item[1], reverse = True)
    avg_error_list = []
    F2_list = [[],[]]

    # Print total n of items, total n of items in R, number of distinct items in R
    print("****** OUTPUT ******")
    print(f"D = {D} W = {W} [left,right] = [{left},{right}] K = {K} Port = {portExp}")
    print(f"Total number of items = {streamLength[0]}")
    print(f"Total number of items in [{left},{right}] = {streamLength[1]}")
    print(f"Number of distinct items in [{left},{right}] = {len(histogram)}")

    # If K<=20: print the top K frequencies in descending order,
    #           print the avg relative error of the top-K highest true frequencies
    #           print F2
    count = 1
    if K<=20:
        # For loop to print the top K freq, and to store values to compute the avg error and F2
        for k in np.arange(0,K):
            item = desc_histogram[k][0]
            true_freq = desc_histogram[k][1]
            approx_freq = approx_histogram[desc_histogram[k][0]]
            print(f"Item {item} Freq = {true_freq} Est. Freq = {approx_freq}")
            avg_error_list.append(abs(true_freq-approx_freq)/true_freq)          
            F2_list[0].append(true_freq**2)
            F2_list[1].append(approx_freq**2)
        # Ensure to print all the items even if K, K+1, ... have all the same frequencies
        while True:
                if approx_histogram[desc_histogram[K-1][0]] == approx_histogram[desc_histogram[K-1+count][0]]:
                    count += 1
                    item = desc_histogram[K+count][0]
                    true_freq = desc_histogram[K+count][1]
                    approx_freq = approx_histogram[desc_histogram[K+count][0]]
                    print(f"Item {item} Freq = {true_freq} Est. Freq = {approx_freq}")
                    avg_error_list.append(abs(true_freq-approx_freq)/true_freq)
                else:
                    break
        # For loop to store the remaining values to compute F2
        for k in np.arange(K, len(histogram)):
            F2_list[0].append(true_freq**2)
            F2_list[1].append(approx_freq**2)
        # Compute and print the avg error and F2
        print(f"Avg err for top {K} = {np.mean(avg_error_list)}")
        print(f"F2 {sum(F2_list[0])/(streamLength[1]**2)} F2 Estimate {sum(F2_list[1])/(streamLength[1]**2)}")

    # If K> 20: print the avg relative error of the top-K highest true frequencies
    #           print F2
    else:
        # For loop to store values to compute the avg error and F2
        for k in np.arange(0,K):
            true_freq = desc_histogram[k][1]
            approx_freq = approx_histogram[desc_histogram[k][0]]
            avg_error_list.append(abs(true_freq-approx_freq)/true_freq)
            F2_list[0].append(true_freq**2)
            F2_list[1].append(approx_freq**2)
        # Ensure to compute the average error considering K, K+1, ... if they have the same frequency
        while True:
                if approx_histogram[desc_histogram[K-1][0]] == approx_histogram[desc_histogram[K-1+count][0]]:
                    count += 1
                    true_freq = desc_histogram[K+count][1]
                    approx_freq = approx_histogram[desc_histogram[K+count][0]]
                    avg_error_list.append(abs(true_freq-approx_freq)/true_freq)
                else:
                    break
        # For loop to store the remaining values to compute F2
        for k in np.arange(K,len(histogram)):
            F2_list[0].append(true_freq**2)
            F2_list[1].append(approx_freq**2)
        # Compute and print the avg error and F2
        print(f"Avg err for top {K} = {np.mean(avg_error_list)}")
        print(f"F2 {sum(F2_list[0])/(streamLength[1]**2)} F2 Estimate {sum(F2_list[1])/(streamLength[1]**2)}")