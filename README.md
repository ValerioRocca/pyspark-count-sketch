# Count Sketch algorithm implementation
The code contains a MapReduce implementation for data streams of the Count Sketch algorithm, which is used to estimate the absolute frequency of a set of items when working with prohibitively large datasets. [Link to the original paper describing the Count Sketch algorithm](https://www.cs.princeton.edu/courses/archive/spring04/cos598B/bib/CharikarCF.pdf).

The program receives as input a continuous stream of integers items, which is transformed into a Discretized Stream of batches of items. 

On a batch per time, the Count Sketch algorithm is performed; specifically, the
function "count_sketch()" extracts the j-th update of the sketch from the RDD
through a MapReduce approach. The hash functions "h_hash()" and "g_hash()" are used to
map the items' count into a lower dimensional space, specified by the input parameters
W and D, which values determine the performance/accuracy tradeoff.

The stream processing's stop is invoked after approximately 10M items have been read. Finally, the following statistics are computed:
- The true and approximated frequencies of all distinct filtered items;
- The true and approximated second moment F2 of all distinct filtered items;
- The average relative error of the frequency estimate of all distinct filtered items,
  computed as follows: |true_freq - approx_freq| / true_freq

### Files in the repository
- _count-sketch-code_, containing the Python code;
- _count-sketch-results_, which is a PDF table summarizing the main results obtained.
