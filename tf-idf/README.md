# TF-IDF

This page documents the tf-idf-sets.py file which is my final working implementation of the tf-idf algorithm for a
person entity matching project.

A general overview of tf-idf can be found on [Wikipedia](https://en.wikipedia.org/wiki/Tf%E2%80%93idf)

### Overview
The following code implements an idf algorithm (tf-idf) in near linear time using Spark to generate
similarity scores between two collections of person data. The fields of the data for each collection are:
```first name, last name, middle initial, state, city, zip, address, birth month-year```

The algorithm leverages broadcast variables to run in as quick as linear time.  The smaller collection is broadcasted
as a hashmap to every node in the cluster.  The algorithm reads through the larger collection and looks up common words
in the broadcast variables of the smaller collection in constant time.  In a perfect world this would require one pass.
However, running the entire algo in one pass requires more space then is available in each executor node in spark.
Therefore I used a batching technique to run the algorithm 100 times on 1/100th of the data each time.
This technique essentially trades off time for space.  The 100 slices of the data were assigned randomly and are referred
to as meta buckets in the code

##### Broadcast Variables
In this implementation the two collections of person data were of vastly different sizes, with the smaller collection
being able to fit in memory on each node using broadcast variables.  Had the smaller collection been too big,
it would still be possible to run the algo multiple times on subsets of the smaller person collection that does fit in memory.
Again, this would trade time for space, but would still be considered linear O(n) time as it is upper bounded by some constant.
The number of reads would be:
```[pre-processing reads] + [# of subsets of the smaller collection that fit in memory] X [# of slices] X [# of words in the larger collection (N)] = constant + constant X constant X N = O(n)```

#### IDF Only
In a normal tf-idf calculation, the term frequency (tf) is used to give a metric as to how important a word is to a
specific document.  However when comparing specific entities such as people with distinct fields (first-name, last-name, address etc..),
the TF score for every document is 1.  This is essentially the Boolean version of the tf-idf described in the [Wikipedia](https://en.wikipedia.org/wiki/Tf%E2%80%93idf) page.

#### Place low cardinality fields in a hashset, and high cardinality fields in list
Matching on person data also gives the additional ability to use the domain knowledge about each field in order to tune the efficiency of
the algorithm.  In a normal tf-idf algorithm, any two documents that have any word in common will produce a data point with a score,
even if that word is a low information, common word.  Because we are matching person data we can predetermine which fields are
the most important to start the matching on based on their cardinality.  For the person data I assume:
the high cardinality fields are: `first name, last name,  address`
and the low cardinality fields are `birth month-year, middle init, zip, city, state`.

In the algorithm, as each person in the larger collection is read, it first pulls in every person from the smaller collection
that matches on one of the high cardinality fields.  So if a person from the smaller collection doesn't match on one of the three
high cardinality fields, that person is not considered in this implementation.  Once it has this initial set of
potential matches from the smaller collection, it then checks if these matches have additional fields in common in the low cardinality words.
These additional matches from the low cardinality fields add to the scores of the existing potential matches, but they do not create new matches.

In order to accomplish this in the pySpark code I placed the corresponding person ids for the low cardinality words in a hashset,
and the high cardinality words in a list.  Both of these are put in broadcast variables so each executor has the entire small collection
in local memory.  For each person in the large collection, the list of potential matches for each high cardinality field, and the hashset of
potential matches for the low cardinality fields are pulled out of the local memory broadcast variables.

Then each id in the high cardinality lists are checked for existence against the low cardinality sets.  If there is a match, then the score
of the word for the low cardinality set is added to that match.


## High level algorithm step by step walk-through

1. Confgure Spark Correctly -  Make sure you start spark with the appropriate amount of memory for the driver, executors,
serializer buffers, and use kryoserialzier.
2. Read through both collections of data to generate the idf score for each word.
3. Read through the smaller collection of data and transform it into two different types of rdds based on the cardinality of the field
  1. High Cardinality fields:  The word is the key, and the values are tuples comprised of ids and the idf score:
      ```[(word, [(small_collection_id1, tf_score), (small_collection_id2, tf_score)...])]```
  1. Low Cardinality fields:  The word is the key, and the values are the idf score, and hashset of all ids corresponding to that word:
      ```[(word, (idf_score, {Hash Set of ids}))]```
4. Pull that RDD into memory as a hashmap in the driver node, then broadcast it to all worker nodes.  When reading through the bigger
collection in the final loop of the algo, this dictionary will allow constant time lookup to find the ids and scores of the
smaller collection that have words in common.
5. Read through the larger collection one set of meta buckets at a time.  I ran it successfully by doing 1/100th of the total data per run
6. For each meta bucket, read through the larger collection person by person.
  1. For each person, first lookup the high cardinality fields in the broadcasted lists.  This will return a batch of ids and a score:
      ```[(id1, high_cardinality_score1), (id2, high_cardinality_score2) ...]```
  1. Convert this list into a hashmap of id and total score:
      ```{id1: score1, id2, score2 ...}```
7. Lookup the existence of each id found above in the broadcasted hashmaps of the low cardinality words.
   If there is a hit, then add the score of the word found in the broadcasted set to the existing id in the hash map:
8. Output the hash map into a list, sort the list, and return the three id, score combos with the highest scores:
     ```[(id45, highest_total_score), (id77, second_highest_total-highest_total_score), (id642, third_highest_total_score-score) ...]```
9. Reformat for final output
   combining the id from the large collection with the three highest ids found from the smallest collection:
   ```[(large_id1, small_id45, highest_total_score), (large_id1, small_id77, second_highest_total_score), (large_id1, small_id642, third_highest_total_score)]```
10. Output the results to a S3.
11. Copy the results into a Redshift table.
12. Run a series of SQL commands to pull in matches based on match scores, and name substrings.
13. For matches with high scores where the names are different, compute the string similarity scores using the
    fuzzywuzzy python module which calculates the Levenshtein string similarity score.



# Spark Learnings

A lot of the time I spent working on this project was spent dealing with spark issues, and learning how to properly tune both
spark, and my algorithm to manage memory usage.

This page was particularly useful: [part1](http://blog.cloudera.com/blog/2015/03/how-to-tune-your-apache-spark-jobs-part-1/)
and [part2](http://blog.cloudera.com/blog/2015/03/how-to-tune-your-apache-spark-jobs-part-2/)

For the tf-idf algo I started my pySpark application with following configuration using r3.xlarge instances:
```pyspark --conf spark.kryoserializer.buffer.max=1g --conf spark.driver.memory=20g --conf spark.executor.instances=19 --conf spark.executor.memory=4g --conf spark.executor.cores=4 --conf spark.yarn.executor.memoryOverhead=18g --conf spark.serializer=org.apache.spark.serializer.KryoSerializer```

* `spark.serializer=org.apache.spark.serializer.KryoSerializer` -  Spark uses a default spark serializer to
    serialize data and objects when spilling to disk or sending over the network.  Most documents advise
    using the kryoserializer which is supposed to be much faster.
* `spark.kryoserializer.buffer.max=1g` -  This increases the buffer size of the amount of data that can be sent
    over the network at any one time.  I needed this as my broadcast variables were too large, and were throwing
    network buffer errors
* `spark.driver.memory=20g` -  By default Spark will only set a very small amount of memory to be used in the driver node.
    I needed a larger amount for the broadcast variables
* `spark.executor.instances=19`  - This sets the number of worker nodes.  This variable however is not really needed as
    the current version of Spark on YARN on EMR knows how many executor nodes are in the cluster at any time.  If you use
    the auto scaling feature on EMR (which removes and adds nodes as needed) this will all be automatically handled
* `spark.executor.memory=4g`, `spark.yarn.executor.memoryOverhead=18g`  - The amount of memory spark allocates to the executor
    nodes by default is very small.  Use these settings to increase those amounts.  Note there are two different types of
    executor memories being set here as they are used for different things.  A full explanation can be found in part2
    mentioned above, but in short the executor-memory is the heap size (for storing in memory objects),
    while the executor-memoryOverhead is used by spark to handle the movement of data caused by your rdds and
    your code such as shuffles and joins.  The tf-idf algo required a lot of shuffling and joining of the data so I set the
    memoryOverhead to get much more of the total memory then the heap memory.  Note that there is a total limit on the amount
    of memory you can use and setting one higher will require lowering the other one.  I also have read that pySpark utilizes
    python memory completely outside of the heap, which might have helped alleviate the need for a lot of executor heap memory

### Auto Scaling

I leveraged EMR's autoscaling in this project by using some simple rules:

* `Add 2 instances if ContainerPendingRatio is greater than or equal to .85 for 2 five-minute periods with a cooldown of 300 seconds`

* `Add 2 instances if YARNMemoryAvailablePercentage is less than or equal to .15 for 2 five-minute period swith a cooldown of 300 seconds`

* `Terminate instances if YARNMemoryAvailablePercentage is greater than or equal to 85 for 2 five-minute periods with a cooldown of 300 seconds`