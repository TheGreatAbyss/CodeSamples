# Step 1 enter pyspark using the below configuration with r3.xlarge instances
# pyspark --conf spark.kryoserializer.buffer.max=1g --conf spark.driver.memory=20g --conf spark.executor.instances=19 --conf spark.executor.memory=4g --conf spark.executor.cores=4 --conf spark.yarn.executor.memoryOverhead=18g --conf spark.serializer=org.apache.spark.serializer.KryoSerializer --conf yarn.nodemanager.pmem-check-enabled=false --conf spark.executor.extraJavaOptions=-XX:OldSize=100m --conf spark.executor.extraJavaOptions=-XX:MaxNewSize=100m


# Use the below to check spark configuration
for item in sc._conf.getAll():
    print(item) 


sc.setLogLevel('INFO')

import datetime as dt
from collections import defaultdict
from math import log10
from functools import partial

large_collection_raw_rdd = sc.textFile("s3://example_bucket/tf-idf_text_mining_machine_learning/input/large_collection/")
small_collection_raw_rdd = sc.textFile("s3://example_bucket/tf-idf_text_mining_machine_learning/input/small_collection_limited/")

# Step 2
# This preprocessing section computes the score for each word seen throughout both documents.

def split_text_into_list_remove_blanks(text):
    word_list = text.split("|")
    stripped = list(filter(None, list(map(lambda word: word.strip(), word_list))))
    id = stripped[0]
    word_list = list(set(stripped[1:])) # convert to a set first to dedup any repeated words
    return (id, word_list)

# The small_collection data has the additional meta_bucket field prepened to it to allow for chunk processing
def split_text_into_list_remove_blanks_meta_buckets(text):
    word_list = text.split("|")
    stripped = list(filter(None, list(map(lambda word: word.strip(), word_list))))
    meta_bucket = stripped[0]
    id = stripped[1]
    word_list = list(set(stripped[1:])) # convert to a set first to dedup any repeated words
    return (id, word_list)    

large_collection_no_blanks_rdd = large_collection_raw_rdd.map(split_text_into_list_remove_blanks_meta_buckets)
small_collection_no_blanks_rdd = small_collection_raw_rdd.map(split_text_into_list_remove_blanks)

#[(id, [word1, word2, word3]), (id2, [word3, word 5])...]

combined_rdd = sc.union([large_collection_no_blanks_rdd, small_collection_no_blanks_rdd])

# N
count_of_docs = sc.broadcast(combined_rdd.count())

def key_up_words_for_count(word_list):
    kv_list = []
    for word in word_list:
        kv_list.append((word, 1))
    return kv_list

count_of_each_word_rdd = combined_rdd.flatMap(lambda tup: key_up_words_for_count(tup[1])).reduceByKey(lambda a,b: a + b)

# The idf score for each word is log(N/count_of_word)
# For this use case I am using the boolean frequency as there shouldn't be a reason for words to repeat.  
# Therefore each word should only occur once in each document. 
# Because we know the tf is always 1, and we know the count of each word, we can pre-compute the tf-idf_text_mining_machine_learning score for each word.

def create_tf_idf(word, word_count):
    idf_score = log10(count_of_docs.value/word_count) 
    return (word,idf_score)

tf_idf_rdd = count_of_each_word_rdd.map(create_tf_idf)
# [(word1: tf_idf_score), (word2: tf_idf_score) ...]

# Step 3
# Read and transform the smaller dataset into a word to id_list and tf-idf_text_mining_machine_learning score lookup
def split_text_into_list(text):
    word_list = text.split("|")
    stripped = list(map(lambda word: word.strip(), word_list))
    id = stripped[0]
    word_list = list(stripped[1:])
    return (id, word_list)

small_collection_rdd = small_collection_raw_rdd.map(split_text_into_list)

# Step 3a
# For the high cardinality fields, convert the ids to a list of ids to be used later to find the initial batch of matches
def map_out_match_word_to_list(small_collection_tup, index):
    small_collection_id, word_list = small_collection_tup
    word = word_list[index]
    if word:
        return (word, [small_collection_id])
    else:
        return (word, [])

# Step 3b
# For the low cardinality fields, convert the ids to a set of ids to be used later to find if the ids in the initial batch of 
# matches have additinal matches, and the corresponding score
def map_out_match_word_to_set(small_collection_tup, index):
    small_collection_id, word_list = small_collection_tup
    word = word_list[index]
    if word:
        return (word, set([small_collection_id]))
    else:
        return (word, None)

# High cardinality fields
first_name_mapper = partial(map_out_match_word_to_list, index=0)
last_name_mapper = partial(map_out_match_word_to_list, index=1)
address_mapper = partial(map_out_match_word_to_list, index=6)

# Low cardinality fields
birth_mapper = partial(map_out_match_word_to_set, index=7)
middle_init_mapper = partial(map_out_match_word_to_set, index=2)
zip_mapper = partial(map_out_match_word_to_set, index=3)
state_mapper = partial(map_out_match_word_to_set, index=4)
city_mapper = partial(map_out_match_word_to_set, index=5)

def filter_out_empties(tup):
    (word, item) = tup
    if item:
        return True
    else:
        return False 

first_name_rdd = small_collection_rdd.map(first_name_mapper).filter(filter_out_empties).reduceByKey(lambda list_a, list_b: list_a + list_b)
last_name_rdd = small_collection_rdd.map(last_name_mapper).filter(filter_out_empties).reduceByKey(lambda list_a, list_b: list_a + list_b)
address_rdd = small_collection_rdd.map(address_mapper).filter(filter_out_empties).reduceByKey(lambda list_a, list_b: list_a + list_b)

birth_rdd = small_collection_rdd.map(birth_mapper).filter(filter_out_empties).reduceByKey(lambda set_a, set_b: set_a.union(set_b))
zip_rdd = small_collection_rdd.map(zip_mapper).filter(filter_out_empties).reduceByKey(lambda set_a, set_b: set_a.union(set_b))
city_rdd = small_collection_rdd.map(city_mapper).filter(filter_out_empties).reduceByKey(lambda set_a, set_b: set_a.union(set_b))
middle_init_rdd = small_collection_rdd.map(middle_init_mapper).filter(filter_out_empties).reduceByKey(lambda set_a, set_b: set_a.union(set_b))
state_rdd = small_collection_rdd.map(state_mapper).filter(filter_out_empties).reduceByKey(lambda set_a, set_b: set_a.union(set_b))

# [(large_collection_word1, [small_collection_id1, small_collection_id2])...]

# Step 3a
# Change the format of the high cardinality rdds by combining the score with each id in the list.
# word: (id_list, tf_idf_score) 
# to 
# word: [(id1, tf_idf_score), (id2, tf_idf_score)...].  
# While this seems space inefficient, it is actually more time efficient later as this won't have to be repeated 
# for every iteration of the algo.
def cartesian_join_word_and_small_collection_ids(tup):
    (word, (small_collection_id_list, tf_score)) = tup
    return (word, [(small_collection_id, tf_score) for small_collection_id in small_collection_id_list])

first_name_tf_idf_rdd = first_name_rdd.join(tf_idf_rdd).map(cartesian_join_word_and_small_collection_ids)
last_name_tf_idf_rdd = last_name_rdd.join(tf_idf_rdd).map(cartesian_join_word_and_small_collection_ids)
address_tf_idf_rdd = address_rdd.join(tf_idf_rdd).map(cartesian_join_word_and_small_collection_ids)
# [(word, [(small_collection_id, tf_score), (small_collection_id, tf_score)...])]


# For the low cardinality words just join with tf_idf, don't do the cartesion expansion
birth_set_tf_idf_rdd = birth_rdd.join(tf_idf_rdd)
zip_set_tf_idf_rdd = zip_rdd.join(tf_idf_rdd)
city_set_tf_idf_rdd = city_rdd.join(tf_idf_rdd)
middle_init_set_tf_idf_rdd = middle_init_rdd.join(tf_idf_rdd)
state_set_tf_idf_rdd = state_rdd.join(tf_idf_rdd)
# [(large_collection_word1, (small_collection_id_list, tf-idf_text_mining_machine_learning-score)



# If the job isn't completing, or getting stuck in a loop of failing executors use the below to store progress
# By saving these intermediary rdds the job can easily be restarted from this point without having to rerun the above code.
now = dt.datetime.now()

tf_idf_rdd.saveAsPickleFile("s3://example_bucket/tf-idf_text_mining_machine_learning/intermediate/tf_idf_rdd/" + now.strftime('%m_%d_%Y_%H_%M') + "/")
first_name_tf_idf_rdd.saveAsPickleFile("s3://example_bucket/tf-idf_text_mining_machine_learning/intermediate/first_name_tf_idf_rdd/" + now.strftime('%m_%d_%Y_%H_%M') + "/")
last_name_tf_idf_rdd.saveAsPickleFile("s3://example_bucket/tf-idf_text_mining_machine_learning/intermediate/last_name_tf_idf_rdd/" + now.strftime('%m_%d_%Y_%H_%M') + "/")
address_tf_idf_rdd.saveAsPickleFile("s3://example_bucket/tf-idf_text_mining_machine_learning/intermediate/address_tf_idf_rdd/" + now.strftime('%m_%d_%Y_%H_%M') + "/")

birth_set_tf_idf_rdd.saveAsPickleFile("s3://example_bucket/tf-idf_text_mining_machine_learning/intermediate/birth_set_tf_idf_rdd/" + now.strftime('%m_%d_%Y_%H_%M') + "/")
zip_set_tf_idf_rdd.saveAsPickleFile("s3://example_bucket/tf-idf_text_mining_machine_learning/intermediate/zip_set_tf_idf_rdd/" + now.strftime('%m_%d_%Y_%H_%M') + "/")
state_set_tf_idf_rdd.saveAsPickleFile("s3://example_bucket/tf-idf_text_mining_machine_learning/intermediate/state_set_tf_idf_rdd/" + now.strftime('%m_%d_%Y_%H_%M') + "/")
middle_init_set_tf_idf_rdd.saveAsPickleFile("s3://example_bucket/tf-idf_text_mining_machine_learning/intermediate/middle_init_set_tf_idf_rdd/" + now.strftime('%m_%d_%Y_%H_%M') + "/")
city_set_tf_idf_rdd.saveAsPickleFile("s3://example_bucket/tf-idf_text_mining_machine_learning/intermediate/city_set_tf_idf_rdd/" + now.strftime('%m_%d_%Y_%H_%M') + "/")

# On restart of the job
birth_set_tf_idf_rdd = sc.pickleFile("s3://example_bucket/tf-idf_text_mining_machine_learning/intermediate/birth_set_tf_idf_rdd/03_02_2017_21_42/")
zip_set_tf_idf_rdd = sc.pickleFile("s3://example_bucket/tf-idf_text_mining_machine_learning/intermediate/zip_set_tf_idf_rdd/03_02_2017_21_42/")
state_set_tf_idf_rdd = sc.pickleFile("s3://example_bucket/tf-idf_text_mining_machine_learning/intermediate/state_set_tf_idf_rdd/03_02_2017_21_42/")
middle_init_set_tf_idf_rdd = sc.pickleFile("s3://example_bucket/tf-idf_text_mining_machine_learning/intermediate/middle_init_set_tf_idf_rdd/03_02_2017_21_42/")
city_set_tf_idf_rdd = sc.pickleFile("s3://example_bucket/tf-idf_text_mining_machine_learning/intermediate/city_set_tf_idf_rdd/03_02_2017_21_42/")

tf_idf_rdd = sc.pickleFile("s3://example_bucket/tf-idf_text_mining_machine_learning/intermediate/tf_idf_rdd/02_24_2017_18_00/")
first_name_tf_idf_rdd = sc.pickleFile("s3://example_bucket/tf-idf_text_mining_machine_learning/intermediate/first_name_tf_idf_rdd/03_02_2017_21_42/")
last_name_tf_idf_rdd = sc.pickleFile("s3://example_bucket/tf-idf_text_mining_machine_learning/intermediate/last_name_tf_idf_rdd/03_02_2017_21_42/")
address_tf_idf_rdd = sc.pickleFile("s3://example_bucket/tf-idf_text_mining_machine_learning/intermediate/address_tf_idf_rdd/03_02_2017_21_42/")



# Step 4 - Broacdcast the smaller collection into memory
first_name_tf_idf_dict = sc.broadcast(first_name_tf_idf_rdd.collectAsMap())
last_name_tf_idf_dict = sc.broadcast(last_name_tf_idf_rdd.collectAsMap())
address_tf_idf_dict = sc.broadcast(address_tf_idf_rdd.collectAsMap())
# {word: [id, score]}


birth_set_tf_idf_dict = sc.broadcast(birth_set_tf_idf_rdd.collectAsMap())
middle_init_set_tf_idf_dict = sc.broadcast(middle_init_set_tf_idf_rdd.collectAsMap())
zip_set_tf_idf_dict = sc.broadcast(zip_set_tf_idf_rdd.collectAsMap())
state_set_tf_idf_dict = sc.broadcast(state_set_tf_idf_rdd.collectAsMap())
city_set_tf_idf_dict = sc.broadcast(city_set_tf_idf_rdd.collectAsMap())
# {word: (bloom_filter, tf_idf_score)}

# Helper function to set the size of the slices of data you want to run in each batch
def chunker(seq, size):
    return (seq[pos:pos + size] for pos in xrange(0, len(seq), size))

# Filters the large collection for just the slice of data you want to run in each iteration of the algo
def filter_meta(tup, meta_set):
    (meta_bucket, large_collection_id, [first, last, middle_init, zip_code, state, city, address, birth]) = tup
    if int(meta_bucket) in meta_set:
        return True
    else:
        return False

def convert_tup_to_string(tup):
    small_collection_id, large_collection_id, score = tup
    return str(small_collection_id) + "," + str(large_collection_id) + "," + str(score)

# Steps 6 - 9
# This is the main workhorse of the algorithm which looks at each person of the large collection, and generates the 
# top 3 matches from the broadcasted smaller collection.
def compare_large_collection_against_small_collection(tup):
    (meta_bucket, large_collection_id, [first, last, middle_init, zip_code, state, city, address, birth]) = tup

    # Step 6a
    # Find all matches on the high cardinality fields
    first_name_id_score_list = first_name_tf_idf_dict.value.get(first, [])
    last_name_id_score_list = last_name_tf_idf_dict.value.get(last, [])
    address_id_score_list = address_tf_idf_dict.value.get(address, [])

    #Create a combined list of ids and scores
    full_id_score_list = first_name_id_score_list + last_name_id_score_list + address_id_score_list
    #[(small_collection_id1, score), (small_collection_id1, score2), (small_collection_id2, score3)...]

    # Step 6b
    # Convert this list into a hash map and aggregate scores for each small_collection_id
    score_dict = defaultdict(int)
    for (small_collection_id, score) in full_id_score_list:
        score_dict[small_collection_id] += score
    
    # Step 7
    # Get disctinct small_collection_ids found in the high cardinality words
    distinct_small_collection_id_list = list(score_dict.keys())

    # Get the hash set of ids, and the idf score for each of the low cardinality words
    middle_init_set, middle_score = middle_init_set_tf_idf_dict.value.get(middle_init, (set([]), 0))
    zip_set, zip_score = zip_set_tf_idf_dict.value.get(zip_code, (set([]), 0))
    state_set, state_score = state_set_tf_idf_dict.value.get(state, (set([]), 0))
    city_set, city_score = city_set_tf_idf_dict.value.get(city, (set([]), 0))
    birth_set, birth_score = birth_set_tf_idf_dict.value.get(birth, (set([]), 0))
    
    # For each id found in the high cardinality words, see if they are in the hash set.  If they are, then add their score
    for small_collection_id in distinct_small_collection_id_list:
        if small_collection_id in middle_init_set:
            score_dict[small_collection_id] += middle_score
        if small_collection_id in zip_set:
            score_dict[small_collection_id] += zip_score
        if small_collection_id in state_set:
            score_dict[small_collection_id] += state_score
        if small_collection_id in city_set:
            score_dict[small_collection_id] += city_score
        if small_collection_id in birth_set:
            score_dict[small_collection_id] += birth_score            
    # The score dictionary now has the total score associated with each id.  
    
    # Step 8
    # Iterate through and output the values with the large_collection_id for final output
    scores_list = []
    for small_collection_id, score in score_dict.items():
        scores_list.append((small_collection_id, large_collection_id, score))
    # Return the top 3 scores
    scores_list_sorted_top_3 = sorted(scores_list, key=lambda tup: -tup[2])[:3]
    
    # Step 9
    # Reformat to string for later output
    return list(map(convert_tup_to_string, scores_list_sorted_top_3))


def split_text_into_list_meta_bucket(text):
    word_list = text.split("|")
    stripped = list(map(lambda word: word.strip(), word_list))
    meta_bucket = stripped[0]
    id = stripped[1]
    word_list = list(stripped[2:])
    return (meta_bucket, id, word_list)

large_collection_rdd = large_collection_raw_rdd.map(split_text_into_list_meta_bucket)

chunk_size = 100
counter = 0
now = dt.datetime.now()
for chunk in chunker(range(10000), chunk_size):
    meta_set = set(chunk)
    large_collection_filtered_meta_rdd = large_collection_rdd.filter(lambda tup: filter_meta(tup, meta_set))
    final_rdd = large_collection_filtered_meta_rdd.flatMap(compare_large_collection_against_small_collection)
    final_rdd.saveAsTextFile("s3://example_bucket/tf-idf_text_mining_machine_learning/output/" + now.strftime('%m_%d_%Y_%H_%M') + "/" + str(counter) + "/")
    counter += chunk_size