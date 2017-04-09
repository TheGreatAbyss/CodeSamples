# High Level Overview
# The below code is essentially an algorithim that was designed to compare rolling 30 day booker rates between two test groups using Spark Python.
# In order to compute results for each day, we need the MIN data point for Event_1, and the MAX data point for Event_2
# It follows the following high level steps:
# 1) Pull in 30 days worth of data starting on the first day of the input date range. Compute Min(Event_1), Max(Event_2)
# 2) Compute results for the first day
# 3) Begin a loop that moves forwards each day
# 4) During each iteration of the loop delete the last days worth of data, and pull in the next days worth of data.  Recompute Min(Event_1), Max(Event_2), 
# 5) Generate results for each day.

# Whats with those dictionaries?
# One problem we ran into was that Garbage Collection on the Java Heap wasn't keeping up with our code, and we were getting memory errors.
# In order to combat this set the RDD created during each iteration of the loop to a value in a Dictionary.
# At the end of the iteration the RDD from the last iteration is unpersisted on the cluster. 

import datetime as dt

# pyspark --num-executors 59 --executor-memory 10g --executor-cores 4 --driver-memory 4g --serializer org.apache.spark.serializer.KryoSerializer
# export PYTHONPATH=$SPARK_HOME/python:$SPARK_HOME/python/build:$PYTHONPATH

import os
spark_home = os.environ.get('SPARK_HOME', None)
from pyspark import *

conf = SparkConf()
conf.setAll([('spark.executor.instances','59'),('spark.executor.cores','4'),('spark.executor.memory','10g'),('spark.driver.memory', '4g'),('spark.serializer', 'org.apache.spark.serializer.KryoSerializer')])
sc = SparkContext(conf = conf)

sc.addPyFile("s3://company-hawk-output/spark_python/im_library/spark_python_functions.py")
from spark_python_functions import *

def save_file(l):
    now = dt.datetime.now().strftime("%m-%d-%y-%H-%M-%S")
    filelocation = "s3://company-hawk-output/ppa_rolling_brd/run_with_steps_test/" + now
    
    test_rdd = sc.parallelize(l)
    test_rdd.saveAsTextFile(filelocation)

save_file(conf.getAll())

EVENT_1s_broadcast = load_EVENT_1_dictionary(sc)
EVENT_4s_broadcast = load_EVENT_4_dictionary(sc)

# initialize rdds
EVENT_4s_rdd = sc.parallelize([])
visitors_rdd = sc.parallelize([])
bernie_rdd = sc.parallelize([])
next_day_EVENT_1s_rdd = sc.parallelize([])
next_day_EVENT_4s_rdd = sc.parallelize([])
union_EVENT_1s_rdd = sc.parallelize([])
union_bookers_rdd = sc.parallelize([])
result_rdd = sc.parallelize([])

# list to save the results
# each element will be a tuple of (date, control_br, test_br, brd)
results = []

# rdd dictionary
EVENT_1s_rdd_dict = {}
bookers_rdd_dict = {}

MEMORY_AND_DISK = StorageLevel(True, True, False, True, 1)

# ### Functions for EVENT_1 Data

# union two rdds
def union_rdds(rdd1, rdd2):
    union_rdd = rdd1.union(rdd2)
    return union_rdd

# get EVENT_1s with FILTERABLE_FIELD_1 = 0 and id_1 not empty, [start_date, end_date)
def get_EVENT_1s_rdd(start_date, end_date):
    # get raw data
    data_start_date = dt.datetime.strptime(start_date, '%m/%d/%Y') + dt.timedelta(days=-1)
    raw_EVENT_1s_rdd = load_EVENT_1s(sc, data_start_date.strftime('%m/%d/%Y'), end_date, sale_types = ['HOTELS'], products = ['META'], publishers = ['ENTITY_1'])
    
    # filter EVENT_1s with FILTERABLE_FIELD_1 = 0 and id_1 not empty
    filtered_EVENT_1s_rdd = (raw_EVENT_1s_rdd.map(lambda x: (
                                                    get_field(x, EVENT_1s_broadcast, "id_1"),
                                                    (get_field(x, EVENT_1s_broadcast, "type_1"),
                                                    dt.datetime.strptime(toEST_time(get_field(x, EVENT_1s_broadcast, "requested_at")),'%m-%d-%Y %H:%M:%S'),
                                                    get_field(x, EVENT_1s_broadcast, "FILTERABLE_FIELD_1"),
                                                    get_field(x, EVENT_1s_broadcast, "id_2"))
                                                   )
                                              )
                                         .filter(lambda (id_1, (t, r, i, au)):
                                                   (i == False 
                                                    and r >= dt.datetime.strptime(start_date, '%m/%d/%Y')
                                                    and r < dt.datetime.strptime(end_date, '%m/%d/%Y')
                                                    and id_1 != ''
                                                    and au == 129
                                                 )
                                              )
                         )
    return filtered_EVENT_1s_rdd

# drop data < date from the rdd
def remove_date_from_rdd(EVENT_1s_rdd, date):
    filtered_rdd = EVENT_1s_rdd.filter(lambda (id_1, (t, r, i, au)):
                                        (
                                            r >= date
                                        )
                                      )
    return filtered_rdd

zero_value = (set([]), dt.datetime.now())

def seqOp((t1,r1), (t2, r2, i2, au2)):
    return (t1.union(set([t2])), min(r1,r2))

def combineOp((t1,r1), (t2,r2)):
    return (t1.union(t2), min(r1,r2))

# find clean users (only 1 traffic type) and the min EVENT_1 request time
def get_clean_user_with_min_EVENT_1_time_rdd(EVENT_1s_rdd):
    # get distinct type_1 and min(date) for each user
    user_data_rdd = EVENT_1s_rdd.aggregateByKey(zero_value, seqOp, combineOp)
    
    # get users with only 1 traffic share type
    clean_users_with_min_EVENT_1_time_rdd = user_data_rdd.filter(lambda (id_1, (t, r)):
                                                (len(t) == 1) 
                                               )
    return clean_users_with_min_EVENT_1_time_rdd

# ### Functions for EVENT_4 Data
# get EVENT_4s [start_date, end_date)
def get_EVENT_4s_rdd(start_date, end_date):
    # get raw EVENT_4 data
    data_start_date = dt.datetime.strptime(start_date, '%m/%d/%Y') + dt.timedelta(days=-1)
    raw_EVENT_4_rdd = load_EVENT_4s(sc, data_start_date.strftime('%m/%d/%Y'), end_date, sale_types = ['HOTELS'], publishers = ['ENTITY_1'])
    
    # filter EVENT_4s with: FILTERABLE_FIELD_1 = 0, id_1 is not null, EVENT_4_value > 0 
    filtered_EVENT_4s_rdd = (raw_EVENT_4_rdd.map(lambda x: (
                                                        get_field(x, EVENT_4s_broadcast, "id_1"),
                                                        (
                                                        dt.datetime.strptime(toEST_time(get_field(x, EVENT_4s_broadcast, "requested_at")),'%m-%d-%Y %H:%M:%S'),
                                                        get_field(x, EVENT_4s_broadcast, "FILTERABLE_FIELD_1"),
                                                        get_field(x, EVENT_4s_broadcast, "EVENT_4_value")
                                                        )
                                                       )
                                                   )
                                                   .filter(lambda (id_1, (r, i, c)):
                                                     (i == False 
                                                      and r >= dt.datetime.strptime(start_date, '%m/%d/%Y')
                                                      and r < dt.datetime.strptime(end_date, '%m/%d/%Y')
                                                      and c > 0
                                                      and id_1 != ''
                                                     )
                                                   )
                                )
    return filtered_EVENT_4s_rdd

# get max convesion time for each publisher user
def get_booker_with_max_EVENT_4_time_rdd(EVENT_4s_rdd):
    booker_with_max_EVENT_4_time_rdd = EVENT_4s_rdd.reduceByKey(lambda (r1, i1, c1), (r2, i2, c2):
                                                                         (
                                                                             max(r1, r2),
                                                                             False,
                                                                             1
                                                                         ) 
                                                                       )
    return booker_with_max_EVENT_4_time_rdd

# ### Function to left join visitors with bookers
# left join visitors and bookers, # filter so that EVENT_1 request time < EVENT_4 request time
def left_join(visitor_rdd, booker_rdd):
    bernie_rdd = visitor_rdd.leftOuterJoin(booker_rdd)
    result_rdd = bernie_rdd.filter(lambda (id_1, ((t1, r1),R)):
                                     (
                                       True if R == None else r1 < R[0]
                                     )
                                  )
    return result_rdd

# ### Function to calculate BRD
# calculate booker rate of each traffic share type
def calculate_BRD(bernie_rdd):
    # count visitors/booker for each traffic share type
    # key: (traffic share type, True if a user is a booker)
    answer = (bernie_rdd.map(lambda (id_1, ((t1, r1),R)):
                                 (
                                 (list(t1)[0], False if R == None else True), 1
                                 )
                            )
                            .reduceByKey(lambda a,b: a + b)
                            .collect()
              )
    answer = dict(answer)
    control_br = answer[('PUBLISHER', True)] * 1. / (answer[('PUBLISHER', True)] + answer[('PUBLISHER', False)])
    test_br = answer[('INTENT_MEDIA', True)] * 1. / (answer[('INTENT_MEDIA', True)] + answer[('INTENT_MEDIA', False)])
    brd = test_br/control_br - 1
    return answer, control_br, test_br, brd

# run the loop
def run_the_loop(year, month, day, X, endyear, endmonth, endday):
    #global EVENT_1s_rdd
    global EVENT_4s_rdd
    global visitors_rdd
    #global bookers_rdd
    global bernie_rdd
    global next_day_EVENT_1s_rdd
    global next_day_EVENT_4s_rdd
    global union_EVENT_1s_rdd
    global union_bookers_rdd
    global result_rdd
    global results
    global EVENT_1s_rdd_dict
    global bookers_rdd_dict
    
    end_date = dt.date(endyear, endmonth, endday)
    
    # 1. For the 1st day: 
    current_running_date = dt.date(year, month, day)
    rolling_end_date = dt.date(year, month, day) + dt.timedelta(days=1)
    rolling_start_date = dt.datetime(year, month, day, 0, 0, 0) + dt.timedelta(days=-X)
    print "start run for date", dt.date(year, month, day).strftime('%m/%d/%Y')
    
    # get X day EVENT_1s and EVENT_4 data
    print "get EVENT_1s and EVENT_4s of date [", rolling_start_date.strftime('%m/%d/%Y'), ",", rolling_end_date.strftime('%m/%d/%Y'), ")"
    EVENT_1s_rdd_dict[current_running_date.strftime('%m/%d/%Y')] = get_EVENT_1s_rdd(rolling_start_date.strftime('%m/%d/%Y'), rolling_end_date.strftime('%m/%d/%Y')).persist(MEMORY_AND_DISK)
    EVENT_4s_rdd = get_EVENT_4s_rdd(rolling_start_date.strftime('%m/%d/%Y'), rolling_end_date.strftime('%m/%d/%Y'))
    
    # find clean users with min EVENT_1 time, and bookers with max EVENT_4 time
    print "find visitors and bookers"
    visitors_rdd = get_clean_user_with_min_EVENT_1_time_rdd(EVENT_1s_rdd_dict[current_running_date.strftime('%m/%d/%Y')])
    bookers_rdd_dict[current_running_date.strftime('%m/%d/%Y')] = get_booker_with_max_EVENT_4_time_rdd(EVENT_4s_rdd).persist(MEMORY_AND_DISK)
    
    print "calculate BRD"
    bernie_rdd = left_join(visitors_rdd, bookers_rdd_dict[current_running_date.strftime('%m/%d/%Y')])
    answer, control_br, test_br, brd = calculate_BRD(bernie_rdd)
    results.append((dt.date(year, month, day).strftime('%m/%d/%Y'), control_br, test_br, brd))
    print "\n\n\n\n\n######### Result: \n\n", results
    print answer
    print "\n\n\n\n\n"
    
    # 2. Start the loop, go forward:
    print 
    print "start the loop..."
    while (rolling_end_date <= end_date):
        print
        print "start run for date", rolling_end_date.strftime('%m/%d/%Y')
        
        last_running_date = current_running_date
        current_running_date = current_running_date + dt.timedelta(days=1)
        prev_rolling_end_date = rolling_end_date
        rolling_end_date = rolling_end_date + dt.timedelta(days=1)
        rolling_start_date = rolling_start_date + dt.timedelta(days=1)
        
        #get the next day's EVENT_1s and EVENT_4s data
        print "get EVENT_1s and EVENT_4s of date [", prev_rolling_end_date.strftime('%m/%d/%Y'), ",", rolling_end_date.strftime('%m/%d/%Y'),")"
        next_day_EVENT_1s_rdd = get_EVENT_1s_rdd(prev_rolling_end_date.strftime('%m/%d/%Y'), rolling_end_date.strftime('%m/%d/%Y'))
        next_day_EVENT_4s_rdd = get_EVENT_4s_rdd(prev_rolling_end_date.strftime('%m/%d/%Y'), rolling_end_date.strftime('%m/%d/%Y'))
        
        # union next day's EVENT_1s data with the X days' data
        union_EVENT_1s_rdd = union_rdds(EVENT_1s_rdd_dict[last_running_date.strftime('%m/%d/%Y')], next_day_EVENT_1s_rdd)
        
        # remove earliest date from the data 
        print "remove date <=", rolling_start_date.strftime('%m/%d/%Y'), "from EVENT_1s"
        EVENT_1s_rdd_dict[current_running_date.strftime('%m/%d/%Y')] = remove_date_from_rdd(union_EVENT_1s_rdd, rolling_start_date).persist(MEMORY_AND_DISK)
        
        # union next day's EVENT_4 data with the bookers data
        union_bookers_rdd = union_rdds(bookers_rdd_dict[last_running_date.strftime('%m/%d/%Y')], next_day_EVENT_4s_rdd)
        
        print "find visitors and bookers"
        visitors_rdd = get_clean_user_with_min_EVENT_1_time_rdd(EVENT_1s_rdd_dict[current_running_date.strftime('%m/%d/%Y')])
        bookers_rdd_dict[current_running_date.strftime('%m/%d/%Y')] = get_booker_with_max_EVENT_4_time_rdd(union_bookers_rdd).persist(MEMORY_AND_DISK)
        
        print "calculate BRD"
        bernie_rdd = left_join(visitors_rdd, bookers_rdd_dict[current_running_date.strftime('%m/%d/%Y')])
        answer, control_br, test_br, brd = calculate_BRD(bernie_rdd)
        
        results.append((prev_rolling_end_date.strftime('%m/%d/%Y'), control_br, test_br, brd))
        print "\n\n\n\n\n######### Result: \n\n", results
        print answer
        print "\n\n\n\n\n"
        
        # uncache last day's rdd
        EVENT_1s_rdd_dict[last_running_date.strftime('%m/%d/%Y')].unpersist()
        bookers_rdd_dict[last_running_date.strftime('%m/%d/%Y')].unpersist()
        #print "\n\n\n\n\n***** rdd uncached\n\n\n\n\n"
        
        # convert the result to a rdd and save it to a txt file
        result_rdd = sc.parallelize(results)
        now = dt.datetime.now()
        filename = "s3://company-hawk-output/ppa_rolling_brd/ENTITY_1_list_page_rolling_30_day/" + now.strftime('%m_%d_%Y_%H_%M_%S')
        result_rdd.saveAsTextFile(filename)

run_the_loop(2015, 8, 29, 30, 2015, 9, 13)

