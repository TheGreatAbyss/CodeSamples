import os
import json as json
import datetime as dt
import ast
import pytz

def test_func():
    print "hello"

def load_EVENT_1(sc, start_date, end_date, sale_types = ['*'], products = ['*'], publishers = ['*']):


    try: 
        assert type(sale_types) == list
        assert type(products) == list
        assert type(publishers) == list
    except AssertionError:
        print "Please enter lists for sale_types, products, and publishers"
        raise


    start_date_obj = dt.datetime.strptime(start_date, '%m/%d/%Y')    
    end_date_obj = dt.datetime.strptime(end_date, '%m/%d/%Y')


    output = []

    while start_date_obj <= end_date_obj:
        start_year = start_date_obj.strftime('%Y')
        start_month = start_date_obj.strftime('%m')
        start_day = start_date_obj.strftime('%d')

        #0 = year
        #1 = month
        #2 = day
        #3 = sale type
        #4 = product
        #5 = publisher

        s3_file = ('s3://company-hadoop-production/input/EVENT_1/{0}{1}{2}/{3}/{4}/{5}/*'
                   .format(start_year, start_month, start_day, '{' + ','.join(sale_types) + '}', '{' + ','.join(products) + '}', 
                           '{' + ','.join(publishers) + '}')
                   )

        output.append(s3_file)
        start_date_obj = start_date_obj + dt.timedelta(days=1)

    print output
    outputRDD = sc.textFile(','.join(output))
    jsoned = outputRDD.map(lambda text: json.loads(text))

    return jsoned




def load_EVENT_2(sc, start_date, end_date, sale_types = ['*'], products = ['*'], publishers = ['*']):
    
    
        
    try: 
        assert type(sale_types) == list
        assert type(products) == list
        assert type(publishers) == list
    except AssertionError:
        print "Please enter lists for sale_types, products, and publishers"
        raise
    
    
    start_date_obj = dt.datetime.strptime(start_date, '%m/%d/%Y')    
    end_date_obj = dt.datetime.strptime(end_date, '%m/%d/%Y')
    

    output = []
    
    while start_date_obj <= end_date_obj:
        start_year = start_date_obj.strftime('%Y')
        start_month = start_date_obj.strftime('%m')
        start_day = start_date_obj.strftime('%d')
        
        #0 = year
        #1 = month
        #2 = day
        #3 = sale type
        #4 = product
        #5 = publisher

        s3_file = ('s3://company-hadoop-production/input/EVENT_2/{0}{1}{2}/{3}/{4}/{5}/*'
                   .format(start_year, start_month, start_day, '{' + ','.join(sale_types) + '}', '{' + ','.join(products) + '}', 
                           '{' + ','.join(publishers) + '}')
                   )
        
        output.append(s3_file)
        start_date_obj = start_date_obj + dt.timedelta(days=1)

    print output
    outputRDD = sc.textFile(','.join(output))
    jsoned = outputRDD.map(lambda text: json.loads(text))
    
    return jsoned




def load_EVENT_3(sc, start_date, end_date, sale_types = ['*'], publishers = ['*']):
    
    
        
    try: 
        assert type(sale_types) == list
        assert type(publishers) == list
    except AssertionError:
        print "Please enter lists for sale_types, and publishers"
        raise
    
    
    start_date_obj = dt.datetime.strptime(start_date, '%m/%d/%Y')    
    end_date_obj = dt.datetime.strptime(end_date, '%m/%d/%Y')
    

    output = []
    
    while start_date_obj <= end_date_obj:
        start_year = start_date_obj.strftime('%Y')
        start_month = start_date_obj.strftime('%m')
        start_day = start_date_obj.strftime('%d')
        
        #0 = year
        #1 = month
        #2 = day
        #3 = sale type
        #4 = product
        #5 = publisher

        s3_file = ('s3://company-hadoop-production/input/EVENT_3/{0}{1}{2}/{3}/{4}/{5}/*'
                   .format(start_year, start_month, start_day, '{' + ','.join(sale_types) + '}', 'NO_AD_TYPE', 
                           '{' + ','.join(publishers) + '}')
                   )
        
        output.append(s3_file)
        start_date_obj = start_date_obj + dt.timedelta(days=1)

    print output
    outputRDD = sc.textFile(','.join(output))
    jsoned = outputRDD.map(lambda text: json.loads(text))
    
    return jsoned


def load_EVENT_4(sc, start_date, end_date, sale_types = ['*'], publishers = ['*']):
    
    
        
    try: 
        assert type(sale_types) == list
        assert type(publishers) == list
    except AssertionError:
        print "Please enter lists for sale_types, and publishers"
        raise
    
    
    start_date_obj = dt.datetime.strptime(start_date, '%m/%d/%Y')    
    end_date_obj = dt.datetime.strptime(end_date, '%m/%d/%Y')
    

    output = []
    
    while start_date_obj <= end_date_obj:
        start_year = start_date_obj.strftime('%Y')
        start_month = start_date_obj.strftime('%m')
        start_day = start_date_obj.strftime('%d')
        
        #0 = year
        #1 = month
        #2 = day
        #3 = sale type
        #4 = product
        #5 = publisher

        s3_file = ('s3://company-hadoop-production/input/EVENT_4/{0}{1}{2}/{3}/{4}/{5}/*'
                   .format(start_year, start_month, start_day, '{' + ','.join(sale_types) + '}', 'NO_AD_TYPE', 
                           '{' + ','.join(publishers) + '}')
                   )
        
        output.append(s3_file)
        start_date_obj = start_date_obj + dt.timedelta(days=1)

    print output
    outputRDD = sc.textFile(','.join(output))
    jsoned = outputRDD.map(lambda text: json.loads(text))
    
    return jsoned



def load_EVENT_5(sc, start_date, end_date, sale_types = ['*'], publishers = ['*']):
    
    
        
    try: 
        assert type(sale_types) == list
        assert type(publishers) == list
    except AssertionError:
        print "Please enter lists for sale_types, and publishers"
        raise
    
    
    start_date_obj = dt.datetime.strptime(start_date, '%m/%d/%Y')    
    end_date_obj = dt.datetime.strptime(end_date, '%m/%d/%Y')
    

    output = []
    
    while start_date_obj <= end_date_obj:
        start_year = start_date_obj.strftime('%Y')
        start_month = start_date_obj.strftime('%m')
        start_day = start_date_obj.strftime('%d')
        
        #0 = year
        #1 = month
        #2 = day
        #3 = sale type
        #4 = product
        #5 = publisher

        s3_file = ('s3://company-hadoop-production/input/EVENT_5/{0}{1}{2}/{3}/{4}/{5}/*'
                   .format(start_year, start_month, start_day, '{' + ','.join(sale_types) + '}', 'NO_AD_TYPE', 
                           '{' + ','.join(publishers) + '}')
                   )
        
        output.append(s3_file)
        start_date_obj = start_date_obj + dt.timedelta(days=1)

    print output
    outputRDD = sc.textFile(','.join(output))
    jsoned = outputRDD.map(lambda text: json.loads(text))
    
    return jsoned


def get_field(json_dic, lookup_dic, field, keys = []):
    
    if len(keys) == 0:
        keys = lookup_dic.value[field]   
    
    try:
        result =  json_dic[keys[0]]
        if len(keys) > 1:
            print keys[1:]
            return get_field(result, {}, field, keys[1:])
        else:
            return result
            
            
    except KeyError:
        result = None
        
        
        
        
# Load Dictionaries
def load_EVENT_1_dictionary(sc):
    EVENT_1_dic = (sc.textFile('s3://company-hawk-output/spark_python/dictionaries/EVENT_1_dict.txt')
                   .collect())
    EVENT_1_dic_j = ast.literal_eval(''.join(EVENT_1_dic))
    
    return sc.broadcast(EVENT_1_dic_j)

def load_EVENT_2_dictionary(sc):
    EVENT_2_dic = (sc.textFile('s3://company-hawk-output/spark_python/dictionaries/EVENT_2_dict.txt')
                   .collect())
    EVENT_2_dic_j = ast.literal_eval(''.join(EVENT_2_dic))
    
    return sc.broadcast(EVENT_2_dic_j)


def load_EVENT_3_dictionary(sc):
    EVENT_3_dic = (sc.textFile('s3://company-hawk-output/spark_python/dictionaries/EVENT_3_dict.txt')
                   .collect())
    EVENT_3_dic_j = ast.literal_eval(''.join(EVENT_3_dic))
    
    return sc.broadcast(EVENT_3_dic_j)


def load_EVENT_4_dictionary(sc):
    EVENT_4_dic = (sc.textFile('s3://company-hawk-output/spark_python/dictionaries/EVENT_4_dict.txt')
                   .collect())
    EVENT_4_dic_j = ast.literal_eval(''.join(EVENT_4_dic))
    
    return sc.broadcast(EVENT_4_dic_j)


def load_EVENT_5_dictionary(sc):
    EVENT_5_dic = (sc.textFile('s3://company-hawk-output/spark_python/dictionaries/EVENT_5_dict.txt')
                   .collect())
    EVENT_5_dic_j = ast.literal_eval(''.join(EVENT_5_dic))
    
    return sc.broadcast(EVENT_5_dic_j)

def toEST_time(unix_timestamp):
    try:
        est_time = dt.datetime.fromtimestamp(int(str(unix_timestamp)[:10]),pytz.timezone('America/New_York')).strftime('%m-%d-%Y %H:%M:%S')
        return est_time
    except ValueError:
        return None    
    
    
def toEST_date(unix_timestamp):
    try:
        est_time = dt.datetime.fromtimestamp(int(str(unix_timestamp)[:10]),pytz.timezone('America/New_York')).strftime('%m-%d-%Y')
        return est_time
    except ValueError:
        return None    
