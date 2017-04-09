#  I wrote the below code while working as an analyst to create an aggregation table by pulling from log data.
#  This was always meant to be a temporary stop gap until it could be properly productionized, and is more of an example 
#  of Python/SQL scripting skills, then how I would build a production data pipeline.  A lot of this, especially the transposition steps 
#  would be more efficient using Spark, or piping out of Vertica into a Python application using Numpy nd-Arrays
#  The code was put in a Docker Container and pushed to Chronos.  

# Field and table names have been heavily modified.

import sys
import datetime as dt
import vertica_python as vp
import sys
user = raw_input("Enter Vertica UserName: ")
sys.path.append("/Users/" + user + "/Documents/")
import credential_func as cred

# import "/Users/eric.abis/Documents/credential_func.py" as cred


 # to_date('""" + strmonth + strday + stryear + """','mm/dd/yyyy')

def pull_from_log_data(stryear, strmonth, strday, currenttime):

    print "infunction_1"
    print strmonth + strday + stryear

    statement = """ 

        ------ Basic Data Pull from Ad Calls, Event_Type_3s, Event_Type_2s

        DELETE FROM  company_sandbox_production.EA_PRODUCT_1_competition_stage_1; COMMIT;
        SELECT add_vertica_options('EE', 'ENABLE_JOIN_SPILL');
        INSERT INTO  company_sandbox_production.EA_PRODUCT_1_competition_stage_1

        SELECT 
          CASE WHEN publisher_price - Min(price_of_something) OVER (Partition BY a.request_ID) <= 1 AND
                    publisher_price - MIN(price_of_something) OVER (Partition BY a.request_ID) >= -1 THEN 1 ELSE 0 END as Pub_tie,
                          
          CASE WHEN publisher_price - MIN(price_of_something) OVER (Partition BY a.request_ID) <= 1 AND
                    publisher_price - MAX(price_of_something) OVER (Partition BY a.request_ID) >= -1 THEN 1 ELSE 0 END AS pub_all_around_tie,                       
                          
          CASE WHEN publisher_price - Min(price_of_something) OVER (Partition BY a.request_ID) > 1 THEN 1 ELSE 0 END as Pub_loss,
          CASE WHEN publisher_price - Min(price_of_something) OVER (Partition BY a.request_ID) < -1 THEN 1 ELSE 0 END as Pub_win,
          a.*
        FROM
        (
          SELECT  
            a.request_id, 
            a.request_correlation_id, 
            a.Field_1, 
            Field_2, 
            Field_6, 
            Date_1, 
            Date_2, 
            transaction_attribute_1, 
            transaction_attribute_2, 
            transaction_attribute_3, 
            a.requested_at_in_et,
            a.Field_4,  
            a.requested_at_date_in_et, 
            Field_5, 
            a.site_currency, 
            'VALUE_1' as filter_cause_type, 
            i.base_bid as bid,
            count_of_something, 
            count_of_something_2, 
            site_reporting_value_06, 
            a.device_family, 
            floor,
            CASE WHEN a.some_order <= 5 THEN 'TOP 5' ELSE 'NOT TOP 5'END as some_order,
            CASE WHEN REGEXP_REPLACE(lower(country),'[\s\W]') IN ('unitedstates','unitedstatesofamerica','us','usa') THEN 'Domestic' 
                 WHEN country is null  THEN 'Unknown' 
                 ELSE 'International' END As Dom_Intl,
                      
            CAST(CASE WHEN is_some_rate = TRUE AND is_some_other_rate = FALSE THEN sub_entity_average_nightly_rate*Field_6/(Date_1 - Date_2)
                      WHEN is_some_rate = TRUE AND is_some_other_rate = TRUE THEN sub_entity_average_nightly_rate*Field_6
                      WHEN is_some_rate = FALSE AND is_some_other_rate = FALSE THEN sub_entity_average_nightly_rate/(Date_1 - Date_2)
                      WHEN is_some_rate = FALSE AND is_some_other_rate = TRUE THEN sub_entity_average_nightly_rate END as float) as price_of_something,
               
            CAST(CASE WHEN is_some_rate = TRUE AND is_some_other_rate = FALSE THEN publisher_sub_entity_price*Field_6/(Date_1 - Date_2)
                      WHEN is_some_rate = TRUE AND is_some_other_rate = TRUE THEN publisher_sub_entity_price*Field_6
                      WHEN is_some_rate = FALSE AND is_some_other_rate = FALSE THEN publisher_sub_entity_price/(Date_1 - Date_2) 
                      WHEN is_some_rate = FALSE AND is_some_other_rate = TRUE THEN publisher_sub_entity_price END as float) as publisher_price,     
               
            SUM(CASE WHEN c.fraudulent = TRUE THEN 0 ELSE ISNULL(c.actual_cpc,0) END) as revenue, 
            SUM(CASE WHEN c.fraudulent = FALSE and c.actual_cpc > 0 THEN 1 ELSE 0 END) as Event_Type_2s
          FROM 
            company_log_data_production.Event_Type_1s a
          LEFT OUTER JOIN 
            company_production.publishers_sub_entity_properties hp  
            ON a.publisher_sub_entity_property_id = hp.publisher_sub_entity_property_id 
            AND CASE a.Field_4 WHEN 106 THEN 45 ELSE a.Field_4 END = hp.Field_4
          INNER JOIN 
            company_log_data_production.Event_Type_3s i 
            on i.request_id = a.request_id
          LEFT OUTER JOIN 
            company_log_data_production.Event_Type_2s c 
            on i.external_id = c.external_Event_Type_3_id
          WHERE 
            ad_unit_type = 'PRODUCT_1' 
            AND a.Filterable_Field = false 
            AND outcome_type = 'VALUE_1' 
            AND i.Filterable_Field = false  
            AND a.requested_at_date_in_et = to_date('""" + strmonth + strday + stryear + """','mm/dd/yyyy')
            AND i.requested_at_date_in_et = to_date('""" + strmonth + strday + stryear + """','mm/dd/yyyy')
            AND   sub_entity_average_nightly_rate  > 0 
            --AND publisher_sub_entity_price > 0 IF turned on will filter out some_company
          GROUP BY 
               a.request_correlation_id, a.request_id, a.Field_1, a.Field_4,
               a.requested_at_date_in_et, a.requested_at_in_et, Field_5, a.site_currency, Field_2, count_of_something_2,
               CASE WHEN REGEXP_REPLACE(lower(country),'[\s\W]') IN ('unitedstates','unitedstatesofamerica','us','usa') THEN 'Domestic' 
                    WHEN country is null  THEN 'Unknown' 
                    ELSE 'International' END ,
               CASE WHEN a.some_order <= 5 THEN 'TOP 5' ELSE 'NOT TOP 5'END, 
               CAST(CASE WHEN is_some_rate = TRUE AND is_some_other_rate = FALSE THEN sub_entity_average_nightly_rate*Field_6/(Date_1 - Date_2)
               WHEN is_some_rate = TRUE AND is_some_other_rate = TRUE THEN sub_entity_average_nightly_rate*Field_6
               WHEN is_some_rate = FALSE AND is_some_other_rate = FALSE THEN sub_entity_average_nightly_rate/(Date_1 - Date_2)
               WHEN is_some_rate = FALSE AND is_some_other_rate = TRUE THEN sub_entity_average_nightly_rate END as float),
               CAST(CASE WHEN is_some_rate = TRUE AND is_some_other_rate = FALSE THEN publisher_sub_entity_price*Field_6/(Date_1 - Date_2)
               WHEN is_some_rate = TRUE AND is_some_other_rate = TRUE THEN publisher_sub_entity_price*Field_6
               WHEN is_some_rate = FALSE AND is_some_other_rate = FALSE THEN publisher_sub_entity_price/(Date_1 - Date_2)
               WHEN is_some_rate = FALSE AND is_some_other_rate = TRUE THEN publisher_sub_entity_price END as float),  
                  i.base_bid, count_of_something, site_reporting_value_06
                  , Field_6, Date_1, Date_2, transaction_attribute_1, transaction_attribute_2, transaction_attribute_3, a.requested_at_in_et, a.device_family, floor
        ) a
                  
        UNION ALL
          
        SELECT 
          0 as Pub_tie, 
          0 as pub_all_around_Tie, 
          0 as pub_loss, 
          0 as pub_win,  
          a.request_id,  
          a.request_correlation_id,
          a.Field_1, 
          0 as Field_2,
          Field_6, 
          Date_1, 
          Date_2, 
          transaction_attribute_1, 
          transaction_attribute_2, 
          transaction_attribute_3,  
          a.requested_at_in_et,
          a.Field_4, 
          a.requested_at_date_in_et, 
          Field_5, 
          a.site_currency, 
          filter_cause_type, 
          i.base_bid as bid, 
          count_of_something, 
          count_of_something_2, 
          site_reporting_value_06, 
          a.device_family, 
          floor,
          CASE WHEN a.some_order <= 5 THEN 'TOP 5' ELSE 'NOT TOP 5'END as some_order,
          CASE WHEN REGEXP_REPLACE(lower(country),'[\s\W]') IN ('unitedstates','unitedstatesofamerica','us','usa') THEN 'Domestic' 
               WHEN country is null  THEN 'Unknown' 
               ELSE 'International' END As Dom_Intl,
                      
          CAST(CASE WHEN is_some_rate = TRUE AND is_some_other_rate = FALSE THEN sub_entity_average_nightly_rate*Field_6/(Date_1 - Date_2)
                    WHEN is_some_rate = TRUE AND is_some_other_rate = TRUE THEN sub_entity_average_nightly_rate*Field_6
                    WHEN is_some_rate = FALSE AND is_some_other_rate = FALSE THEN sub_entity_average_nightly_rate/(Date_1 - Date_2)
                    WHEN is_some_rate = FALSE AND is_some_other_rate = TRUE THEN sub_entity_average_nightly_rate END as float) as price_of_something,
               
          CAST(CASE WHEN is_some_rate = TRUE AND is_some_other_rate = FALSE THEN publisher_sub_entity_price*Field_6/(Date_1 - Date_2)
                    WHEN is_some_rate = TRUE AND is_some_other_rate = TRUE THEN publisher_sub_entity_price*Field_6
                    WHEN is_some_rate = FALSE AND is_some_other_rate = FALSE THEN publisher_sub_entity_price/(Date_1 - Date_2) 
                    WHEN is_some_rate = FALSE AND is_some_other_rate = TRUE THEN publisher_sub_entity_price END as float) as publisher_price,      
               
          0 as revenue, 
          0 as Event_Type_2s
        FROM 
          company_log_data_production.Event_Type_1s a
        LEFT OUTER JOIN 
          company_production.publishers_sub_entity_properties hp  
          ON a.publisher_sub_entity_property_id = hp.publisher_sub_entity_property_id   
          AND  CASE a.Field_4 WHEN 106 THEN 45 ELSE a.Field_4 END = hp.Field_4
        INNER JOIN 
          company_log_data_production.filtered_advertisements i 
          ON i.request_id = a.request_id
        WHERE 
          ad_unit_type = 'PRODUCT_1' 
          AND a.Filterable_Field = false 
          AND outcome_type = 'VALUE_1' 
          AND i.Filterable_Field = false  
          AND a.requested_at_date_in_et = to_date('""" + strmonth + strday + stryear + """','mm/dd/yyyy')
          AND i.requested_at_date_in_et = to_date('""" + strmonth + strday + stryear + """','mm/dd/yyyy'); COMMIT

          ; SELECT clr_vertica_options('EE', 'ENABLE_JOIN_SPILL')

         ; SELECT analyze_histogram('company_sandbox_production.EA_PRODUCT_1_competition_stage_1',100)    

  
                            """
    print statement
    conn = cred.create_connection()
    cur = conn.cursor()
    cur.execute(statement)
    conn.close()

       
def win_loss_tie(stryear, strmonth, strday, currenttime):

    print "infunction_2"
    print strmonth + strday + stryear

    statement = """ 


      ------ The second inner query that is unioned is just pulling the publisher data, and adding it as a row as if it were another entity.  The is_publisher field distinguishes between entitys and Publishers
      ------ The Outer query determines for each request_id the winning_price, whether each price is a win or a tie, or a loss, and then simply creates an order rank for each row
      ------ A win is defined as a price beating all other prices by more then $1
      ------ Since I'm only able to pull the minimum price, for each row I can only determine if the prices is within a dollar of that price so also a win, but I don't know if other prices are also within a dollar
      ------ so I call it a win_or_tie, and will figure out which later

      DELETE FROM  company_sandbox_production.EA_PRODUCT_1_competition_stage_2_ID; COMMIT;
      INSERT INTO company_sandbox_production.EA_PRODUCT_1_competition_stage_2_ID

      SELECT 
        pub_win, 
        pub_tie, 
        pub_loss, 
        filter_cause_type, 
        Field_2, 
        request_ID,  
        a.Field_4,                 
        a.Field_1,  
        a.requested_at_date_in_et, 
        requested_at_in_et, 
        a.site_currency,  
        a.some_order, 
        revenue, 
        Event_Type_2s, 
        bid, 
        a.Field_5, 
        Dom_Intl,
        price_of_something,  
        is_publisher, 
        count_of_something, 
        count_of_something_2,               
        Field_6, 
        Date_1, 
        Date_2, 
        transaction_attribute_1, 
        transaction_attribute_2, 
        transaction_attribute_3,     
        Min(price_of_something) OVER (Partition BY request_ID, filter_cause_type) as winning_Price, 
        CASE WHEN price_of_something -  Min(price_of_something) OVER (Partition BY request_ID, filter_cause_type) < 1 Then 1 ELSE 0 END as win_or_tie,
        CASE WHEN price_of_something -  Min(price_of_something) OVER (Partition BY request_ID, filter_cause_type) < 1 Then 0 ELSE 1 END as loss,
        RANK() OVER (PARTITION BY request_ID, filter_cause_type ORDER BY filter_cause_type, Field_5) as torder,
        site_reporting_value_06, 
        a.request_correlation_id , 
        device_family, 
        pub_all_around_tie, 
        floor
      FROM 
      (
        --------entitys
        SELECT 
          pub_win, 
          pub_tie, 
          pub_all_around_tie, 
          pub_loss, 
          filter_cause_type, 
          Field_2, 
          request_ID,  
          a.Field_4,                 
          a.Field_1,  
          a.requested_at_date_in_et, 
          requested_at_in_et, 
          a.site_currency,  
          a.some_order, 
          revenue, 
          Event_Type_2s, 
          bid, 
          a.Field_5, 
          Dom_Intl,
          price_of_something, 
          0 as is_publisher, 
          count_of_something, 
          count_of_something_2,               
          Field_6, 
          Date_1, 
          Date_2, 
          transaction_attribute_1, 
          transaction_attribute_2, 
          transaction_attribute_3, 
          site_reporting_value_06, 
          a.request_correlation_id, 
          device_family, 
          floor                                      
        FROM 
          company_sandbox_production.EA_PRODUCT_1_competition_stage_1 a
        WHERE 
          requested_at_date_in_et = to_date('""" + strmonth + strday + stryear + """','mm/dd/yyyy')
              
        UNION ALL
              
        --Publishers
        SELECT DISTINCT 
          0 as pub_win, 
          0 as pub_tie, 
          0 as pub_all_around_tie,  
          0 as pub_loss, 
          'VALUE_1' as filter_cause_type, 
          0 as Field_2, 
          request_ID,  
          a.Field_4, 
          a.Field_1, 
          a.requested_at_date_in_et, 
          requested_at_in_et, 
          a.site_currency, 
          a.some_order, 
          0 as revenue, 
          0 as Event_Type_2s, 
          0 as bid, 
          a.Field_1, 
          Dom_Intl,
          publisher_price as price_of_something, 
          1 as is_publisher, 
          count_of_something, 
          count_of_something_2,
          Field_6, 
          Date_1, 
          Date_2, 
          transaction_attribute_1, 
          transaction_attribute_2, 
          transaction_attribute_3,  
          site_reporting_value_06, 
          a.request_correlation_id, 
          device_family, 
          floor        
        FROM 
          company_sandbox_production.EA_PRODUCT_1_competition_stage_1 a
        WHERE 
          filter_cause_type = 'VALUE_1'
          AND requested_at_date_in_et = to_date('""" + strmonth + strday + stryear + """','mm/dd/yyyy')
        ) a ;  COMMIT

        ; SELECT analyze_histogram('company_sandbox_production.EA_PRODUCT_1_competition_stage_2_ID',100) 


                            """
    print statement
    conn = cred.create_connection()
    cur = conn.cursor()
    cur.execute(statement)
    conn.close()



def win_loss_tie_entitys(stryear, strmonth, strday, currenttime):

    print "infunction_2"
    print strmonth + strday + stryear

    statement = """ 

      ------ The Outer query determines for each request_id the winning_price, whether each price is a win or a tie, or a loss, and then simply creates an order rank for each row
      ------ A win is defined as a price beating all other prices by more then $1
      ------ Since I'm only able to pull the minimum price, for each row I can only determine if the prices is within a dollar of that price so also a win, but I don't know if other prices are also within a dollar
      ------ so I call it a win_or_tie, and will figure out which later

      DELETE FROM company_sandbox_production.EA_PRODUCT_1_competition_entity_stage_2_ID; COMMIT;
      INSERT INTO company_sandbox_production.EA_PRODUCT_1_competition_entity_stage_2_ID
        
      SELECT 
        a.*, 
        Min(price_of_something) OVER (Partition BY request_ID, filter_cause_type) as winning_Price, 
        CASE WHEN price_of_something -  Min(price_of_something) OVER (Partition BY request_ID, filter_cause_type) < 1 Then 1 ELSE 0 END as win_or_tie,
        CASE WHEN price_of_something -  Min(price_of_something) OVER (Partition BY request_ID, filter_cause_type) < 1 Then 0 ELSE 1 END as loss,
        RANK() OVER (PARTITION BY request_ID, filter_cause_type ORDER BY filter_cause_type, Field_5) as torder
      FROM 
      (
        --------entitys
        SELECT 
          pub_tie, 
          filter_cause_type, 
          Field_2, 
          request_ID,  
          a.Field_4,  
          a.Field_1, 
          a.requested_at_date_in_et, 
          a.site_currency,  
          a.some_order, 
          revenue, 
          Event_Type_2s, 
          bid, 
          a.Field_5, 
          Dom_Intl, 
          price_of_something, 
          0 as is_publisher, 
          count_of_something, 
          count_of_something_2               
        FROM 
          company_sandbox_production.EA_PRODUCT_1_competition_stage_1 a
      ) a ; COMMIT

      ; SELECT analyze_histogram('company_sandbox_production.EA_PRODUCT_1_competition_entity_stage_2_ID',100) ;
           
      DELETE FROM company_sandbox_production.EA_PRODUCT_1_competition_stage_1; COMMIT

                            """
    print statement
    conn = cred.create_connection()
    cur = conn.cursor()
    cur.execute(statement)
    conn.close()

def create_5_versions(stryear, strmonth, strday, version, currenttime):

    print "infunction_3"
    print strmonth + strday + stryear

    statement = """ 

      ------ Create 5 sets of the same data with each Entity occupying the first slot once
      ------ Each request_Id can have up to 5 Entities (including the publisher).  In the final results we want to see how each of these 5 entities relates to the other entities so we need 
      ------ to create the same data set 5 times in relation to each entity
      ------ Each union below pulls the same data below but for each of the 5 entities it sets torder = 1 and the others 2-5.  
      ------ It then adds a PRODUCT_1_rank to keep track of which of the 5 sets of data the row belongs to
      ------ The outer query simply joins in the runner_up price as determined from sub query b.  The runner up price is the lowest price of the loosing prices

      DELETE FROM company_sandbox_production.EA_PRODUCT_1_competition_""" + version + """stage_3_ID; COMMIT;
      INSERT INTO company_sandbox_production.EA_PRODUCT_1_competition_""" + version + """stage_3_ID

      SELECT b.*, ru.runner_up
      FROM
      (SELECT request_ID, Field_4,  Field_1, requested_at_date_in_et, site_currency, some_order, 
       revenue, Event_Type_2s, Field_5, Dom_Intl, price_of_something, is_publisher, winning_price, win_or_tie, loss,
       torder, 1 as PRODUCT_1_rank FROM company_sandbox_production.EA_PRODUCT_1_competition_""" + version + """stage_2_ID a WHERE filter_cause_type = 'VALUE_1'

      UNION ALL

      SELECT request_ID, Field_4,  Field_1, requested_at_date_in_et, site_currency, some_order, 
       revenue, Event_Type_2s, Field_5, Dom_Intl, price_of_something, is_publisher, winning_price, win_or_tie, loss,
      CASE  
      WHEN torder = 2 THEN 1
      WHEN torder < 2 THEN torder + 1
      ELSE torder END as torder, 2 as PRODUCT_1_rank
      FROM company_sandbox_production.EA_PRODUCT_1_competition_""" + version + """stage_2_ID
      WHERE filter_cause_type = 'VALUE_1' AND requested_at_date_in_et = '""" + strmonth + strday + stryear + """'

      UNION ALL

      SELECT request_ID, Field_4,  Field_1, requested_at_date_in_et, site_currency, some_order, 
       revenue, Event_Type_2s, Field_5, Dom_Intl, price_of_something, is_publisher, winning_price, win_or_tie, loss,
      CASE  
      WHEN torder = 3 THEN 1
      WHEN torder < 3 THEN torder + 1
      ELSE torder END as torder, 3 as PRODUCT_1_rank
      FROM company_sandbox_production.EA_PRODUCT_1_competition_""" + version + """stage_2_ID
      WHERE filter_cause_type = 'VALUE_1' AND requested_at_date_in_et = '""" + strmonth + strday + stryear + """'

      UNION ALL

      SELECT request_ID, Field_4,  Field_1, requested_at_date_in_et, site_currency, some_order, 
       revenue, Event_Type_2s, Field_5, Dom_Intl, price_of_something, is_publisher, winning_price, win_or_tie, loss,
      CASE  
      WHEN torder = 4 THEN 1
      WHEN torder < 4 THEN torder + 1
      ELSE torder END as torder, 4 as PRODUCT_1_rank
      FROM company_sandbox_production.EA_PRODUCT_1_competition_""" + version + """stage_2_ID
      WHERE filter_cause_type = 'VALUE_1' AND requested_at_date_in_et = '""" + strmonth + strday + stryear + """'

      UNION ALL

      SELECT request_ID, Field_4,  Field_1, requested_at_date_in_et, site_currency, some_order, 
       revenue, Event_Type_2s, Field_5, Dom_Intl, price_of_something, is_publisher, winning_price, win_or_tie, loss,
      CASE  
      WHEN torder = 5 THEN 1
      WHEN torder < 5 THEN torder + 1
      ELSE torder END as torder, 5 as PRODUCT_1_rank
      FROM company_sandbox_production.EA_PRODUCT_1_competition_""" + version + """stage_2_ID
      WHERE filter_cause_type = 'VALUE_1'  AND requested_at_date_in_et = '""" + strmonth + strday + stryear + """') b
      LEFT OUTER JOIN 
              (SELECT request_id, min(price_of_something) as runner_up
              FROM company_sandbox_production.EA_PRODUCT_1_competition_""" + version + """stage_2_ID
              Where loss = 1 AND  filter_cause_type = 'VALUE_1' AND requested_at_date_in_et = '""" + strmonth + strday + stryear + """'
              GROUP BY request_ID) ru
      on ru.request_ID = b.request_ID ; COMMIT

           ; SELECT analyze_histogram('company_sandbox_production.EA_PRODUCT_1_competition_""" + version + """stage_3_ID',100) ;


                            """
    print statement
    conn = cred.create_connection()
    cur = conn.cursor()
    cur.execute(statement)
    conn.close()


def transpose_rows_for_each_version(stryear, strmonth, strday, version, currenttime):

    print "infunction_4"
    print strmonth + strday + stryear

    statement = """ 

      ------ Transpose each set into a single line
      ------ For each of the 5 sets of data created above, we want to transpose the rows into a single line for analysis reasons
      ------ query 1 starts with the first row for each of the 5 sets of data (torder = 1) 
      ------ Each subsequent query left joins on the additional rows within each of the five sets as determined by the PRODUCT_1_rank above.
      ------ The outer query simply cleans it all up and assigns numers to each of the 5 players

      ------------ Transpose each set into a single line

      DELETE FROM company_sandbox_production.EA_PRODUCT_1_competition_""" + version + """stage_4_ID; COMMIT;
      INSERT INTO company_sandbox_production.EA_PRODUCT_1_competition_""" + version + """stage_4_ID

      SELECT  a.request_ID, Field_4, Field_1, requested_at_date_in_et, site_currency,  some_order,  
              Dom_Intl, is_publisher_1, Field_5_1, price_of_something_1, entity_1_revenue, entity_1_Event_Type_2s, is_publisher_2, Field_5_2, 
              price_of_something_2, entity_2_revenue, entity_2_Event_Type_2s, is_publisher_3, Field_5_3, price_of_something_3, entity_3_revenue, entity_3_Event_Type_2s, is_publisher_4, 
              Field_5_4, price_of_something_4, entity_4_revenue, entity_4_Event_Type_2s, is_publisher_5, Field_5_5, price_of_something_5, entity_5_revenue, entity_5_Event_Type_2s,
              a.winning_price, a.runner_up, win_or_tie_1, isnull(win_or_tie_2,0) + isnull(win_or_tie_3,0) + isnull(win_or_tie_4,0) + isnull(win_or_tie_5,0) as other_win_or_tie, loss_1
      FROM 
              
              (SELECT request_ID,  Field_4, Field_1, requested_at_date_in_et, site_currency,  some_order, PRODUCT_1_rank, Dom_Intl, 
              is_publisher as is_publisher_1,  Field_5 as Field_5_1, price_of_something as price_of_something_1, revenue as entity_1_revenue, Event_Type_2s as entity_1_Event_Type_2s,
              win_or_tie as win_or_tie_1, loss as loss_1, winning_price, runner_up
              FROM company_sandbox_production.EA_PRODUCT_1_competition_""" + version + """stage_3_ID
              WHERE torder = 1 ) a
              
              LEFT OUTER JOIN
              
              (SELECT request_ID, PRODUCT_1_rank, 
              is_publisher as is_publisher_2, Field_5 as Field_5_2, price_of_something as price_of_something_2, revenue as entity_2_revenue, Event_Type_2s as entity_2_Event_Type_2s,
              win_or_tie as win_or_tie_2, loss as loss_2
              FROM company_sandbox_production.EA_PRODUCT_1_competition_""" + version + """stage_3_ID
              WHERE torder = 2 ) b
              
              ON a.request_ID = b.request_ID AND a.PRODUCT_1_rank = b.PRODUCT_1_rank
              
              LEFT OUTER JOIN
              
              (SELECT request_ID, PRODUCT_1_rank, 
              is_publisher as is_publisher_3, Field_5 as Field_5_3, price_of_something as price_of_something_3, revenue as entity_3_revenue, Event_Type_2s as entity_3_Event_Type_2s,
              win_or_tie as win_or_tie_3, loss as loss_3
              FROM company_sandbox_production.EA_PRODUCT_1_competition_""" + version + """stage_3_ID
              WHERE torder = 3 ) c
              
              ON a.request_ID = c.request_ID AND a.PRODUCT_1_rank = c.PRODUCT_1_rank

              LEFT OUTER JOIN
              
              (SELECT request_ID, PRODUCT_1_rank, 
              is_publisher as is_publisher_4,  Field_5 as Field_5_4, price_of_something as price_of_something_4, revenue as entity_4_revenue, Event_Type_2s as entity_4_Event_Type_2s,
              win_or_tie as win_or_tie_4, loss as loss_4
              FROM company_sandbox_production.EA_PRODUCT_1_competition_""" + version + """stage_3_ID
              WHERE torder = 4 ) d
              
              ON a.request_ID = d.request_ID AND a.PRODUCT_1_rank = d.PRODUCT_1_rank

              LEFT OUTER JOIN
              
              (SELECT request_ID, PRODUCT_1_rank, 
              is_publisher as is_publisher_5, Field_5 as Field_5_5, price_of_something as price_of_something_5, revenue as entity_5_revenue, Event_Type_2s as entity_5_Event_Type_2s,
              win_or_tie as win_or_tie_5, loss as loss_5
              FROM company_sandbox_production.EA_PRODUCT_1_competition_""" + version + """stage_3_ID
              WHERE torder = 5 ) e
              
              ON a.request_ID = e.request_ID AND a.PRODUCT_1_rank = e.PRODUCT_1_rank; COMMIT

           ; SELECT analyze_histogram('company_sandbox_production.EA_PRODUCT_1_competition_""" + version + """stage_4_ID',100) ;

             DELETE FROM company_sandbox_production.EA_PRODUCT_1_competition_""" + version + """stage_3_ID; COMMIT

                            """

    print statement
    conn = cred.create_connection()
    cur = conn.cursor()
    cur.execute(statement)
    conn.close()


def insert_into_competition_report(stryear, strmonth, strday, version, currenttime):

    print "infunction_5"
    print strmonth + strday + stryear

    statement = """ 


      ------ Compute all metrics necesarry for final analysis
      ------ Sum up results to reduce rows
      ------ Data is grouped by Overall Wins, Losses and Ties.  There are no rows were two of these is greater then 0
      ------ Within Overall Wins, Losses and Ties however, a row can have multiple outcomes vs each of the different entitys.  Example, a row could be an overall loss but still win vs other players. A row cannot be an overall Win or Tie, and have a loss to another player.
      ------ Data is also grouped by the combination of wins, losses, and ties against other entitys
      ------ For entitys, Data is also grouped by whether they beat the publisher or not.


      DELETE FROM company_sandbox_production.EA_PRODUCT_1_competition_""" + version + """report_ID
      WHERE requested_at_date_in_et in  (SELECT DISTINCT requested_at_date_in_et FROM company_sandbox_production.EA_PRODUCT_1_competition_""" + version + """stage_4_ID); COMMIT;

      INSERT INTO company_sandbox_production.EA_PRODUCT_1_competition_""" + version + """report_ID
      
      SELECT 
              Field_4, Field_1, requested_at_date_in_et, site_currency, some_order, 
              Dom_Intl, is_publisher_1, Field_5_1, is_publisher_2, Field_5_2, 
              is_publisher_3, Field_5_3, is_publisher_4, Field_5_4,  is_publisher_5, Field_5_5,
              
              COUNT(DISTINCT request_ID) as Event_Type_1s,
              
              SUM(CASE WHEN win_or_tie_1 = 1 AND  other_win_or_tie = 0 THEN 1 ELSE 0 END) as WIN,
              SUM(CASE WHEN win_or_tie_1 = 1 AND  other_win_or_tie > 0 THEN 1 ELSE 0 END) as TIE,                                            
              SUM(Loss_1) as LOSS,
              
              CASE WHEN is_publisher_2 = 1 AND price_of_something_1 - price_of_something_2 <= -1 THEN 1
                   WHEN is_publisher_3 = 1 AND price_of_something_1 - price_of_something_3 <= -1 THEN 1
                   WHEN is_publisher_4 = 1 AND price_of_something_1 - price_of_something_4 <= -1 THEN 1
                   WHEN is_publisher_5 = 1 AND price_of_something_1 - price_of_something_5 <= -1 THEN 1
                   ELSE 0 END as Beats_Publisher,
                     
              SUM(CASE WHEN price_of_something_1 - isnull(price_of_something_2,99999999) >= 1 AND  price_of_something_1 - isnull(price_of_something_3,99999999) >= 1 AND  price_of_something_1 - isnull(price_of_something_4,99999999) >= 1  AND  price_of_something_1 - isnull(price_of_something_5,99999999) >= 1 Then 1 ELSE 0 END) AS LOSE_ALL,

              case WHEN price_of_something_1 - price_of_something_2 <= -1 Then 1 ELSE 0 END +
              case WHEN price_of_something_1 - price_of_something_3 <= -1 Then 1 ELSE 0 END +
              case WHEN price_of_something_1 - price_of_something_4 <= -1 Then 1 ELSE 0 END +
              case WHEN price_of_something_1 - price_of_something_5 <= -1 Then 1 ELSE 0 END as num_of_wins,
                      
              case WHEN price_of_something_1 - price_of_something_2 < 1 AND price_of_something_1 - price_of_something_2 > -1 Then 1 ELSE 0 END +
              case WHEN price_of_something_1 - price_of_something_3 < 1 AND price_of_something_1 - price_of_something_3 > -1 Then 1 ELSE 0 END +       
              case WHEN price_of_something_1 - price_of_something_4 < 1 AND price_of_something_1 - price_of_something_4 > -1 Then 1 ELSE 0 END +
              case WHEN price_of_something_1 - price_of_something_5 < 1 AND price_of_something_1 - price_of_something_5 > -1 Then 1 ELSE 0 END as num_of_ties,

              
              case WHEN price_of_something_1 - price_of_something_2 >= 1 Then 1 ELSE 0 END +
              case WHEN price_of_something_1 - price_of_something_3 >= 1 Then 1 ELSE 0 END +
              case WHEN price_of_something_1 - price_of_something_4 >= 1 Then 1 ELSE 0 END +
              case WHEN price_of_something_1 - price_of_something_5 >= 1 Then 1 ELSE 0 END as num_of_losses,

              SUM(entity_1_revenue) as entity_1_revenue,
              SUM(entity_2_revenue) as entity_2_revenue, 
              SUM(entity_3_revenue) as entity_3_revenue,
              SUM(entity_4_revenue) as entity_4_revenue,
              SUM(entity_5_revenue) as entity_5_revenue,
                                             
              SUM(entity_1_Event_Type_2s) as entity_1_Event_Type_2s,
              SUM(entity_2_Event_Type_2s) as entity_2_Event_Type_2s,
              SUM(entity_3_Event_Type_2s) as entity_3_Event_Type_2s,
              SUM(entity_4_Event_Type_2s) as entity_4_Event_Type_2s,
              SUM(entity_5_Event_Type_2s) as entity_5_Event_Type_2s, 

              ---- PCT Difference to winning price of loss
              SUM(CASE WHEN Loss_1 = 1 THEN  (price_of_something_1 - winning_price)/winning_price ELSE 0 END) as SUM_LOSS_PRCT_Differences,
              
              ---- PCT Difference to next highest price of win -- use Runner_up_price
              SUM(CASE WHEN win_or_tie_1 = 1 AND  other_win_or_tie = 0 THEN  (price_of_something_1 - isnull(runner_up, price_of_something_1))/runner_up ELSE 0 END) as SUM_WIN_PRCT_Differences,
                                             
              SUM((price_of_something_1 - price_of_something_2)/price_of_something_2) as SUM_PRCT_Differences_2,
              SUM((price_of_something_1 - price_of_something_3)/price_of_something_3) as SUM_PRCT_Differences_3,
              SUM((price_of_something_1 - price_of_something_4)/price_of_something_4) as SUM_PRCT_Differences_4,
              SUM((price_of_something_1 - price_of_something_5)/price_of_something_5) as SUM_PRCT_Differences_5,

              ---- PCT Differences over 50pct are used to show how often something goes really wrong
              SUM(CASE WHEN  (price_of_something_1 - price_of_something_2)/price_of_something_2 > .5 THEN 1 ELSE 0 END)  +
              SUM(CASE WHEN  (price_of_something_1 - price_of_something_3)/price_of_something_3 > .5 THEN 1 ELSE 0 END)  +
              SUM(CASE WHEN  (price_of_something_1 - price_of_something_4)/price_of_something_4 > .5 THEN 1 ELSE 0 END)  +
              SUM(CASE WHEN  (price_of_something_1 - price_of_something_5)/price_of_something_5 > .5 THEN 1 ELSE 0 END)  as PRCT_Differences_Over_50,
                      
              SUM(CASE WHEN  (price_of_something_1 - price_of_something_2)/price_of_something_2 < -.5 THEN 1 ELSE 0 END) +
              SUM(CASE WHEN  (price_of_something_1 - price_of_something_3)/price_of_something_3 < -.5 THEN 1 ELSE 0 END) +
              SUM(CASE WHEN  (price_of_something_1 - price_of_something_4)/price_of_something_4 < -.5 THEN 1 ELSE 0 END) +
              SUM(CASE WHEN  (price_of_something_1 - price_of_something_5)/price_of_something_5 < -.5 THEN 1 ELSE 0 END)  as PRCT_Differences_Under_neg_50,
       
              ---- Event_Type_2s of PCT Differences over 50%  - Used with Publisher
              SUM(CASE WHEN  (price_of_something_1 - price_of_something_2)/price_of_something_2 > .5 THEN entity_2_Event_Type_2s ELSE 0 END)  +
              SUM(CASE WHEN  (price_of_something_1 - price_of_something_3)/price_of_something_3 > .5 THEN entity_3_Event_Type_2s ELSE 0 END)  +
              SUM(CASE WHEN  (price_of_something_1 - price_of_something_4)/price_of_something_4 > .5 THEN entity_4_Event_Type_2s ELSE 0 END)  +
              SUM(CASE WHEN  (price_of_something_1 - price_of_something_5)/price_of_something_5 > .5 THEN entity_5_Event_Type_2s ELSE 0 END)  as Event_Type_2s_PRCT_Differences_Over_50,
                      
              SUM(CASE WHEN  (price_of_something_1 - price_of_something_2)/price_of_something_2 < -.5 THEN entity_2_Event_Type_2s ELSE 0 END) +
              SUM(CASE WHEN  (price_of_something_1 - price_of_something_3)/price_of_something_3 < -.5 THEN entity_3_Event_Type_2s ELSE 0 END) +
              SUM(CASE WHEN  (price_of_something_1 - price_of_something_4)/price_of_something_4 < -.5 THEN entity_4_Event_Type_2s ELSE 0 END) +
              SUM(CASE WHEN  (price_of_something_1 - price_of_something_5)/price_of_something_5 < -.5 THEN entity_5_Event_Type_2s ELSE 0 END)  as Event_Type_2s_PRCT_Differences_Under_neg_50,        

              ----- Event_Type_2s that are more then 50pct price differences from runner_up - Used with entity WINS
              SUM(CASE WHEN  (price_of_something_1 - runner_up)/runner_up < -.5 THEN entity_1_Event_Type_2s ELSE 0 END)  as entity_1_Event_Type_2s_50_runner_up,
              
              SUM(case WHEN price_of_something_1 - price_of_something_2 <= -1 Then 1 ELSE 0 END) as WINS_1_2,
              SUM(case WHEN price_of_something_1 - price_of_something_3 <= -1 Then 1 ELSE 0 END) as WINS_1_3,
              SUM(case WHEN price_of_something_1 - price_of_something_4 <= -1 Then 1 ELSE 0 END) as WINS_1_4 ,       
              SUM(case WHEN price_of_something_1 - price_of_something_5 <= -1 Then 1 ELSE 0 END) as WINS_1_5,
                      
              SUM(case WHEN price_of_something_1 - price_of_something_2 < 1 AND price_of_something_1 - price_of_something_2 > -1 Then 1 ELSE 0 END) as TIES_1_2,
              SUM(case WHEN price_of_something_1 - price_of_something_3 < 1 AND price_of_something_1 - price_of_something_3 > -1 Then 1 ELSE 0 END) as TIES_1_3,        
              SUM(case WHEN price_of_something_1 - price_of_something_4 < 1 AND price_of_something_1 - price_of_something_4 > -1 Then 1 ELSE 0 END) as TIES_1_4,
              SUM(case WHEN price_of_something_1 - price_of_something_5 < 1 AND price_of_something_1 - price_of_something_5 > -1 Then 1 ELSE 0 END) as TIES_1_5,

              SUM(case WHEN price_of_something_1 - price_of_something_2 >= 1 Then 1 ELSE 0 END) as LOSSES_1_2,
              SUM(case WHEN price_of_something_1 - price_of_something_3 >= 1 Then 1 ELSE 0 END) as LOSSES_1_3,        
              SUM(case WHEN price_of_something_1 - price_of_something_4 >= 1 Then 1 ELSE 0 END) as LOSSES_1_4,
              SUM(case WHEN price_of_something_1 - price_of_something_5 >= 1 Then 1 ELSE 0 END) as LOSSES_1_5,
                               
              --Prct Diff        
              SUM(case WHEN price_of_something_1 - price_of_something_2 <= -1 Then (price_of_something_1 - price_of_something_2)/price_of_something_2 ELSE null END) as WINS_SUM_PRCT_Differences_1_2,
              SUM(case WHEN price_of_something_1 - price_of_something_3 <= -1 Then (price_of_something_1 - price_of_something_3)/price_of_something_3 ELSE null END) as WINS_SUM_PRCT_Differences_1_3,        
              SUM(case WHEN price_of_something_1 - price_of_something_4 <= -1 Then (price_of_something_1 - price_of_something_4)/price_of_something_4 ELSE null END) as WINS_SUM_PRCT_Differences_1_4,
              SUM(case WHEN price_of_something_1 - price_of_something_5 <= -1 Then (price_of_something_1 - price_of_something_5)/price_of_something_5 ELSE null END) as WINS_SUM_PRCT_Differences_1_5,                
              --Prct Diff^2
 
              --Prct Diff        
              SUM(case WHEN price_of_something_1 - price_of_something_2 >= 1 Then (price_of_something_1 - price_of_something_2)/price_of_something_2 ELSE null END) as LOSES_SUM_PRCT_Differences_1_2,
              SUM(case WHEN price_of_something_1 - price_of_something_3 >= 1 Then (price_of_something_1 - price_of_something_3)/price_of_something_3 ELSE null END) as LOSES_SUM_PRCT_Differences_1_3,        
              SUM(case WHEN price_of_something_1 - price_of_something_4 >= 1 Then (price_of_something_1 - price_of_something_4)/price_of_something_4 ELSE null END) as LOSES_SUM_PRCT_Differences_1_4,
              SUM(case WHEN price_of_something_1 - price_of_something_5 >= 1 Then (price_of_something_1 - price_of_something_5)/price_of_something_5 ELSE null END) as LOSES_SUM_PRCT_Differences_1_5                    

      FROM company_sandbox_production.EA_PRODUCT_1_competition_""" + version + """stage_4_ID
      GROUP BY 
              Field_4, Field_1, requested_at_date_in_et, site_currency, some_order, Dom_Intl,
               is_publisher_1, Field_5_1, is_publisher_2, Field_5_2, 
              is_publisher_3, Field_5_3, is_publisher_4, Field_5_4,  is_publisher_5, Field_5_5,
              
              case WHEN price_of_something_1 - price_of_something_2 <= -1 Then 1 ELSE 0 END +
              case WHEN price_of_something_1 - price_of_something_3 <= -1 Then 1 ELSE 0 END +
              case WHEN price_of_something_1 - price_of_something_4 <= -1 Then 1 ELSE 0 END +
              case WHEN price_of_something_1 - price_of_something_5 <= -1 Then 1 ELSE 0 END ,
                      
              case WHEN price_of_something_1 - price_of_something_2 < 1 AND price_of_something_1 - price_of_something_2 > -1 Then 1 ELSE 0 END +
              case WHEN price_of_something_1 - price_of_something_3 < 1 AND price_of_something_1 - price_of_something_3 > -1 Then 1 ELSE 0 END +       
              case WHEN price_of_something_1 - price_of_something_4 < 1 AND price_of_something_1 - price_of_something_4 > -1 Then 1 ELSE 0 END +
              case WHEN price_of_something_1 - price_of_something_5 < 1 AND price_of_something_1 - price_of_something_5 > -1 Then 1 ELSE 0 END ,

              
              case WHEN price_of_something_1 - price_of_something_2 >= 1 Then 1 ELSE 0 END +
              case WHEN price_of_something_1 - price_of_something_3 >= 1 Then 1 ELSE 0 END +
              case WHEN price_of_something_1 - price_of_something_4 >= 1 Then 1 ELSE 0 END +
              case WHEN price_of_something_1 - price_of_something_5 >= 1 Then 1 ELSE 0 END,
              
              
              CASE WHEN is_publisher_2 = 1 AND price_of_something_1 - price_of_something_2 <= -1 THEN 1
               WHEN is_publisher_3 = 1 AND price_of_something_1 - price_of_something_3 <= -1 THEN 1
               WHEN is_publisher_4 = 1 AND price_of_something_1 - price_of_something_4 <= -1 THEN 1
               WHEN is_publisher_5 = 1 AND price_of_something_1 - price_of_something_5 <= -1 THEN 1
              ELSE 0 END ; COMMIT;

           ; SELECT analyze_histogram('company_sandbox_production.EA_PRODUCT_1_competition_""" + version + """report_ID',100) ; COMMIT;
           
             DELETE FROM company_sandbox_production.EA_PRODUCT_1_competition_""" + version + """stage_4_ID;  COMMIT

                            """

    print statement
    conn = cred.create_connection()
    cur = conn.cursor()
    cur.execute(statement)
    conn.close()

def insert_into_auction_report(stryear, strmonth, strday, currenttime):

# _"""  + strmonth + strday + stryear + """

    print "infunction_6"
    print strmonth + strday + stryear

    statement = """ 


      DELETE FROM company_sandbox_production.EA_PRODUCT_1_auction_reporting_ID
        WHERE requested_at_date_in_et in  (SELECT DISTINCT requested_at_date_in_et FROM company_sandbox_production.EA_PRODUCT_1_competition_stage_2_ID);

        INSERT INTO  company_sandbox_production.EA_PRODUCT_1_auction_reporting_ID
        
        SELECT   count(DISTINCT a1.request_id) as Event_Type_1s,  Pub_win, Pub_Tie, Pub_All_Around_Tie, Pub_loss,  a1.Field_4,  a1.Field_1, a1.requested_at_date_in_et, 
                 a1.site_currency, some_order, Dom_Intl, count_of_something, count_of_something_2,

                 SUM(Event_Type_2s_1) as Event_Type_2s_1, SUM(revenue_1) as revenue_1, SUM(Event_Type_2s_2) as Event_Type_2s_2, SUM(revenue_2) as revenue_2, SUM(Event_Type_2s_3) as Event_Type_2s_3, 
                 SUM(revenue_3) as revenue_3, SUM(Event_Type_2s_4) as Event_Type_2s_4, SUM(revenue_4) as revenue_4, SUM(bid_1) as bid_1, SUM(bid_2) as bid_2, SUM(bid_3) as bid_3, SUM(bid_4) as bid_4,
                 SUM(Num_Bids_At_Minimum) as Num_Bids_At_Minimum, SUM(Event_Type_2s_At_Minimum) as Event_Type_2s_At_Minimum, SUM(UnVALUE_1_Event_Type_3s) as UnVALUE_1_Event_Type_3s, 
                 VALUE_1_pages, site_reporting_value_06, knockout_Event_Type_3s, country_id, device_family

        FROM 
                ---Position 1 plus Ad Call Info
                (SELECT request_id, request_correlation_id, site_reporting_value_06, Pub_win, Pub_Tie, Pub_All_Around_Tie, Pub_loss,  Field_4,  Field_1, requested_at_date_in_et, site_currency, some_order, Dom_Intl, 
                count_of_something, count_of_something_2,
                Event_Type_2s as Event_Type_2s_1, revenue as revenue_1, bid as bid_1, tc.country_id, device_family
                FROM company_sandbox_production.EA_PRODUCT_1_competition_stage_2_ID r
                LEFT JOIN company_production.aft_entitys_targeted_countries tc on tc.aft_Field_5 = r.Field_5
                WHERE Field_2 = 1 ) a1

        LEFT JOIN  
                --Position 2
                (SELECT request_id,  Event_Type_2s as Event_Type_2s_2, revenue as revenue_2, bid as bid_2
                FROM company_sandbox_production.EA_PRODUCT_1_competition_stage_2_ID
                WHERE Field_2 = 2) a2 ON a2.request_id = a1.request_id

        LEFT JOIN  
                --Position 3
                (SELECT request_id,  Event_Type_2s as Event_Type_2s_3, revenue as revenue_3, bid as bid_3
                FROM company_sandbox_production.EA_PRODUCT_1_competition_stage_2_ID
                WHERE Field_2 = 3) a3 ON a3.request_id = a1.request_id

        LEFT JOIN  
                --Position 4
                (SELECT request_id,  Event_Type_2s as Event_Type_2s_4, revenue as revenue_4, bid as bid_4
                FROM company_sandbox_production.EA_PRODUCT_1_competition_stage_2_ID
                WHERE Field_2 = 4) a4 ON a4.request_id = a1.request_id

        LEFT JOIN 
                -- Number of Bids at Min
                (
                        SELECT request_id, COUNT(Bid) as Num_Bids_At_Minimum
                        FROM company_sandbox_production.EA_PRODUCT_1_competition_stage_2_ID r
                        LEFT JOIN company_production.aft_entitys_targeted_countries tc on tc.aft_Field_5 = r.Field_5
                        WHERE BID = floor
                        AND Field_2 > 0 
                        GROUP BY request_id
                ) minb ON a1.request_id = minb.request_id

        LEFT JOIN 
                -- Number of Event_Type_2s at Min
                (
                        SELECT request_id, SUM(Event_Type_2s) as Event_Type_2s_At_Minimum
                        FROM company_sandbox_production.EA_PRODUCT_1_competition_stage_2_ID r
                        LEFT JOIN company_production.aft_entitys_targeted_countries tc on tc.aft_Field_5 = r.Field_5
                        WHERE BID = floor                       
                        AND Field_2 > 0 AND revenue > 0
                        GROUP BY request_id    
                ) minEvent_Type_2 ON a1.request_id = minEvent_Type_2.request_id

        LEFT JOIN 
                -- Count of unVALUE_1 Event_Type_3s
                (SELECT request_id, COUNT(*) as UnVALUE_1_Event_Type_3s
                FROM company_sandbox_production.EA_PRODUCT_1_competition_stage_2_ID
                WHERE filter_cause_type = 'BID_TOO_LOW'
                GROUP BY request_id) f ON a1.request_id = f.request_id

        LEFT JOIN 
                -- Count of knockouts
                (SELECT request_id, COUNT(*) as knockout_Event_Type_3s
                FROM company_sandbox_production.EA_PRODUCT_1_competition_stage_2_ID
                WHERE filter_cause_type = 'GRADIENT_KNOCKOUT'
                GROUP BY request_id) k ON a1.request_id = k.request_id

        LEFT JOIN 
                -- Count of VALUE_1 Pages
                (SELECT COUNT(DISTINCT request_correlation_id) as VALUE_1_pages,  
                Field_4,  Field_1, requested_at_date_in_et,  site_currency
                FROM company_sandbox_production.EA_PRODUCT_1_competition_stage_2_ID
                WHERE Field_2 > 0 
                GROUP BY Field_4,  Field_1, requested_at_date_in_et,  site_currency) c on a1.Field_4 = c.Field_4 AND a1.Field_1 = c.Field_1 AND a1.requested_at_date_in_et = c.requested_at_date_in_et AND a1.site_currency = c.site_currency

        GROUP BY   
          Pub_win, Pub_Tie, Pub_all_around_tie, Pub_loss, a1.Field_4, a1.Field_1, a1.requested_at_date_in_et, a1.site_currency, some_order, Dom_Intl, count_of_something, count_of_something_2, knockout_Event_Type_3s,
          site_reporting_value_06, VALUE_1_pages, country_id, device_family;    COMMIT

           ; SELECT analyze_histogram('company_sandbox_production.EA_PRODUCT_1_auction_reporting_ID',100) ;
           
           DELETE FROM company_sandbox_production.EA_PRODUCT_1_competition_stage_2_ID ; COMMIT;
           DELETE FROM company_sandbox_production.EA_PRODUCT_1_competition_entity_stage_2_ID ; COMMIT


                            """

    print statement
    conn = cred.create_connection()
    cur = conn.cursor()
    cur.execute(statement)
    conn.close()


def runtheloop(year, month, day, step):

    stryear = str(year)
    # Set daystorun variable initially before starting loop
    # Add zeros for single digit days and forward slashes for dates
    if len(str(day)) == 1:
        strday = '0' + str(day) + '/'
    else:
        strday = str(day) + '/'

    if len(str(month)) == 1:
        strmonth = '0' + str(month) + '/'
    else:
        strmonth = str(month) + '/'

    #daystorun = (dt.date.today() - dt.date(year,month,day)).days - 1
    daystorun = (dt.date.today() - dt.date(year,month,day)).days - 1
    ongoing_date = dt.date(year,month,day)
    print daystorun

    while daystorun >= 0:
        
        # Redfine string variables as the loop progresses
        if len(str(ongoing_date.day)) == 1:
            strday = '0' + str(ongoing_date.day) + '/'
        else:
            strday = str(ongoing_date.day) + '/'

        if len(str(ongoing_date.month)) == 1:
            strmonth = '0' + str(ongoing_date.month) + '/'
        else:
            strmonth = str(ongoing_date.month) + '/'

        stryear = str(ongoing_date.year)


        # try:
        print "starting"

        # Run each of the functions for each part of the sql query.  I do it in steps so the loop can pick back up in the same place in case of an error
        if step == 1:
            pull_from_log_data(stryear, strmonth, strday, dt.datetime.today());
            step = 2;

        if step == 2:                
            win_loss_tie(stryear, strmonth, strday, dt.datetime.today())
            step = 3

        if step == 3:                
            win_loss_tie_entitys(stryear, strmonth, strday, dt.datetime.today())
            step = 4                

        if step == 4:
            create_5_versions(stryear, strmonth, strday, '', dt.datetime.today())            
            step = 5

        if step == 5:
            create_5_versions(stryear, strmonth, strday, 'entity_', dt.datetime.today())            
            step = 6

        if step == 6:
            transpose_rows_for_each_version(stryear, strmonth, strday, '', dt.datetime.today())            
            step = 7

        if step == 7:
            transpose_rows_for_each_version(stryear, strmonth, strday, 'entity_', dt.datetime.today())            
            step = 8

        if step == 8:
            insert_into_competition_report(stryear, strmonth[:-1], strday[:-1], '', dt.datetime.today())
            step = 9   

        if step == 9:
            insert_into_competition_report(stryear, strmonth[:-1], strday[:-1], 'entity_', dt.datetime.today())
            step = 10

        if step == 10:
            insert_into_auction_report(stryear, strmonth[:-1], strday[:-1], dt.datetime.today())
            step = 1                    

        print "ending"
        daystorun = daystorun - 1
        day = day + 1
        ongoing_date = ongoing_date + dt.timedelta(days=1)

        # except Exception:
        #     print "Error"
        #     print "Error", sys.exc_info()[0], sys.exc_info()[1]
        #     runtheloop(ongoing_date.year, ongoing_date.month, ongoing_date.day, step)

runtheloop(2015, 8, 15, 1)