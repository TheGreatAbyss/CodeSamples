-- Step 16

-- Large Collection Extract
UNLOAD('
SELECT
  TRUNC(RANDOM() * 10000) as meta_bucket,
  l.person_id,
  first_name,
  split_part(last_name, \' \', 1) AS last_name,
  middle_initial,
  zip_code,
  state,
  city,
  address_cat,
  NULLIF(birth_month + \'-\' + birth_year,
         \'-\')                        AS birth_month_year
FROM large_collection.person_data l
  INNER JOIN large_collection.person_data_ext r
    ON l.person_id = r.person_id AND l.person_id2 = r.person_id2
')
to 's3://example_bucket/tf-idf/input/large_collection/'
CREDENTIALS 'aws_access_key_id=XXXX;aws_secret_access_key=XXXX';


--Small Collection Extract
UNLOAD('
SELECT
  person_id,
  person_first_nam,
  person_last_nam,
  left(person_middle_nam, 1)                      AS middle_name,
  left(zip_cde, 5)                                AS zip,
  state_prov_cde,
  city_long_nam,
  CASE address_line_1_txt
  WHEN \'NULL\'
    THEN \'\'
  ELSE address_line_1_txt + \' \' END            AS address,
  NULLIF(CASE WHEN LEN(CONVERT(VARCHAR(2), birthdate_mth_num)) = 1
    THEN \'0\' || birthdate_mth_num
         ELSE \'\' || birthdate_mth_num END
         + \'-\' + person_data.birthdate_yr_num, \'-\') AS birth_month_year
FROM small_collection_data.person_data
WHERE person_id NOT IN (SELECT person_id FROM small_collection_data.string_matching_final_results)
')
to 's3://example_bucket/tf-idf/input/small_collection_limited/'
CREDENTIALS 'aws_access_key_id=XXX;aws_secret_access_key=XXXX';


-- Step 18
DROP TABLE small_collection_data.tf_idf_test;
CREATE TABLE small_collection_data.tf_idf_test
(
  person_id        BIGINT,
  person_id CHAR(16),
  similarity_score  FLOAT
);


TRUNCATE small_collection_data.tf_idf_test;
COPY small_collection_data.tf_idf_test
FROM 's3://example_bucket/tf-idf/output/03_03_2017_00_23'
CREDENTIALS 'aws_access_key_id=XXXX;aws_secret_access_key=XXXX';
DELIMITER ',';

GRANT SELECT ON small_collection_data.tf_idf_test TO group small_collection_data;

SELECT * FROM small_collection_data.tf_idf_test
ORDER BY person_id, person_id;


-- Step 19
--- See top matches
DROP TABLE small_collection_data.tf_idf_top_matches_test;
CREATE TABLE small_collection_data.tf_idf_top_matches_test
  AS
SELECT tf.person_id, min(person_id) as person_id, tf.similarity_score
FROM small_collection_data.tf_idf_test tf
  JOIN
  (SELECT
     person_id,
     max(similarity_score) AS max_score
   FROM small_collection_data.tf_idf_test
   GROUP BY person_id) ms
    ON tf.person_id = ms.person_id AND tf.similarity_score = ms.max_score
GROUP BY tf.person_id, tf.similarity_score;



--- Step 20
--- Join to original tables to get names
--- Use string position to determine if name from one is included in name from the other.
--- So this ignores anybody who doesn't have some sort of matching name
SELECT *
FROM (
  SELECT
    p.person_first_nam,
    l.first_name,
    p.person_last_nam,
    l.last_name,
    STRPOS(trim(p.person_first_nam), trim(l.first_name)) +
    STRPOS(trim(l.first_name), trim(p.person_first_nam))                                                         AS first_name_sub_score,
    STRPOS(trim(p.person_last_nam), trim(l.last_name)) + STRPOS(trim(l.last_name), trim(
        p.person_last_nam))                                                                                           AS last_name_sub_score,
    tf.*
  FROM small_collection_data.tf_idf_top_matches_test tf
    JOIN large_collection.person_data l ON l.person_id = tf.person_id
    JOIN small_collection_data.person_data p ON p.person_id = tf.person_id
)main
WHERE first_name_sub_score > 0 AND last_name_sub_score > 0
  AND similarity_score >= 10
ORDER BY similarity_score DESC;

---- Load above results into string matching table
INSERT INTO small_collection_data.string_matching_accepted_results
SELECT 'tf-idf' as bucket, person_id, person_id, null, null, null, null, null, null, null, null, null, similarity_score, null
FROM (
  SELECT
    p.person_first_nam,
    l.first_name,
    p.person_last_nam,
    l.last_name,
    STRPOS(trim(p.person_first_nam), trim(l.first_name)) +
    STRPOS(trim(l.first_name), trim(p.person_first_nam))                                                         AS first_name_sub_score,
    STRPOS(trim(p.person_last_nam), trim(l.last_name)) + STRPOS(trim(l.last_name), trim(
        p.person_last_nam))                                                                                           AS last_name_sub_score,
    tf.*
  FROM small_collection_data.tf_idf_top_matches_test tf
    JOIN large_collection.person_data l ON l.person_id = tf.person_id
    JOIN small_collection_data.person_data p ON p.person_id = tf.person_id
)main
WHERE first_name_sub_score > 0 AND last_name_sub_score > 0
  AND similarity_score >= 10;

--- Step 21
--- Further examine the top tf-idf matches that are not in the accepted results by running a string matching score between first and last
--- names with similarity scores over 10
SELECT
    tf.*,
    p.person_first_nam,
    l.first_name,
    p.person_last_nam,
    l.last_name
FROM small_collection_data.tf_idf_top_matches_test tf
      JOIN large_collection.person_data l ON l.person_id = tf.person_id
    JOIN small_collection_data.person_data p ON p.person_id = tf.person_id
WHERE tf.person_id NOT IN (SELECT person_id FROM small_collection_data.string_matching_accepted_results)
AND similarity_score >= 10;

UNLOAD (
'SELECT
    tf.*,
    p.person_first_nam,
    l.first_name,
    p.person_last_nam,
    l.last_name
FROM small_collection_data.tf_idf_top_matches_test tf
      JOIN large_collection.person_data l ON l.person_id = tf.person_id
    JOIN small_collection_data.person_data p ON p.person_id = tf.person_id
WHERE tf.person_id NOT IN (SELECT person_id FROM small_collection_data.string_matching_accepted_results)
AND similarity_score >= 10
')
to 's3://example_bucket/tf-idf/results-for-string-matching/03-13-2017'
CREDENTIALS 'aws_access_key_id=XXXX;aws_secret_access_key=XXXX';
PARALLEL OFF;

-- Step 23
----- Load tf-idf results with name string similarity score into a table
CREATE TABLE tf_idf_name_string_matching_scores (
  person_id BIGINT,
  person_id VARCHAR(16),
  similarity_score DOUBLE PRECISION,
  small_collection_person_first_name VARCHAR(50),
  large_collection_person_first_name VARCHAR(50),
  small_collection_person_last_name VARCHAR(50),
  large_collection_person_last_name VARCHAR(50),
  first_name_string_score FLOAT,
  last_name_string_score FLOAT
);
--- Load data to table
COPY tf_idf_name_string_matching_scores
FROM 's3://example_bucket/tf-idf/results-for-string-matching/tf-idf-name-similarity-scores.txt'
CREDENTIALS 'aws_access_key_id=XXXX;aws_secret_access_key=XXXX';

-- Step 24
---- Find results that appear to match
SELECT *
FROM tf_idf_name_string_matching_scores
WHERE first_name_string_score > 70 AND last_name_string_score > 70
ORDER BY last_name_string_score ASC;

SELECT COUNT(*)
FROM tf_idf_name_string_matching_scores
WHERE first_name_string_score > 70 AND last_name_string_score > 70;

---- Load above results into string matching table
INSERT INTO small_collection_data.string_matching_accepted_results
SELECT 'tf-idf-name-string-match' as bucket, person_id, person_id, null, null, null, null, null, null, null, null, null, similarity_score, null
FROM tf_idf_name_string_matching_scores
WHERE first_name_string_score > 70 AND last_name_string_score > 70;