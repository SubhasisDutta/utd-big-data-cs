business_raw = LOAD 'input2/business.csv' using PigStorage(':') as (business_id: chararray, b1 ,full_address: chararray, b2 ,
		categories: chararray);
business_table = FOREACH business_raw GENERATE business_id, full_address, categories;

business_filtered = FILTER business_table BY (full_address matches '.*Palo Alto, CA.*');

--DESCRIBE business_filtered;

review_raw = LOAD 'input2/review.csv' using PigStorage(':') as (review_id,b1, user_id,b2, business_id: chararray,b3, stars: float);

review_table = FOREACH review_raw GENERATE business_id, stars;

--DESCRIBE review_table;

review_group_table = GROUP review_table BY business_id;

review_avg_table = FOREACH review_group_table GENERATE group AS business_id, AVG(review_table.stars) AS average_rating;

--DESCRIBE review_avg_table;

join_table = JOIN business_filtered BY business_id, review_avg_table BY business_id;

--DESCRIBE join_table;

group_by_rating = GROUP join_table BY average_rating;

--DESCRIBE group_by_rating;

group_by_rating_desc = ORDER group_by_rating BY group DESC;

--DESCRIBE group_by_rating_desc;

top_10 = LIMIT group_by_rating_desc 10;

--DESCRIBE top_10;

full_result = FOREACH top_10 GENERATE FLATTEN(join_table), group;

--DESCRIBE full_result;

result = FOREACH full_result GENERATE $0 AS Business_ID, $1 AS Full_Address, $2 AS Categories, $4 AS AVG_Rating;

--DESCRIBE result;

--DUMP result;

STORE result INTO 'pig_output1' USING PigStorage('\t');
