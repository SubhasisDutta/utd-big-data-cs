business_raw = LOAD 'input2/business.csv' using PigStorage(':') as (business_id: chararray, b1 ,full_address: chararray, b2 ,
		categories: chararray);
business_table = FOREACH business_raw GENERATE business_id, full_address, categories;

review_raw = LOAD 'input2/review.csv' using PigStorage(':') as (review_id,b1, user_id,b2, business_id: chararray,b3, stars: float);

review_table = FOREACH review_raw GENERATE business_id, stars;

cogroup_table = COGROUP business_table BY business_id, review_table BY business_id;

top_5 = LIMIT cogroup_table 5;

--DESCRIBE cogroup_table;
--cogroup_table: {group: chararray,business_table: {(business_id: chararray,full_address: chararray,categories: chararray)},review_table: {(business_id: chararray,stars: float)}}

full_result = FOREACH top_5 GENERATE FLATTEN(business_table), FLATTEN(review_table), group;

DESCRIBE full_result;
--full_result: {business_table::business_id: chararray,business_table::full_address: chararray,business_table::categories: chararray,review_table::business_id: chararray,review_table::stars: float,group: chararray}

result = FOREACH full_result GENERATE $0 AS Business_ID, $1 AS Full_Address, $2 AS Categories, $4 AS Stars;

--DESCRIBE result;

--DUMP result;

STORE result INTO 'pig_output3' USING PigStorage('\t');


