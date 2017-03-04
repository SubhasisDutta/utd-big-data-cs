business_raw = LOAD 'input2/business.csv' using PigStorage(':') as (business_id: chararray, b1 ,full_address: chararray, b2 ,
		categories: chararray);
business_table = FOREACH business_raw GENERATE business_id, full_address;

business_filtered = FILTER business_table BY (full_address matches '.*Stanford.*');

--DESCRIBE business_filtered;

review_raw = LOAD 'input2/review.csv' using PigStorage(':') as (review_id,b1, user_id: chararray,b2, business_id: chararray,b3, stars: float);

review_table = FOREACH review_raw GENERATE business_id, user_id, stars;

join_table = JOIN business_filtered BY business_id, review_table BY business_id;

top_N = LIMIT join_table 10;

--DESCRIBE top_N;

--DUMP top_N;

--top_N: {business_filtered::business_id: chararray,business_filtered::full_address: chararray,review_table::business_id: chararray,review_table::user_id: bytearray,review_table::stars: float}

result = FOREACH top_N GENERATE $3 AS User_ID, $4 AS Stars;

--DESCRIBE result;

STORE result INTO 'pig_output4' USING PigStorage('\t');


