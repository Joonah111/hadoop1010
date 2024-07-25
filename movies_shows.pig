-- Load the dataset from the local filesystem
data = LOAD 'C:/Users/Dell/Downloads/Netflix_Shows.csv' USING PigStorage(',') AS (show_id:chararray, type:chararray, title:chararray, director:chararray, country:chararray, date_added:chararray, release_year:int, rating:chararray, duration:chararray, listed_in:chararray);

-- Filter shows released after 2015
filtered_data = FILTER data BY release_year > 2015;

-- Process the filtered data
processed_data = FOREACH filtered_data GENERATE show_id, type, title, director, country, date_added, release_year, rating, duration, listed_in;

-- Store the processed data
STORE processed_data INTO 'C:/Users/Dell/Downloads/processed_data' USING PigStorage(',');

-- Order the data by release year in descending order
ordered_data = ORDER filtered_data BY release_year DESC;

-- Store the ordered data
STORE ordered_data INTO 'C:/Users/Dell/Downloads/ordered_data' USING PigStorage(',');

-- Load datasets for two countries (assuming these files are also on your local filesystem)
data_country1 = LOAD 'C:/Users/Dell/Downloads/Netflix_Shows_Country1.csv' USING PigStorage(',') AS (show_id:chararray, type:chararray, title:chararray, director:chararray, country:chararray, date_added:chararray, release_year:int, rating:chararray, duration:chararray, listed_in:chararray);
data_country2 = LOAD 'C:/Users/Dell/Downloads/Netflix_Shows_Country2.csv' USING PigStorage(',') AS (show_id:chararray, type:chararray, title:chararray, director:chararray, country:chararray, date_added:chararray, release_year:int, rating:chararray, duration:chararray, listed_in:chararray);

-- Join datasets on show_id
joined_data = JOIN data_country1 BY show_id, data_country2 BY show_id;

-- Process the joined data
processed_joined_data = FOREACH joined_data GENERATE 
    data_country1::show_id, 
    data_country1::type AS type_country1, 
    data_country1::title AS title_country1, 
    data_country2::type AS type_country2, 
    data_country2::title AS title_country2, 
    data_country1::director AS director_country1, 
    data_country2::director AS director_country2, 
    data_country1::country AS country_country1, 
    data_country2::country AS country_country2, 
    data_country1::release_year, 
    data_country1::rating AS rating_country1, 
    data_country2::rating AS rating_country2, 
    data_country1::duration AS duration_country1, 
    data_country2::duration AS duration_country2, 
    data_country1::listed_in AS listed_in_country1, 
    data_country2::listed_in AS listed_in_country2;

-- Store the joined data
STORE processed_joined_data INTO 'C:/Users/Dell/Downloads/joined_data' USING PigStorage(',');

-- Optional: Export data for further statistical analysis (variance, covariance, etc.)
STORE processed_data INTO 'C:/Users/Dell/Downloads/exported_data' USING PigStorage(',');