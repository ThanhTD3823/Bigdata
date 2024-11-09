CREATE TABLE netflix_movies_raw (title STRING, director STRING, casts STRING, country STRING, date_added STRING, release_year INT, rating STRING, duration STRING, listed_in STRING, description STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
STORED AS TEXTFILE;
