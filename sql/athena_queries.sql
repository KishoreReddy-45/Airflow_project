-- Confirm the Glue database and tables exist before querying.
SELECT * FROM ratings.ratings_silver LIMIT 20;

-- Top rated movies with at least 10 ratings.
SELECT movie_id, average_rating, rating_count
FROM ratings.gold_movie_rating_summary
WHERE rating_count >= 10
ORDER BY average_rating DESC, rating_count DESC
LIMIT 20;

-- Daily volume and average score.
SELECT rating_date, rating_count, average_rating
FROM ratings.gold_daily_rating_summary
ORDER BY rating_date DESC;

-- Most active users.
SELECT user_id, rating_count, average_rating
FROM ratings.gold_user_rating_summary
ORDER BY rating_count DESC
LIMIT 20;
