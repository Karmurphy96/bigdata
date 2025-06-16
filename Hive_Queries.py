 Total Number of Reviews:

SELECT COUNT(*) FROM cleaned_reviews;

 Average Rating:

SELECT AVG(rating) AS average_rating FROM cleaned_reviews;


Top 10 Products by Average Rating:

SELECT asin, AVG(rating) AS avg_rating
FROM cleaned_reviews
GROUP BY asin
ORDER BY avg_rating DESC
LIMIT 10;

 Top 10 Users by Number of Reviews:

SELECT user_id, COUNT(*) AS review_count
FROM cleaned_reviews
GROUP BY user_id
ORDER BY review_count DESC
LIMIT 10;
