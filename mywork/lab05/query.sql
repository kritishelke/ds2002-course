USE lab5;

SELECT 
    movies.title,
    movies.genre,
    reviews.reviewer_name,
    reviews.rating,
    reviews.review_text
FROM movies
JOIN reviews ON movies.movie_id = reviews.movie_id
WHERE reviews.rating >= 4; 