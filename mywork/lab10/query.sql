USE bae7kx_db;

SELECT * FROM movies;

SELECT * FROM reviews;

SELECT m.movie_id, m.title, r.reviewer_name, r.rating
FROM movies m
JOIN reviews r ON m.movie_id = r.movie_id
ORDER BY m.movie_id, r.review_id;
