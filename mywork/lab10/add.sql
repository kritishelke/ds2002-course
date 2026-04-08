USE bae7kx_db;

INSERT INTO movies (movie_id, title, director, release_year, genre)
VALUES (11, 'Whiplash', 'Damien Chazelle', 2014, 'Drama');

INSERT INTO movies (movie_id, title, director, release_year, genre)
VALUES (12, 'The Prestige', 'Christopher Nolan', 2006, 'Thriller');

INSERT INTO reviews (review_id, movie_id, reviewer_name, rating, review_text, review_date)
VALUES (11, 11, 'name11', 5, 'Intense and brilliantly acted.', '2026-04-08 13:00:00');

INSERT INTO reviews (review_id, movie_id, reviewer_name, rating, review_text, review_date)
VALUES (12, 12, 'name12', 5, 'Clever plot and great performances.', '2026-04-08 13:15:00');

INSERT INTO reviews (review_id, movie_id, reviewer_name, rating, review_text, review_date)
VALUES (13, 3, 'name13', 4, 'Great visuals and a strong story.', '2026-04-08 13:30:00');
