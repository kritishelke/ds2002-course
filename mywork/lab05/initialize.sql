-- Create movies table
CREATE TABLE movies (
    movie_id INT PRIMARY KEY,
    title VARCHAR(255) NOT NULL,
    director VARCHAR(255),
    release_year INT,
    genre VARCHAR(100)
);

-- Create reviews table
CREATE TABLE reviews (
    review_id INT PRIMARY KEY,
    movie_id INT,
    reviewer_name VARCHAR(255),
    rating INT,
    review_text TEXT,
    review_date DATETIME,

    FOREIGN KEY (movie_id) REFERENCES movies(movie_id)
);

-- Insert 10 movies
INSERT INTO movies (movie_id, title, director, release_year, genre)
VALUES (1, 'Inception', 'Christopher Nolan', 2010, 'Sci-Fi');

INSERT INTO movies (movie_id, title, director, release_year, genre)
VALUES (2, 'The Dark Knight', 'Christopher Nolan', 2008, 'Action');

INSERT INTO movies (movie_id, title, director, release_year, genre)
VALUES (3, 'Interstellar', 'Christopher Nolan', 2014, 'Sci-Fi');

INSERT INTO movies (movie_id, title, director, release_year, genre)
VALUES (4, 'Parasite', 'Bong Joon-ho', 2019, 'Thriller');

INSERT INTO movies (movie_id, title, director, release_year, genre)
VALUES (5, 'La La Land', 'Damien Chazelle', 2016, 'Musical');

INSERT INTO movies (movie_id, title, director, release_year, genre)
VALUES (6, 'Titanic', 'James Cameron', 1997, 'Romance');

INSERT INTO movies (movie_id, title, director, release_year, genre)
VALUES (7, 'Avengers: Endgame', 'Anthony and Joe Russo', 2019, 'Superhero');

INSERT INTO movies (movie_id, title, director, release_year, genre)
VALUES (8, 'Get Out', 'Jordan Peele', 2017, 'Horror');

INSERT INTO movies (movie_id, title, director, release_year, genre)
VALUES (9, 'The Grand Budapest Hotel', 'Wes Anderson', 2014, 'Comedy');

INSERT INTO movies (movie_id, title, director, release_year, genre)
VALUES (10, 'Spirited Away', 'Hayao Miyazaki', 2001, 'Animation');

-- Insert 10 reviews
INSERT INTO reviews (review_id, movie_id, reviewer_name, rating, review_text, review_date)
VALUES (1, 1, 'name1', 5, 'Amazing concept and visuals.', '2026-03-09 10:00:00');

INSERT INTO reviews (review_id, movie_id, reviewer_name, rating, review_text, review_date)
VALUES (2, 2, 'name2', 5, 'One of the best superhero movies ever.', '2026-03-09 10:15:00');

INSERT INTO reviews (review_id, movie_id, reviewer_name, rating, review_text, review_date)
VALUES (3, 3, 'name3', 4, 'Very emotional and thought-provoking.', '2026-03-09 10:30:00');

INSERT INTO reviews (review_id, movie_id, reviewer_name, rating, review_text, review_date)
VALUES (4, 4, 'name4', 5, 'Brilliant social commentary.', '2026-03-09 10:45:00');

INSERT INTO reviews (review_id, movie_id, reviewer_name, rating, review_text, review_date)
VALUES (5, 5, 'name5', 4, 'Beautiful music and cinematography.', '2026-03-09 11:00:00');

INSERT INTO reviews (review_id, movie_id, reviewer_name, rating, review_text, review_date)
VALUES (6, 6, 'name6', 4, 'Classic romance with unforgettable scenes.', '2026-03-09 11:15:00');

INSERT INTO reviews (review_id, movie_id, reviewer_name, rating, review_text, review_date)
VALUES (7, 7, 'name7', 5, 'Epic ending to the Marvel saga.', '2026-03-09 11:30:00');

INSERT INTO reviews (review_id, movie_id, reviewer_name, rating, review_text, review_date)
VALUES (8, 8, 'name8', 5, 'Creepy, smart, and original.', '2026-03-09 11:45:00');

INSERT INTO reviews (review_id, movie_id, reviewer_name, rating, review_text, review_date)
VALUES (9, 9, 'name9', 4, 'Stylish and fun movie.', '2026-03-09 12:00:00');

INSERT INTO reviews (review_id, movie_id, reviewer_name, rating, review_text, review_date)
VALUES (10, 10, 'name10', 5, 'A magical animated masterpiece.', '2026-03-09 12:15:00');