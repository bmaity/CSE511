
-- fill query1 table
CREATE TABLE query1 AS
SELECT name AS NAME, COUNT(*) AS MOVIECOUNT 
FROM movies m
	JOIN hasagenre h ON h.movieid=m.movieid
	JOIN genres g ON g.genreid=h.genreid
GROUP BY g.name;

-- fill query2 table
CREATE TABLE query2 AS
SELECT g.name AS NAME, AVG(rating) AS RATING
FROM genres g
	JOIN hasagenre h ON g.genreid=h.genreid
	JOIN ratings r ON h.movieid=r.movieid
GROUP BY g.genreid;

-- fill query3 table
-- movies with at least 10 ratings

CREATE TABLE query3 AS
SELECT title, countofratings
FROM movies, (
SELECT movieid, count(rating) as countofratings
FROM ratings
GROUP BY movieid
HAVING count(rating)>=10) helper
WHERE movies.movieid = helper.movieid;

-- fill query4 table
-- all comedy movies

CREATE TABLE query4 AS
SELECT m.movieid AS MOVIEID, title AS TITLE
FROM hasagenre h
JOIN movies m on h.movieid=m.movieid
WHERE h.genreid=5;

-- fill query5 table
-- movies and average rating
CREATE TABLE query5 AS
SELECT m.title AS TITLE, AVG(r.rating) AS AVERAGE
FROM movies m
	JOIN ratings r ON m.movieid=r.movieid
GROUP BY m.title;

-- fill query6 table
-- average rating of all comedy movies
CREATE TABLE query6 AS
SELECT AVG(r.rating) AS AVERAGE
FROM movies m
	JOIN ratings r ON m.movieid=r.movieid
	JOIN hasagenre h ON h.movieid=m.movieid
	JOIN genres g ON g.genreid=h.genreid
GROUP BY g.genreid
HAVING g.genreid=5;

-- fill query7 table
-- average rating of all comedy and romance movies movies
CREATE TABLE query7 AS
SELECT avg(ratings.rating) AS average
FROM movies, hasagenre, genres, ratings
WHERE movies.movieid = hasagenre.movieid
	AND hasagenre.genreid = genres.genreid
	AND ratings.movieid = movies.movieid
	AND genres.name = 'Romance'
	AND movies.movieid IN
	(
		SELECT movies.movieid
		FROM movies, hasagenre, genres
		WHERE movies.movieid = hasagenre.movieid
			AND hasagenre.genreid = genres.genreid
			AND genres.name = 'Comedy'
	);

--- fill query8 table
-- -- average rating of all romance and not comedy movies movies
CREATE TABLE query8 AS
SELECT AVG(r.rating) AS AVERAGE
FROM movies m
	JOIN ratings r ON m.movieid=r.movieid
	JOIN hasagenre h ON h.movieid=m.movieid
	JOIN genres g ON g.genreid=h.genreid
GROUP BY g.genreid
HAVING g.genreid=14 AND NOT g.genreid=5;

-- fill query9 table
CREATE TABLE query9 AS
SELECT r.movieid, r.rating
FROM ratings r
WHERE r.userid = :v1;

-- query10
CREATE TABLE query10
(
	TITLE TEXT
);

-- fill query10 table
-- movie recommendation


-- recommendation
CREATE TABLE recommendation
(
	MOVIEID1 INT,
	MOVIEID2 INT,
	SIM NUMERIC
);
