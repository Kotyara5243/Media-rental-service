
CREATE DATABASE IF NOT EXISTS media_rental_db;
USE media_rental_db;

CREATE TABLE Family (
	family_id INT AUTO_INCREMENT PRIMARY KEY,
	family_type VARCHAR(50),
	creation_date DATE
);

CREATE TABLE Users (
	user_id INT AUTO_INCREMENT PRIMARY KEY,
	user_name VARCHAR(100),
	email VARCHAR(100),
	birthday DATE,
	location VARCHAR(100),
	bio VARCHAR(1000),
	family_id INT,
	FOREIGN KEY (family_id) REFERENCES Family(family_id)
);

CREATE TABLE Friendships (
	user_id INT NOT NULL,
	friend_id INT NOT NULL,
	PRIMARY KEY (user_id, friend_id),
	FOREIGN KEY (user_id) REFERENCES Users(user_id) ON DELETE CASCADE,
	FOREIGN KEY (friend_id) REFERENCES Users(user_id) ON DELETE CASCADE,
	CHECK (user_id <> friend_id)
);

CREATE TABLE Media (
	media_id INT AUTO_INCREMENT PRIMARY KEY,
	media_name VARCHAR(100) NOT NULL,
	genre VARCHAR(100),
	prod_year INT,
	descr VARCHAR(1000),
	location VARCHAR(100),
	cost_per_day INT
);

CREATE TABLE Series (
	series_id INT AUTO_INCREMENT PRIMARY KEY,
	number_of_episodes INT NOT NULL,
	is_ongoing BOOLEAN,
	media_id INT REFERENCES Media(media_id)
);

CREATE TABLE Film (
	film_id INT AUTO_INCREMENT PRIMARY KEY,
	duration INT,
	number_of_parts INT,
	media_id INT REFERENCES Media(media_id)
); 

CREATE TABLE Sessions (
	session_id INT AUTO_INCREMENT,
	user_id INT,
	media_id INT,
	date_of_rent DATETIME,
	cost INT,
	duration INT, 
	PRIMARY KEY (session_id),
	FOREIGN KEY (user_id) REFERENCES Users(user_id) ON DELETE CASCADE,
	FOREIGN KEY (media_id) REFERENCES Media(media_id) ON DELETE CASCADE
);


CREATE TABLE WatchHistory (
	watch_history_id INT AUTO_INCREMENT,
	user_id INT, 
	media_id INT,
	date_of_watch DATE,
	family_watch BOOLEAN,
	PRIMARY KEY (watch_history_id),
	FOREIGN KEY (media_id) REFERENCES Media(media_id) ON DELETE CASCADE,
	FOREIGN KEY (user_id) REFERENCES Users(user_id) ON DELETE CASCADE
);

CREATE TABLE Device (
    device_id INT AUTO_INCREMENT,
    device_name VARCHAR(100),
    registration_date DATE,
    user_id INT,
    PRIMARY KEY (device_id, user_id),  
    FOREIGN KEY (user_id) REFERENCES Users(user_id)
);

-- -- Add some sample data
-- INSERT INTO Family (family_type, creation_date) VALUES
-- ('Movie Lovers', '2025-11-07'),
-- ('Sci-Fi Fans', '2025-11-05');

-- INSERT INTO Users (user_name, email, birthday, location, bio, family_id) VALUES
-- ('Zhami', 'zhami@example.com', '2002-11-22', 'Almaty', 'Film enthusiast', 1),
-- ('Grisha', 'grisha@example.com', '1990-05-10', 'Moscow', 'Loves Sci-Fi movies', 2);

-- INSERT INTO Media (media_name, genre, prod_year, descr, location, cost_per_day) VALUES
-- ('Inception', 'Sci-Fi', 2010, 'Dream-sharing technology', 'USA', 8),
-- ('Breaking Bad', 'Crime Drama', 2008, 'Chemistry teacher makes drugs', 'USA', 4);

-- INSERT INTO Film (duration, number_of_parts, media_id) VALUES
-- (148, 1, 1);

-- INSERT INTO Series (number_of_episodes, is_ongoing, media_id) VALUES
-- (62, FALSE, 2);
