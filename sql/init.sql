
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
