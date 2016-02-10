CREATE TABLE users (
    user_id int(11) NOT NULL AUTO_INCREMENT,
    user_name varchar(100) NOT NULL,
    user_name_index varchar(100) NOT NULL,
    user_pass char(40) NOT NULL,
    PRIMARY KEY (user_id),
    UNIQUE KEY (user_name_index)
)
