use DATABANKDB;
DROP TABLE IF EXISTS LKP_DOCTYPE;
CREATE TABLE LKP_DOCTYPE
(
DOCTYPE_ID INT AUTO_INCREMENT,
DOCTYPE_NAME VARCHAR(100) NOT NULL,
PRIMARY KEY (DOCTYPE_ID),
UNIQUE KEY UNIQUE_DOCTYPE(DOCTYPE_NAME)
); 
