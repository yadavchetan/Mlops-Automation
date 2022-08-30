use DATABANKDB;
DROP TABLE IF EXISTS MST_TBL_JIRA_DETAILS;
CREATE TABLE MST_TBL_JIRA_DETAILS
(
JIRA_ID			VARCHAR(50),
DOWNLOAD_DATE	DATE		 NOT NULL,
CLIENT_NAME		VARCHAR(20)	 NOT NULL,
START_DATE		DATE		 NOT NULL,
END_DATE		DATE		 NOT NULL,
DOCUMENT_TYPE	VARCHAR(500) NOT NULL,
PIPELINE		VARCHAR(25)  NOT NULL,
PRIMARY KEY (JIRA_ID)
); 
