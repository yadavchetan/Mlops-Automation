USE DATABANKDB;
DROP PROCEDURE IF EXISTS STG_PROC_ADR;

/*
Title:
Description:
Date:
Initial Author:

Revisions:
--------------------------------------------------------------------------------------
Date		Author		Revision Reason
--------------------------------------------------------------------------------------
*/

DELIMITER $$

CREATE PROCEDURE STG_PROC_ADR()
BEGIN
	
	DECLARE EXIT HANDLER FOR 1146
	  SELECT 'Please create table abc first' Message;
	  
	TRUNCATE TABLE STG_TBL_ADR;
	SELECT 'Table truncated successfully' Message;

	INSERT INTO STG_TBL_ADR (PIPLELINE, CLIENT_NAME, REFERENCE_KEY, PROCESS, SUB_PROCESS, LOAN_ID, DOCUMENT_ID, PAGE_ID, JIRA_ID, START_DATE, END_DATE, PROCESS_DATE, DOCUMENT_TYPE, PAGE_NO, EXTENSION)
	SELECT 
		PIPLELINE, 
		CLIENT_NAME, 
		REFERENCE_KEY, 
		PROCESS, 
		SUB_PROCESS,	
		LOAN_ID, 
		DOCUMENT_ID, 
		PAGE_ID,
		substring_index((substring_index(REFERENCE_KEY,'_',1)),'_',-1) as JIRA_ID,
		substring_index((substring_index(REFERENCE_KEY,'_',2)),'_',-1) as START_DATE,
		substring_index((substring_index(REFERENCE_KEY,'_',3)),'_',-1) as END_DATE,
		substring_index(REFERENCE_KEY,'_',-1) as PROCESS_DATE,
		substring_index(DOCUMENT_ID,'-',-1) as DOCUMENT_TYPE,
		substring_index((substring_index(PAGE_ID,'.',1)),'-',-1) as PAGE_NO,
		substring_index((substring_index(PAGE_ID,'.',3)),'.',-1) as EXTENSION
	from SRC_TBL_ADR;
	SELECT 'Record successfully Inserted In STG_TBL_ADR' Message;
	
END$$

DELIMITER ;


