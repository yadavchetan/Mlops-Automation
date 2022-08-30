USE DATABANKDB; 


DROP PROCEDURE IF EXISTS TGT_PROC_LOAD_INCRMT_SNAPSHOT;


DELIMITER $$

CREATE PROCEDURE TGT_PROC_LOAD_INCRMT_SNAPSHOT()
BEGIN

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
	DECLARE EXIT HANDLER FOR 1146
	

		INSERT INTO TGT_TBL_DATABANK_DLY_SNAPSHOT (UPDATED_DATE, PIPLELINE, CLIENT_NAME, REFERENCE_KEY, PROCESS, SUB_PROCESS, LOAN_ID, DOCUMENT_ID, PAGE_ID, JIRA_ID, START_DATE, END_DATE, PROCESS_DATE,DOCUMENT_TYPE, PAGE_NO, EXTENSION)
			(SELECT 
				current_date() AS UPDATED_DATE,
				PIPLELINE, 
				CLIENT_NAME, 
				REFERENCE_KEY, 
				PROCESS, 
				SUB_PROCESS,	
				LOAN_ID, 
				DOCUMENT_ID, 
				PAGE_ID,
				JIRA_ID,
				START_DATE,
				END_DATE,
				PROCESS_DATE,
				DOCUMENT_TYPE,
				PAGE_NO,
				EXTENSION
			from STG_TBL_Inventory)
		UNION ALL
			(SELECT 
				current_date() AS UPDATED_DATE,
				PIPLELINE, 
				CLIENT_NAME, 
				REFERENCE_KEY, 
				PROCESS, 
				SUB_PROCESS,	
				LOAN_ID, 
				DOCUMENT_ID, 
				PAGE_ID,
				JIRA_ID,
				START_DATE,
				END_DATE,
				PROCESS_DATE,
				DOCUMENT_TYPE,
				PAGE_NO,
				EXTENSION
			from STG_TBL_ADE)
		UNION ALL
			(SELECT 
				current_date() AS UPDATED_DATE,
				PIPLELINE, 
				CLIENT_NAME, 
				REFERENCE_KEY, 
				PROCESS, 
				SUB_PROCESS,	
				LOAN_ID, 
				DOCUMENT_ID, 
				PAGE_ID,
				JIRA_ID,
				START_DATE,
				END_DATE,
				PROCESS_DATE,
				DOCUMENT_TYPE,
				PAGE_NO,
				EXTENSION
			from STG_TBL_ADR);
	
End$$

DELIMITER ;