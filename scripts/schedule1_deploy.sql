--~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
-- Add new found business elements and manual entries data into the tables
--~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
USE DATABASE BI;
USE SCHEMA _CONTROL_LOGIC;
-------------------------------------------------------------
-- Setup a finder manager 
-------------------------------------------------------------
--
-- Create a finder list table
--
CREATE OR REPLACE TABLE NEW_BUSINESS_ELEMENT_FINDERS (
  FINDER_LABEL VARCHAR COMMENT 'Unique label of the finder',
  FINDER_QUERY VARCHAR COMMENT 'SQL query of the finder',
  FINDER_TYPE VARCHAR COMMENT 'Category of the finder',
  FINDER_NOTE VARCHAR COMMENT 'Finder change note',
  FINDER_ENABLED BOOLEAN COMMENT 'Finder status',
  HOURS_ON_SCHEDULE VARIANT COMMENT 'Finder schedule pattern',        -- 0-23: daily scheduled hour; 24: 24 hours a day
  TIME_OF_LAST_RUN TIMESTAMP_NTZ COMMENT 'Last run time of the finder',
  STATUS_OF_LAST_RUN VARCHAR COMMENT 'SLast run status of the finder'
);
--
-- Create a finder scheduler SP
--
CREATE OR REPLACE PROCEDURE NEW_BUSINESS_ELEMENT_REFRESH(FINDER_ENABLED BOOLEAN)
RETURNS STRING
LANGUAGE JAVASCRIPT STRICT
AS
$$
try {
	var finderLabel = '', finderQuery = '', finderType = '', finderNote = '', finder_Rslt = '';
	var querySet = [], resultSet = [], noteSet = [];

	//-----------------------------------------------
	// Obtain and loop all finders 
	//-----------------------------------------------
	var snow_sql = `
        SELECT FINDER_LABEL, FINDER_QUERY, FINDER_TYPE, FINDER_NOTE
        FROM NEW_BUSINESS_ELEMENT_FINDERS
        WHERE FINDER_ENABLED = :1
        AND (ARRAY_CONTAINS(DATE_PART('HH',CURRENT_TIMESTAMP), HOURS_ON_SCHEDULE) 
             OR ARRAY_CONTAINS(24, HOURS_ON_SCHEDULE)
             OR FINDER_ENABLED = 0)
		`;
	var snow_stmt = snowflake.createStatement({
	  sqlText: snow_sql,
	  binds: [FINDER_ENABLED]
      }); 
	var snow_list = snow_stmt.execute(); 
    
	while (snow_list.next())
	{
        finderLabel = snow_list.getColumnValue(1);
        finderQuery = snow_list.getColumnValue(2);
        finderType = snow_list.getColumnValue(3);
        finderNote = snow_list.getColumnValue(4);
        try {
            var finder_stmt = snowflake.createStatement({ sqlText: finderQuery }); 
            finder_Rslt = finder_stmt.execute(); 
        }
        catch (err1) {
            finder_Rslt = 'Error->' + err1; 
        }    
        finally {
            resultSet.push({"query" : finderQuery, "type": finderType, "note": finderNote, "result": finder_Rslt});
            var finderStatus = finder_Rslt.toString().startsWith("Error") ? 'FAILURE' : 'SUCCESS';
            
            var snow_sql = `
            UPDATE NEW_BUSINESS_ELEMENT_FINDERS 
            SET TIME_OF_LAST_RUN = CURRENT_TIMESTAMP,
                STATUS_OF_LAST_RUN = :2
            WHERE FINDER_LABEL = :1            
            `;
            
            var snow_stmt = snowflake.createStatement({
              sqlText: snow_sql,
              binds: [finderLabel, finderStatus]
              }); 
            var snow_log = snow_stmt.execute(); 
            
        }
    }
    	
	//-----------------------------------------------
    // Done and return the result-set
	//-----------------------------------------------
	return JSON.stringify(resultSet);
}
catch (err) {
	return "Failed: " + err;
}
$$;
