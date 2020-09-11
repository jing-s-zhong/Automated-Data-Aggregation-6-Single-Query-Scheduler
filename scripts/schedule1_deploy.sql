--~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
-- Single snow query job manager
--~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
USE DATABASE BI;
USE DATABASE BI_TEST;
USE SCHEMA _CONTROL_LOGIC;
-------------------------------------------------------------
-- Setup a job manager 
-------------------------------------------------------------
--
-- Create a job management table
--
CREATE OR REPLACE TABLE SINGLE_SNOW_QUERY_JOBS (
  JOB_LABEL VARCHAR COMMENT 'Unique label of the job',
  JOB_QUERY VARCHAR COMMENT 'SQL query of the job',
  JOB_TYPE VARCHAR COMMENT 'Category of the job',
  JOB_NOTE VARCHAR COMMENT 'Job change note',
  JOB_ENABLED BOOLEAN COMMENT 'Job status',
  JOB_PRIORITY NUMBER COMMENT 'Scheudle order of the job',
  SCHEDULE_BATCH_ID NUMBER COMMENT 'Scheduled in batch by same batch_id',
  SCHEDULE_EXPRESSION VARIANT COMMENT 'Job schedule pattern',        -- 0-23: daily scheduled hour; 24: 24 hours a day
  TIME_OF_NEXT_SCHEDULE TIMESTAMP_NTZ COMMENT 'Time of the next job schedule',
  TIME_OF_LAST_RUN TIMESTAMP_NTZ COMMENT 'Time of the last job schedule',
  STATUS_OF_LAST_RUN VARCHAR COMMENT 'SLast run status of the job'
);
-- ALTER TABLE SINGLE_SNOW_QUERY_JOBS RENAME COLUMN TIME_OF_NEXT_RUN TO TIME_OF_NEXT_SCHEDULE;

--
-- Create a job scheduler SP for whole set
--
CREATE OR REPLACE PROCEDURE SINGLE_SNOW_QUERY_JOB_SCHEDULER(
	JOB_ENABLED BOOLEAN
	)
RETURNS STRING
LANGUAGE JAVASCRIPT STRICT
AS
$$
try {
	var jobLabel = '', jobQuery = '', jobType = '', jobNote = '', job_rslt = '';
	var querySet = [], resultSet = [], noteSet = [];

	//-----------------------------------------------
	// Obtain and loop all jobs 
	//-----------------------------------------------
	var snow_sql = `
        SELECT JOB_LABEL, JOB_QUERY, JOB_TYPE, JOB_NOTE
        FROM SINGLE_SNOW_QUERY_JOBS
        WHERE JOB_ENABLED = :1
        AND (ARRAY_CONTAINS(DATE_PART('HH',CURRENT_TIMESTAMP), SCHEDULE_EXPRESSION) 
             OR ARRAY_CONTAINS(24, SCHEDULE_EXPRESSION)
             OR JOB_ENABLED = 0)
        ORDER BY JOB_PRIORITY
		`;
	var snow_stmt = snowflake.createStatement({
		sqlText: snow_sql,
		binds: [JOB_ENABLED]
	});
	var snow_list = snow_stmt.execute();

	while (snow_list.next()) {
		jobLabel = snow_list.getColumnValue(1);
		jobQuery = snow_list.getColumnValue(2);
		jobType = snow_list.getColumnValue(3);
		jobNote = snow_list.getColumnValue(4);
		try {
			var job_Stmt = snowflake.createStatement({ sqlText: jobQuery });
			job_rslt = job_Stmt.execute();
		}
		catch (err1) {
			job_rslt = 'Error->' + err1;
		}
		finally {
			resultSet.push({ "query": jobQuery, "type": jobType, "note": jobNote, "result": job_rslt });
			var jobStatus = job_rslt.toString().startsWith("Error") ? 'FAILURE' : 'SUCCESS';

			var snow_sql = `
            UPDATE SINGLE_SNOW_QUERY_JOBS 
            SET TIME_OF_LAST_RUN = CURRENT_TIMESTAMP,
                STATUS_OF_LAST_RUN = :2
            WHERE JOB_LABEL = :1            
            `;

			var snow_stmt = snowflake.createStatement({
				sqlText: snow_sql,
				binds: [jobLabel, jobStatus]
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
--
--
-- Create a job scheduler SP for jobs in a batch
--
CREATE OR REPLACE PROCEDURE SINGLE_SNOW_QUERY_JOB_SCHEDULER(
	JOB_ENABLED BOOLEAN,
	BATCH_ID FLOAT
	)
RETURNS STRING
LANGUAGE JAVASCRIPT STRICT
AS
$$
function cronItemParse(cronItem, cronFirst, cronLast) {
	var itemOptions = [],
		cronStart = cronFirst,
		cronStop = cronLast,
		cronStep = 1;
	if (cronItem.includes("-") || cronItem.includes("/")) {
		var bounds = cronItem.split("-").map(x => parseInt(x));
		cronStart = isNaN(bounds[0]) ? cronFirst : bounds[0];
		if (bounds.length > 1) {
			cronStop = isNaN(bounds[1]) ? (isNaN(bounds[0]) ? cronFirst : cronLast) : bounds[1]
		}

		var repeats = cronItem.split("-")[0].split("/").map(x => parseInt(x));
		if (repeats.length > 1) {
			cronStart = repeats[0];
			cronStep = repeats[1]
		}
	}
	else if (cronItem === "L") {
		cronStart = cronLast;
		cronStop = cronLast
	}
	else if (cronItem != "*") {
		cronStart = parseInt(cronItem);
		cronStop = parseInt(cronItem)
	}
	else if (cronLast == 59) {
		cronStep = 5
	}
	for (i = cronStart; i <= cronStop; i += cronStep) {
		itemOptions.push(i)
	}
	//return [cronStart, cronStop, cronStep];
	return itemOptions;
}

function cronScheduleTest(cronExpression, testTimestamp) {
	// parse the presented timestamp
	var testDayOfWeek = testTimestamp.getDay();
	let [testDate, testTime] = testTimestamp.toISOString().split("T");
	let [testYear, testMonth, testDayOfMonth] = testDate.split("-").map(x => parseInt(x));
	let [testHour, testMinute, testSecond] = testTime.slice(0, 9).split(":").map(x => parseInt(x));
	var lastDayOfMonth = (new Date(testYear, testMonth, 0)).getDate();

	// parse the cron expression
	let [cronMinutes, cronHours, cronDayOfMonth, cronMonth, cronDayOfWeek] = cronExpression.split(" ");

	// test cron items
	return cronItemParse(cronMinutes, 0, 59).includes(testMinute)
		&& cronItemParse(cronHours, 0, 23).includes(testHour)
		&& cronItemParse(cronDayOfMonth, 1, lastDayOfMonth).includes(testDayOfMonth)
		&& cronItemParse(cronMonth, 1, 12).includes(testMonth)
		&& cronItemParse(cronDayOfWeek, 0, 6).includes(testDayOfWeek);
}

try {
	var jobLabel = '', jobQuery = '', jobType = '', jobNote = '', job_rslt = '';
	var querySet = [], resultSet = [], noteSet = [];
	//-----------------------------------------------
	// Obtain and loop all jobs 
	//-----------------------------------------------
	var snow_sql = `
        SELECT JOB_LABEL, JOB_QUERY, JOB_TYPE, JOB_NOTE, SCHEDULE_EXPRESSION JOB_CRON
        FROM SINGLE_SNOW_QUERY_JOBS
        WHERE JOB_ENABLED = :1
		AND SCHEDULE_BATCH_ID = :2
        AND COALESCE(DATEDIFF('MINUTE',TIME_OF_NEXT_SCHEDULE, CURRENT_TIMESTAMP),0) >= 0
        ORDER BY JOB_PRIORITY
		`;
	var snow_stmt = snowflake.createStatement({
		sqlText: snow_sql,
		binds: [JOB_ENABLED, BATCH_ID]
	});
	var snow_list = snow_stmt.execute();

	while (snow_list.next()) {
		jobLabel = snow_list.getColumnValue(1);
		jobQuery = snow_list.getColumnValue(2);
		jobType = snow_list.getColumnValue(3);
		jobNote = snow_list.getColumnValue(4);
		jobCron = snow_list.getColumnValue(5);
		try {
			var job_Stmt = snowflake.createStatement({ sqlText: jobQuery });
			job_rslt = job_Stmt.execute();
		}
		catch (err1) {
			job_rslt = 'Error->' + err1;
		}
		finally {
			resultSet.push({ "query": jobQuery, "type": jobType, "note": jobNote, "result": job_rslt });
			var jobStatus = job_rslt.toString().startsWith("Error") ? 'FAILURE' : 'SUCCESS';

			var timeMinuteTag = new Date(); timeMinuteTag.setSeconds(0, 0);
			//var yearSpan = new Date(timeMinuteTag.setFullYear(timeMinuteTag.getFullYear() + 1));
			//while ((!cronScheduleTest(jobCron, timeMinuteTag)) && timeMinuteTag <= yearSpan)
			while (!cronScheduleTest(jobCron, timeMinuteTag)) {
				timeMinuteTag.setMinutes(timeMinuteTag.getMinutes() + 1);
			}

			var snow_sql = `
            UPDATE SINGLE_SNOW_QUERY_JOBS 
            SET TIME_OF_NEXT_SCHEDULE = :4,
				TIME_OF_LAST_RUN = CURRENT_TIMESTAMP,
                STATUS_OF_LAST_RUN = :2
            WHERE JOB_LABEL = :1 
			AND SCHEDULE_BATCH_ID = :3        
            `;

			var snow_stmt = snowflake.createStatement({
				sqlText: snow_sql,
				binds: [jobLabel, jobStatus, BATCH_ID, timeMinuteTag.toISOString()]
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

CALL SINGLE_SNOW_QUERY_JOB_SCHEDULER(true);

CALL SINGLE_SNOW_QUERY_JOB_SCHEDULER(true, 2);
select * from SINGLE_SNOW_QUERY_JOBS;
