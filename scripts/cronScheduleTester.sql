USE DATABASE BI;
USE DATABASE BI_TEST;
USE SCHEMA _CONTROL_LOGIC;

--------------------------------------------------------
-- Cron expression item parser
--------------------------------------------------------
DROP FUNCTION CRON_ITEM_PARSER(VARCHAR, FLOAT, FLOAT);
SHOW FUNCTIONS;
CREATE OR REPLACE FUNCTION CRON_ITEM_PARSER (
	CRON_ITEM STRING,
	CRON_FIRST FLOAT,
	CRON_LAST FLOAT
)
  RETURNS VARIANT
  LANGUAGE JAVASCRIPT
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
	else if (cronItem == "L") {
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

return cronItemParse(CRON_ITEM, CRON_FIRST, CRON_LAST);
$$
;

SELECT CRON_ITEM_PARSER('0/5-', 0, 59);

--------------------------------------------------------
-- Cron schedule tester for current timestamp
--------------------------------------------------------
DROP FUNCTION CRON_SCHEDULE_TESTER(VARCHAR);
SHOW FUNCTIONS;
CREATE OR REPLACE FUNCTION CRON_SCHEDULE_TESTER (
	CRON_EXPRESSION STRING
)
  RETURNS BOOLEAN
  LANGUAGE JAVASCRIPT
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

function cronScheduleTest(cronExpression) {
	// parse the current timestamp
	var testTimestamp = new Date();
	var testDayOfWeek = testTimestamp.getDay();
	let [testDate, testTime] = testTimestamp.toISOString().split("T");
	let [testYear, testMonth, testDayOfMonth] = testDate.split("-").map(x => parseInt(x));
	let [testHour, testMinute, testSecond] = testTime.slice(0, 9).split(":").map(x => parseInt(x));
	var lastDayOfMonth = (new Date(testYear, testMonth + 1, 0)).getDate();

	// parse the cron expression
	let [cronMinutes, cronHours, cronDayOfMonth, cronMonth, cronDayOfWeek] = cronExpression.split(" ");

	// test cron items
	return cronItemParse(cronMinutes, 0, 59).includes(testMinute)
		&& cronItemParse(cronHours, 0, 23).includes(testHour)
		&& cronItemParse(cronDayOfMonth, 1, lastDayOfMonth).includes(testDayOfMonth)
		&& cronItemParse(cronMonth, 1, 12).includes(testMonth)
		&& cronItemParse(cronDayOfWeek, 0, 6).includes(testDayOfWeek);
}

return cronScheduleTest(CRON_EXPRESSION);
$$
;

SELECT CURRENT_TIMESTAMP, CRON_SCHEDULE_TESTER('40 0-15 18 * 2');


--------------------------------------------------------
-- Cron schedule tester for specified timestamp
--------------------------------------------------------
DROP FUNCTION CRON_SCHEDULE_TESTER(VARCHAR, TIMESTAMP_NTZ);
SHOW FUNCTIONS;
CREATE OR REPLACE FUNCTION CRON_SCHEDULE_TESTER (
	CRON_EXPRESSION STRING,
	TEST_TIMESTAMP TIMESTAMP_NTZ
)
  RETURNS BOOLEAN
  LANGUAGE JAVASCRIPT
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
	var lastDayOfMonth = (new Date(testYear, testMonth + 1, 0)).getDate();

	// parse the cron expression
	let [cronMinutes, cronHours, cronDayOfMonth, cronMonth, cronDayOfWeek] = cronExpression.split(" ");

	// test cron items
	return cronItemParse(cronMinutes, 0, 59).includes(testMinute)
		&& cronItemParse(cronHours, 0, 23).includes(testHour)
		&& cronItemParse(cronDayOfMonth, 1, lastDayOfMonth).includes(testDayOfMonth)
		&& cronItemParse(cronMonth, 1, 12).includes(testMonth)
		&& cronItemParse(cronDayOfWeek, 0, 6).includes(testDayOfWeek);
}

return cronScheduleTest(CRON_EXPRESSION, TEST_TIMESTAMP);
$$
;

SELECT CURRENT_TIMESTAMP, CRON_SCHEDULE_TESTER('* 7-21 18 * *', CURRENT_TIMESTAMP);


--------------------------------------------------------
-- Cron schedule query for specified timestamp
--------------------------------------------------------
SELECT * 
FROM _CONTROL_LOGIC.SINGLE_SNOW_QUERY_JOBS
WHERE CRON_SCHEDULE_TESTER(SCHEDULE_EXPRESSION, '2020-08-18 15:15:16'::TIMESTAMP_NTZ)
;

SELECT * 
FROM _CONTROL_LOGIC.SINGLE_SNOW_QUERY_JOBS
WHERE CRON_SCHEDULE_TESTER(SCHEDULE_EXPRESSION, '2020-08-18 12:15:16'::TIMESTAMP_NTZ)
;
