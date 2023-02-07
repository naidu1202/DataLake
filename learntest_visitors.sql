BEGIN;
USE ROLE <<Rolename>>;
USE DATABASE <<DB>>;
USE SCHEMA SANDBOX;
USE WAREHOUSE SG_ETL_XS;
CREATE TEMPORARY TABLE visitors_raw_tmp (src VARIANT);
COPY INTO visitors_raw_tmp
FROM '@data_sync/inbound/test/visitors-out/'
FILE_FORMAT = (TYPE = PARQUET BINARY_AS_TEXT = FALSE)
PATTERN = '.*[.]parquet';
DELETE FROM visitors_data_raw;
INSERT INTO visitors_data_raw (v, event_date)
SELECT src,
	TO_DATE(
		'{{ (next_execution_date + macros.timedelta(hours=8)).strftime("%Y-%m-%d") }}'
	)
FROM visitors_raw_tmp;
COMMIT;