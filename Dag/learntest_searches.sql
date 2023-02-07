BEGIN;
USE ROLE <<rolename>>;
USE DATABASE <<db>>;
USE SCHEMA SANDBOX;
USE WAREHOUSE SG_ETL_XS;
CREATE TEMPORARY TABLE searches_raw_tmp (src VARIANT);
COPY INTO searches_raw_tmp
FROM '@data_sync/inbound/test/searches-out/'
FILE_FORMAT = (TYPE = PARQUET BINARY_AS_TEXT = FALSE)
PATTERN = '.*[.]parquet';
DELETE FROM searches_data_raw;
INSERT INTO searches_data_raw (v, event_date)
SELECT src,
	TO_DATE(
		'{{ (next_execution_date + macros.timedelta(hours=8)).strftime("%Y-%m-%d") }}'
	)
FROM searches_raw_tmp;
COMMIT;