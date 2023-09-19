-- Start a transaction
BEGIN;

-- Calculate the start_date as the beginning of the previous week
-- and end_date as the beginning of the current week
SELECT
    DATE_TRUNC('week', NOW() - INTERVAL '1 week')::date AS start_date,
    DATE_TRUNC('week', NOW())::date AS end_date
INTO TEMP dates;

-- Copy data from the Source table to the Archive table
INSERT INTO cobdata_archive
SELECT * FROM cobdata_source
WHERE exchangedatetime >= (SELECT start_date FROM dates) AND exchangedatetime < (SELECT end_date FROM dates);

-- Commit the transaction
COMMIT;