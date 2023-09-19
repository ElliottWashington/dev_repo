-- Start a transaction
BEGIN;

-- Calculate the start_date as the beginning of the previous month
-- and end_date as the beginning of the current month
SELECT
    DATE_TRUNC('week', NOW() - INTERVAL '1 week')::date AS start_date,
    DATE_TRUNC('week', NOW())::date AS end_date
INTO TEMP dates;

-- Copy data from the Source table to the Archive table
INSERT INTO fixmsg_archive
SELECT * FROM fixmsg_source
WHERE tag52 >= (SELECT start_date FROM dates) AND tag52 < (SELECT end_date FROM dates);

-- Commit the transaction
COMMIT;
