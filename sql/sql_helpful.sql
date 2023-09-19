
-- get all columns
SELECT column_name 
FROM information_schema.columns 
WHERE table_name = 'fixmsg';

-- get all columns and data types
SELECT column_name, data_type
FROM information_schema.columns
WHERE table_name = 'fixmsg'
ORDER BY ordinal_position;


SELECT tag1, tag35, tag39, tag150, tag38, tag167,tag32
FROM fixmsg
WHERE tag1 IS NOT null
AND tag35 IS NOT null
AND tag39 IS NOT null
AND tag150 IS NOT null
AND tag38 IS NOT null
AND tag167 IS NOT null
AND tag32 IS NOT null
AND tag52 >= TIMESTAMP '2023-03-31'
AND tag52 < TIMESTAMP '2023-03-31' + INTERVAL '1 DAY'
LIMIT 100;


SELECT DISTINCT tag167 
FROM fixmsg
where tag52 >= TIMESTAMP '2023-03-31'
AND tag52 < TIMESTAMP '2023-03-31' + INTERVAL '1 DAY'
limit 10000;


