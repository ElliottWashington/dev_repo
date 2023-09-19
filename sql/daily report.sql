select *  from totals_by_account_daily
where (account like '20P%' as '20P' OR account like '24P%' as '24P' OR account like '27P%' as '27P')
and data_date = '2023-02-23';

SELECT data_date, account, gross
FROM totals_by_account_daily
WHERE (account LIKE '20P%' OR account LIKE '24P%' OR account LIKE '27P%')
AND data_date = '2023-02-23';

SELECT
  data_date AS time,
  instrument_type AS "Instrument Type",
  SUM(coalesce(net, 0)) AS "PNL"
FROM
  totals_by_account_daily
WHERE
data_date = '2023-02-23'
AND 
  instrument_type != 'Inactive'
GROUP BY 
  data_date,
  instrument_type
ORDER BY 
  data_date