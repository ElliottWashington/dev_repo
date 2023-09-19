    CREATE TEMP TABLE temp_table1 AS
    SELECT 
    b.tag52 AS order_sent_time,
    a.tag52 AS ack_received_time,
    a.tag52 - b.tag52 AS time_difference,
    b.tag11 AS order_id,
    b.tag100 AS exchange
    FROM 
    fixmsg b 
    JOIN fixmsg a ON b.tag11 = a.tag11 
        AND b.tag35 = 'AB' 
        AND a.tag35 = '8'
    WHERE 
    b.tag52 >= TIMESTAMP '2023-02-23'
    AND b.tag52 < TIMESTAMP '2023-02-24'
    AND a.tag52 >= TIMESTAMP '2023-02-23'
    AND a.tag52 < TIMESTAMP '2023-02-24'
    AND b.tag56 = 'INCAPNS';

    create temp table temp_table2 as
    select time_difference, order_id, exchange from temp_table1;
    select * from temp_table2;

    CREATE TEMP TABLE temp_table3 AS
    SELECT 
        exchange, AVG(time_difference) as average_time_difference, COUNT(time_difference) AS counts
    FROM 
        temp_table2 
    GROUP BY 
        exchange;