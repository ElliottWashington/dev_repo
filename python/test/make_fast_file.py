import datetime
import pandas as pd
import psycopg2
import csv

def create_connection():
    conn = psycopg2.connect(host="10.7.8.59", database="fixtransactions", user="scalp", password="QAtr@de442", port='5433')
    return conn

def execute_query(d, conn, limit=200000000):
    with conn.cursor() as cur:
        postQuery = f"""
        SELECT tag34 as sequencenumber, *
        FROM fixmsg
        WHERE tag39 IS NOT NULL
        AND tag52 >= TIMESTAMP '{d}'
        AND tag52 < TIMESTAMP '{d}' + INTERVAL '1 DAY'
        AND tag1 != '21P530324'
        LIMIT {limit}
        """
        
        cur.execute(postQuery)

        # read results into dataframe
        print("second")
        df = pd.read_sql_query(postQuery, conn)
   
    conn.close()
    
    return df

print("third")   
# calculate the date of the previous day
previous_day = datetime.date.today() - datetime.timedelta(days=1)

# create a connection object
print("fourth")
conn = create_connection()

# execute the query for the previous day
print("fifth")
data = execute_query(previous_day, conn)

# create a hash table (dictionary) from the DataFrame
hash_table = data.to_dict(orient="records")

# write the hash table to a CSV file
with open(f"fast_{previous_day}.csv", "w", newline="") as f:
    writer = csv.DictWriter(f, fieldnames=data.columns)
    writer.writeheader()
    writer.writerows(hash_table)
