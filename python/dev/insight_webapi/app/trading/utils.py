import psycopg2
import psycopg2.extras

mapping = {
    "3OB05285": "12P52852",
    "3OB05287": "14P52874",
    "3OB05259": "15P52595",
    "3OB05283": "16P52836",
    "3OB05286": "17P52867",
    "3OB05294": "18P529422",
    "3OB05295": "19P529523",
    "3OB05255": "1P525501",
    "3OB05292": "20P529221",
    "3OB05303": "21P530324",
    "3OB05306": "22P530624",
    "3OB05305": "23P530525",
    "3OB05311": "24P531126",
    "3OB05314": "25P531426",
    "3OB05315": "26P531527",
    "3OB05322": "27P532248",
    "3OB05323": "28P532349",
    "3OB05324": "29P532451",
    "3OB05258": "2P525802",
    "3OB05326": "30P532652",
    "3OB05327": "31P532753",
    "3OB05345": "32P534563",
    "3OB05346": "33P534612",
    "3OB05347": "34P534728",
    "3OB05360": "35P536046",
    "3OB05361": "36P536158",
    "3OB05362": "37P536299",
    "3OB05363": "38P536381",
    "3OB05364": "39P536458",
    "3OB05284": "3P528403",
    "3OB05365": "40P536571",
    "3OB05366": "41P536622",
    "3OB05367": "42P536787",
    "3OB05256": "4P525604",
    "3OB05257": "5P525705",
    "3OB05288": "6P528806",
    "3OB05260": "7P526007",
    "3OB05289": "8P528918",
    "3OB05261": "9P526109",
    "3OB05254": "SIPM5254"
}

def db_query(query, conn_number, format_cols=None):
    if conn_number == "1":
        conn = psycopg2.connect(host='10.7.8.59', database='fixtransactions', user='scalp', password='QAtr@de442', port='5432')
    elif conn_number == "2":
        conn = psycopg2.connect(host='10.5.1.20', database='Risk', user='scalp_ro', password='QAtr@de442', port='5432')
    else:
        conn = psycopg2.connect(host='10.7.8.59', database='theoretical', user='market_data_ro', password='scalp2022', port='5433')    
    cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
    cur.execute(query)
    raw_results = cur.fetchall()
    cur.close()
    conn.close()

    # Post-processing step to handle zero values
    results = []
    for row in raw_results:
        result = dict(row)
        if format_cols:
            for col, format_spec in format_cols.items():
                result[col] = format(result[col], format_spec)
        results.append(result)

    return results


def db_query_account_numbers():
    query = "SELECT DISTINCT account_number FROM positions where account_number like '%P%'"
    account_number_dicts = db_query(query, "1", format_cols=None)
    account_numbers = [d['account_number'] for d in account_number_dicts]
    return account_numbers