import pandas as pd
from sqlite3 import connect


def sqlite_to_jsonl(db, sql, output):
    conn = connect(db)
    df = pd.read_sql(sql, conn)

    return df.to_json(output, date_format='epoch', orient='records', lines=True)
