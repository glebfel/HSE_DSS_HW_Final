from etl.db import get_connection


if __name__ == "__main__":
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("select current_database(), version()")
            row = cur.fetchone()
            print({"database": row[0], "version": row[1].split(" on ")[0]})
