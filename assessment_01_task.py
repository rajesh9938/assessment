import psycopg2

def cont_postgres():
    try:
        conn = psycopg2.connect(
            dbname="test",
            user="postgres",
            password="root123",
            host="localhost",
            port="5432"
        )
        return conn
    except psycopg2.Error as e:
        print("connecting to Error:", e)
        return None

def data(conn, csv_file, table_name):
    try:
        cursor = conn.cursor()
        with open(csv_file, 'r') as f:
            cursor.copy_expert(f"COPY {table_name} FROM STDIN CSV HEADER", f)
        conn.commit()
        print("Data save complete.")
    except psycopg2.Error as e:
        print("Error data:", e)

def main():
    csv_file ='D:\\python\\Book1.csv'
    table_name ='public.test_table'
    conn = cont_postgres()
    if conn is None:
        return
    data(conn, csv_file, table_name)
    conn.close()

if __name__ == "__main__":
    main()
