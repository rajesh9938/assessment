import psycopg2

def cont_postgres():
    try:
        conn = psycopg2.connect(
            dbname="test",
            user="postgres",
            password="password",
            host="localhost",
            port="5432"
        )
        return conn
    except psycopg2.Error as e:
        print("Error connecting:", e)
        return None

def data(conn, csv_file, table_name, chunk_size=10000):
    try:
        cursor = conn.cursor()
        with open(csv_file, 'r') as f:
            # Skip the header
            next(f)
            while True:
                # Read chunk_size lines from the file
                lines = ''.join(f.readlines(chunk_size))
                if not lines:
                    break  # Break if no more lines to read
                cursor.copy_from(f, table_name, sep=',', null='')
                conn.commit()  # Commit the transaction for each chunk
        print("Data save complete.")
    except psycopg2.Error as e:
        print("Error inserting data:", e)

def main():
    csv_file = 'D:\\python\\Book1.csv'
    table_name = 'public.test_table'
    conn = cont_postgres()
    if conn is None:
        return
    data(conn, csv_file, table_name)
    conn.close()

if __name__ == "__main__":
    main()
