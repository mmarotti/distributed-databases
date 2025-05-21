from pyhive import hive
import sys

def main():
    try:
        # Connect to HiveServer2
        connection = hive.Connection(
            host='localhost',
            port=10000,
            username='hive',  # Default user
            auth='NONE'    # Authentication mode (matches our Docker setup)
        )
        cursor = connection.cursor()

        # Execute sample queries
        print("Connected to HiveServer2!")
        
        # Show databases
        cursor.execute("SHOW DATABASES")
        print("\nDatabases:")
        for db in cursor.fetchall():
            print(f"- {db[0]}")

        # Create a sample table
        cursor.execute("CREATE DATABASE IF NOT EXISTS test_db")
        cursor.execute("USE test_db")
        cursor.execute("CREATE TABLE IF NOT EXISTS sample (id INT, name STRING)")
        
        # Insert some data
        cursor.execute("INSERT INTO sample VALUES (1, 'Alice'), (2, 'Bob')")
        
        # Query data
        cursor.execute("SELECT * FROM sample")
        print("\nSample Data:")
        for row in cursor.fetchall():
            print(f"ID: {row[0]}, Name: {row[1]}")

    except Exception as e:
        print(f"Error: {e}")
        sys.exit(1)
    finally:
        if 'cursor' in locals():
            cursor.close()
        if 'connection' in locals():
            connection.close()

if __name__ == "__main__":
    main()