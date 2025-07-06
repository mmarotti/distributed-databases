import os
import click

from utils.connect_to_hive import connect_to_hive

def create_table_and_load(cursor, query: str, csv_path: str):
    try:
        table_name = query.split()[5]
        cursor.execute(query)
        print(f"Table {table_name} created or already exists.")

        # Truncate the table before loading to ensure uniqueness by id
        cursor.execute(f"TRUNCATE TABLE {table_name}")
        cursor.execute(f"LOAD DATA LOCAL INPATH '{csv_path}' INTO TABLE {table_name}")
        cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
        row_count = cursor.fetchone()[0]

        print(f"Data loaded from {csv_path} into table. Row count: {row_count}")
    except Exception as e:
        print(f"Error creating table or loading data: {e}")

def create_database_and_load(cursor, database_name: str):
    try:
        csv_path = '/data/' # Loading from docker container's data directory

        # Create a new database
        cursor.execute(f"CREATE DATABASE IF NOT EXISTS {database_name}")
        print(f"Database '{database_name}' created or already exists.")

        # Use the newly created database
        cursor.execute(f"USE {database_name}")

        time_table_query = """
            CREATE TABLE IF NOT EXISTS `time` (
                id INT,
                period STRING,
                day INT,
                month INT,
                year INT,
                weekday STRING
            )
            ROW FORMAT DELIMITED
            FIELDS TERMINATED BY ';'
            STORED AS TEXTFILE
            -- PRIMARY KEY (id)
        """
        create_table_and_load(cursor, time_table_query, os.path.join(csv_path, 'time.csv'))

        district_table_query = """
            CREATE TABLE IF NOT EXISTS district (
                id INT,
                name STRING,
                geometry STRING
            )
            ROW FORMAT DELIMITED
            FIELDS TERMINATED BY ';'
            STORED AS TEXTFILE
            -- PRIMARY KEY (id)
        """
        create_table_and_load(cursor, district_table_query, os.path.join(csv_path, 'district.csv'))

        neighborhood_table_query = """
            CREATE TABLE IF NOT EXISTS neighborhood (
                id INT,
                name STRING,
                geometry STRING
            )
            ROW FORMAT DELIMITED
            FIELDS TERMINATED BY ';'
            STORED AS TEXTFILE
            -- PRIMARY KEY (id)
        """
        create_table_and_load(cursor, neighborhood_table_query, os.path.join(csv_path, 'neighborhood.csv'))

        vertice_table_query = """
            CREATE TABLE IF NOT EXISTS vertice (
                id INT,
                label STRING,
                district_id INT,
                neighborhood_id INT,
                zone_id INT
            )
            ROW FORMAT DELIMITED
            FIELDS TERMINATED BY ';'
            STORED AS TEXTFILE
            -- PRIMARY KEY (id)
            -- FOREIGN KEY (district_id) REFERENCES district(id)
            -- FOREIGN KEY (neighborhood_id) REFERENCES neighborhood(id)
        """
        create_table_and_load(cursor, vertice_table_query, os.path.join(csv_path, 'vertice.csv'))


        # drop table vbefore executing
        cursor.execute("DROP TABLE IF EXISTS segment")
        segment_table_query = """
            CREATE TABLE IF NOT EXISTS segment (
                id INT,
                geometry STRING,
                oneway STRING,
                length DECIMAL(10, 2),
                final_vertice_id INT,
                start_vertice_id INT
            )
            ROW FORMAT DELIMITED
            FIELDS TERMINATED BY ';'
            STORED AS TEXTFILE
            -- PRIMARY KEY (id)
            -- FOREIGN KEY (final_vertice_id) REFERENCES vertice(id)
            -- FOREIGN KEY (start_vertice_id) REFERENCES vertice(id)
        """
        create_table_and_load(cursor, segment_table_query, os.path.join(csv_path, 'segment.csv'))

        crime_table_query = """
            CREATE TABLE IF NOT EXISTS crime (
                id INT,
                total_feminicide INT,
                total_homicide INT,
                total_felony_murder INT,
                total_bodily_harm INT,
                total_theft_cellphone INT,
                total_armed_robbery_cellphone INT,
                total_theft_auto INT,
                total_armed_robbery_auto INT,
                segment_id INT,
                time_id INT
            )
            ROW FORMAT DELIMITED
            FIELDS TERMINATED BY ';'
            STORED AS TEXTFILE
            -- PRIMARY KEY (id)
            -- FOREIGN KEY (segment_id) REFERENCES segment(id)
            -- FOREIGN KEY (time_id) REFERENCES time(id)
        """
        create_table_and_load(cursor, crime_table_query, os.path.join(csv_path, 'crime.csv'))

    except Exception as e:
        print(f"Error during database creation or data loading: {e}")

@click.command()
@click.option('--host', default='localhost', help='Database host')
@click.option('--user', default='hive', help='Database user')
@click.option('--port', default=10000, help='Hive port')
def main(host, user, port):
    try:
        connection = connect_to_hive(host=host, user=user, port=port)
        print(f"Connected to Hive at {host}:{port} as user {user}")

        with connection.cursor() as cursor:
            create_database_and_load(cursor, database_name='pol_route')
    except Exception as e:
        print(f"Database connection failed: {e}")
    finally:
        if 'cursor' in locals():
            cursor.close()
        if 'connection' in locals():
            connection.close()

if __name__ == '__main__':
    main()
