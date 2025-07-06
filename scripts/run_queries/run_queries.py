import os
import csv
import click
import time
import statistics
import subprocess
import shlex

from utils.connect_to_hive import connect_to_hive

def set_container_cpu_limit(container_name, cpu_count):
    """Set CPU limit for a Docker container"""
    try:
        cmd = f"docker update --cpus={cpu_count} {container_name}"
        subprocess.run(shlex.split(cmd), check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        print(f"[CPU] Set to {cpu_count} core(s), waiting for reallocation...")
        time.sleep(5)  # Allow resource reallocation
        return True
    except subprocess.CalledProcessError:
        return False

def run_query_with_parallelism(
    cursor,
    query_name: str,
    query: str,
    core_limits: list[int],
    number_of_executions: int,
    output_csv_path: str,
    database_name: str,
    container_name: str
):
    """Run query with different CPU core limits and measure execution time"""
    cursor.execute(f"USE {database_name}")
    results = []

    for cpu_count in core_limits:
        # Set Docker CPU limit
        cpus_setted = set_container_cpu_limit(container_name, cpu_count);

        if not cpus_setted:
            raise RuntimeError(f"Failed to set CPU limit to {cpu_count} for container '{container_name}'")

        # Calculate thread count (cores × 2)
        if cpu_count > 1:
            thread_count = cpu_count * 2
            cursor.execute("SET hive.exec.parallel=true")
            cursor.execute(f"SET hive.exec.parallel.thread.number={thread_count}")
            print(f"[CONFIG] Using {thread_count} threads ({cpu_count} cores × 2)")
        else:
            thread_count = 1
            cursor.execute("SET hive.exec.parallel=false")
            cursor.execute(f"SET hive.exec.parallel.thread.number=1")
            print("[CONFIG] Using single-threaded execution")

        execution_times = []
        example_row = None

        for i in range(number_of_executions):
            start_time = time.time()
            try:
                cursor.execute(query)
                rows = cursor.fetchall()
                if example_row is None and rows:
                    example_row = rows[0]
            except Exception as e:
                print(f"[ERROR] {e}")
            finally:
                exec_time = time.time() - start_time
                execution_times.append(exec_time)
                print(f"[RUN {i+1}] Time: {exec_time:.4f}s")
                time.sleep(1)  # Short cooldown

        # Calculate statistics
        mean_time = statistics.mean(execution_times)
        max_time = max(execution_times)
        min_time = min(execution_times)
        
        print(f"\n[SUMMARY] Cores: {cpu_count} | Threads: {thread_count} | "
              f"Time: {mean_time:.4f}s (min: {min_time:.4f}, max: {max_time:.4f})\n")

        results.append([
            query_name,
            cpu_count,
            thread_count,
            mean_time,
            min_time,
            max_time,
            str(example_row) if example_row else "None",
        ])

    # Save results to CSV
    header = [
        'query_name', 'cpu_cores', 'thread_count', 
        'mean_time', 'min_time', 'max_time', 'example_row'
    ]

    
    file_exists = os.path.exists(output_csv_path)
    write_header = not file_exists or os.stat(output_csv_path).st_size == 0
    with open(output_csv_path, 'a', newline='') as csvfile:
        writer = csv.writer(csvfile)
        if write_header:
            writer.writerow(header)
        writer.writerows(results)

def execute_queries(cursor, queries_dir: str, database_name: str, output_csv_path: str, 
                    number_of_executions: int, core_limits: list[int], container_name: str):
    """Process all SQL queries in directory"""
    for sql_file in sorted(f for f in os.listdir(queries_dir) if f.endswith('.sql')):
        sql_path = os.path.join(queries_dir, sql_file)
        with open(sql_path, 'r') as f:
            query = f.read()
        
        query_name = sql_file[:-4]
        print(f"\n{'='*50}")
        print(f"Executing: {query_name}")
        print(f"{'='*50}")
        
        run_query_with_parallelism(
            cursor=cursor,
            query_name=query_name,
            query=query,
            core_limits=core_limits,
            number_of_executions=number_of_executions,
            output_csv_path=output_csv_path,
            database_name=database_name,
            container_name=container_name
        )

@click.command()
@click.option('--host', default='localhost', help='Hive server host')
@click.option('--user', default='hive', help='Hive server user')
@click.option('--port', default=10000, help='Hive server port')
@click.option('--queries_dir', default='queries', help='Directory with SQL files')
@click.option('--database', default='pol_route', help='Database name')
@click.option('--output_csv', default='results.csv', help='Output CSV file')
@click.option('--runs', default=3, help='Executions per configuration')
@click.option('--cores', default='1,2,4', help='CPU cores to test (comma-separated)')
@click.option('--container', required=True, help='Docker container name')
def main(host, port, user, queries_dir, database, output_csv, runs, cores, container):
    """Run performance benchmark with different CPU core allocations"""
    core_limits = [int(c) for c in cores.split(',') if c.strip().isdigit()]
    
    try:
        conn = connect_to_hive(host=host, port=port, user=user)
        with conn.cursor() as cursor:
            execute_queries(
                cursor=cursor,
                queries_dir=queries_dir,
                database_name=database,
                output_csv_path=output_csv,
                number_of_executions=runs,
                core_limits=core_limits,
                container_name=container
            )
    finally:
        if 'conn' in locals():
            conn.close()

if __name__ == '__main__':
    main()