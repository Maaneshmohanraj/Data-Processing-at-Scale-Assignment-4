import psycopg2
import psycopg2.extras
import json
import csv
import math 

# Database connection variables
user = "postgres"
passw = "postgres"
database = "assignment4"
host_ip = "127.0.0.1"

def open_db_connection():
    """
    Connects to the database and returns the connection object.
    """
    return psycopg2.connect(f"dbname='{database}' user='{user}' host='{host_ip}' password='{passw}'")

def load_data(table_name, file_path, conn, header_path):
    """
    Creates a table and loads data from a CSV file.
    
    Args:
        table_name (str): Target table name.
        file_path (str): Path to the CSV file.
        conn: Database connection object.
        header_path (str): Path to the header file.
    """
    cur = conn.cursor()
    try:
        cur.execute(f"DROP TABLE IF EXISTS {table_name}")
        
        with open(header_path) as file:
            columns = json.load(file)
        
        columns_str = (", ".join(f"{col} {dtype}" for col, dtype in columns.items()))
        cur.execute(f"CREATE TABLE {table_name} ({columns_str})")
        
        with open(file_path, 'r') as csv_file:
            reader = csv.DictReader(csv_file)
            batch = []
            
            for row in reader:
                values = [row[col] if row[col] != '' else None for col in columns.keys()]
                batch.append(values)
                
                if len(batch) >= 1000:
                    psycopg2.extras.execute_values(
                        cur,
                        f"INSERT INTO {table_name} ({','.join(columns.keys())}) VALUES %s",
                        batch
                    )
                    batch = []
            
            if batch:
                psycopg2.extras.execute_values(
                    cur,
                    f"INSERT INTO {table_name} ({','.join(columns.keys())}) VALUES %s",
                    batch
                )
        
        conn.commit()
    except Exception as e:
        conn.rollback()
        raise Exception(f"Data loading error: {str(e)}")

def range_partition(data_table, partition_base, parts, header_path, partition_col, conn):
    """
    Partitions data in a table using range partitioning.
    
    Args:
        data_table (str): Table with data to partition.
        partition_base (str): Base name for partition tables.
        parts (int): Number of partitions.
        header_path (str): Path to header file.
        partition_col (str): Column to base partition on.
        conn: Database connection.
    """
    cur = conn.cursor()
    cur.execute(f"SELECT MIN({partition_col}), MAX({partition_col}) FROM {data_table}")
    min_val, max_val = cur.fetchone()
    range_size = math.ceil(((max_val - min_val) + 1) / parts)
    
    with open(header_path) as file:
        columns = json.load(file)
    
    columns_str = (", ".join(f"{col} {dtype}" for col, dtype in columns.items()))
    cur.execute(f"CREATE TABLE {partition_base} ({columns_str}) PARTITION BY RANGE ({partition_col})")
    
    for i in range(parts):
        start_range = min_val + (i * range_size)
        end_range = min_val + ((i + 1) * range_size) if i < parts - 1 else max_val + 1
        
        cur.execute(f"CREATE TABLE {partition_base}{i} PARTITION OF {partition_base} FOR VALUES FROM ({start_range}) TO ({end_range})")
    
    cur.execute(f"INSERT INTO {partition_base} SELECT * FROM {data_table}")
    conn.commit()

def round_robin_partition(data_table, partition_base, parts, header_path, conn):
    """
    Partitions data in a table using round-robin partitioning.
    
    Args:
        data_table (str): Table with data to partition.
        partition_base (str): Base name for partition tables.
        parts (int): Number of partitions.
        header_path (str): Path to header file.
        conn: Database connection.
    """
    cur = conn.cursor()
    with open(header_path) as file:
        columns = json.load(file)
    
    columns_str = (", ".join(f"{col} {dtype}" for col, dtype in columns.items()))
    cur.execute(f"CREATE TABLE {partition_base} ({columns_str})")
    
    for i in range(parts):
        cur.execute(f"CREATE TABLE {partition_base}{i} (CHECK (true)) INHERITS ({partition_base})")
    
    trigger_function = f"""
    CREATE OR REPLACE FUNCTION {partition_base}_trigger_func()
    RETURNS TRIGGER AS $$
    DECLARE
        target TEXT;
        min_count BIGINT;
        i INT;
    BEGIN
        min_count := NULL;
        FOR i IN 0..{parts-1} LOOP
            DECLARE curr_count BIGINT;
            BEGIN
                EXECUTE format('SELECT COUNT(*) FROM {partition_base}%s', i) INTO curr_count;
                IF min_count IS NULL OR curr_count < min_count THEN
                    min_count := curr_count;
                    target := format('{partition_base}%s', i);
                END IF;
            END;
        END LOOP;
        
        EXECUTE format('INSERT INTO %I SELECT ($1).*', target) USING NEW;
        RETURN NULL;
    END;
    $$ LANGUAGE plpgsql;"""
    
    cur.execute(trigger_function)
    
    trigger = f"""
    CREATE TRIGGER {partition_base}_insert_trigger
    BEFORE INSERT ON {partition_base}
    FOR EACH ROW EXECUTE FUNCTION {partition_base}_trigger_func()"""
    
    cur.execute(trigger)
    
    cur.execute(f"SELECT * FROM {data_table}")
    rows = cur.fetchall()
    
    for i, row in enumerate(rows):
        part_idx = i % parts
        insert_query = f"INSERT INTO {partition_base}{part_idx} VALUES ({','.join(['%s'] * len(row))})"
        cur.execute(insert_query, row)
    
    conn.commit()

def delete_partitions(base_table, parts, conn):
    """
    Deletes partitions created by the script (for testing).
    
    Args:
        base_table (str): Base table name.
        parts (int): Number of partitions.
        conn: Database connection.
    """
    cur = conn.cursor()
    try:
        cur.execute(f"DROP TABLE IF EXISTS {base_table}")
        
        for i in range(parts):
            cur.execute(f"DROP TABLE IF EXISTS {base_table}{i}")
        
        conn.commit()
    except Exception as e:
        conn.rollback()
        raise Exception(f"Partition deletion error: {str(e)}")
