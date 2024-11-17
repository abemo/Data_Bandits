import psycopg2
import time
import statistics
from utils import display_results

import config

def run_queries_and_analyze():
    # Connection details
    db_config = {
        'dbname': config.POSTGRES_DBNAME,
        'user': config.POSTGRES_USERNAME,
        'password': config.POSTGRES_PASSWORD,
        'host': config.POSTGRES_HOST,
        'port': config.POSTGRES_PORT
    }
    
    # Define three queries to execute
    queries = [
        "SELECT * FROM trans",  # Query 1
    ]
    
    # Number of executions per query
    iterations = config.NUMBER_ITERATIONS
    results = []

    try:
        # Connect to PostgreSQL database
        connection = psycopg2.connect(**db_config)
        cursor = connection.cursor()
        
        print("Connected to the database successfully.")
        
        for query in queries:
            times = []  # Store execution times for the current query
            
            for _ in range(iterations):
                start_time = time.time()
                cursor.execute(query)
                cursor.fetchall()  # Ensure the query completes execution
                end_time = time.time()
                times.append(end_time - start_time)
            
            # Calculate metrics
            avg_time = sum(times) / iterations
            std_dev = statistics.stdev(times)
            max_time = max(times)
            
            # Append results
            results.append((query, avg_time, std_dev, max_time))
    
    except Exception as e:
        print(f"Error occurred: {e}")
    
    finally:
        if cursor:
            cursor.close()
        if connection:
            connection.close()
        print("Database connection closed.")

    # Display results in a table
    display_results(results)


if __name__ == "__main__":
    run_queries_and_analyze()
